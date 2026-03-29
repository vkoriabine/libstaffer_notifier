import hashlib
import json
import os
import smtplib
import sys
import time
import re
import redis
from dataclasses import dataclass, asdict
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional
from icalendar import Calendar
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from flask import Flask, request, jsonify
from queue import Queue
from threading import Thread, Lock

API_KEY = os.environ.get("API_KEY", "")
ICS_URL = os.environ.get("ICS_URL", "")
NTFY_URL = os.environ.get("NTFY_URL", "")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "")
REDIS_URL = os.environ.get("REDIS_URL", "")
PORT = int(os.environ.get("PORT", 10000))

STATE_KEY = "ics_change_watcher:state"
SNAPSHOT_KEY = "ics_change_watcher:schedule_snapshot"

EASTERN = ZoneInfo("America/New_York")

TIMEOUT_SECONDS = 30
USER_AGENT = "ICS-Change-Watcher/2.0"

app = Flask(__name__)
task_queue = Queue()
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
worker_started = False
worker_lock = Lock()

@dataclass
class ICSState:
    url: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    content_length: Optional[int] = None
    sha256: Optional[str] = None
    last_checked_epoch: Optional[float] = None
    last_changed_epoch: Optional[float] = None

class ICSChangeWatcher:
    def __init__(self) -> None:
        self.state = self._load_state()

    def _load_state(self) -> ICSState:
        raw = redis_client.get(STATE_KEY)
        if raw:
            try:
                data = json.loads(raw)
                if data.get("url") == ICS_URL:
                    return ICSState(**data)
            except Exception:
                pass
        return ICSState(url=ICS_URL)


    def _save_state(self) -> None:
        redis_client.set(
            STATE_KEY,
            json.dumps(asdict(self.state), indent=2, ensure_ascii=False),
        )

    def _request_response(self) -> requests.Response:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(ICS_URL, headers=headers, timeout=TIMEOUT_SECONDS, stream=True)
        response.raise_for_status()
        return response

    def _hash_response_content(self, response: requests.Response) -> tuple[str, int]:
        digest = hashlib.sha256()
        total_size = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            digest.update(chunk)
            total_size += len(chunk)
        return digest.hexdigest(), total_size

    def _is_timestamp_newer(self, old_value: Optional[str], new_value: Optional[str]) -> bool:
        if not old_value or not new_value:
            return False
        try:
            return parsedate_to_datetime(new_value) > parsedate_to_datetime(old_value)
        except Exception:
            return False

    def _send_ntfy(self, title: str, body: str) -> None:
        if not NTFY_URL or not NTFY_TOPIC:
            return

        url = f"{NTFY_URL.rstrip('/')}/{NTFY_TOPIC}"
        headers = {
            "Title": title,
            "Tags": "calendar"
        }

        response = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()

    def _notify(self, title: str, message: str) -> None:
        print(f"\n=== {title} ===\n{message}")
        notify_errors = []

        try:
            self._send_ntfy(title, message)
        except Exception as exc:
            notify_errors.append(f"ntfy notification failed: {exc}")

        for error in notify_errors:
            print(error)

    def check_once(self) -> bool:

        def normalize_ical_value(value):
            if value is None:
                return None
            decoded = value.dt if hasattr(value, "dt") else value
            if hasattr(decoded, "isoformat"):
                return decoded.isoformat()
            return str(decoded)

        def build_event_snapshot(ics_bytes: bytes) -> dict[str, dict]:
            calendar = Calendar.from_ical(ics_bytes)
            events: dict[str, dict] = {}

            for component in calendar.walk():
                if component.name != "VEVENT":
                    continue

                uid = str(component.get("UID", "")).strip()
                recurrence_id = normalize_ical_value(component.get("RECURRENCE-ID"))
                key = f"{uid}__{recurrence_id}" if recurrence_id else uid

                event_data = {
                    "uid": uid,
                    "recurrence_id": recurrence_id,
                    "summary": str(component.get("SUMMARY", "")).strip(),
                    "description": str(component.get("DESCRIPTION", "")).strip(),
                    "location": str(component.get("LOCATION", "")).strip(),
                    "status": str(component.get("STATUS", "")).strip(),
                    "dtstart": normalize_ical_value(component.get("DTSTART")),
                    "dtend": normalize_ical_value(component.get("DTEND")),
                    "sequence": str(component.get("SEQUENCE", "")).strip(),
                }

                events[key] = event_data

            return dict(sorted(events.items()))

        print(f"Checking {ICS_URL} ...")
        response = self._request_response()

        new_etag = response.headers.get("ETag")
        new_last_modified = response.headers.get("Last-Modified")
        content_length_header = response.headers.get("Content-Length")
        new_content_length = int(content_length_header) if content_length_header and content_length_header.isdigit() else None

        ics_bytes = response.content
        if new_content_length is None:
            new_content_length = len(ics_bytes)

        new_sha256 = hashlib.sha256(ics_bytes).hexdigest()
        new_events = build_event_snapshot(ics_bytes)

        old_events: dict[str, dict] = {}
        raw_snapshot = redis_client.get(SNAPSHOT_KEY)
        if raw_snapshot:
            try:
                old_events = json.loads(raw_snapshot)
            except Exception:
                old_events = {}

        diff_messages = summarize_differences(old_events, new_events)
        changed = len(diff_messages) > 0

        redis_client.set(
            SNAPSHOT_KEY,
            json.dumps(new_events, indent=2, ensure_ascii=False),
        )

        self.state.etag = new_etag
        self.state.last_modified = new_last_modified
        self.state.content_length = new_content_length
        self.state.sha256 = new_sha256
        self.state.last_checked_epoch = time.time()
        if changed:
            self.state.last_changed_epoch = self.state.last_checked_epoch
        self._save_state()

        if changed:
            max_lines = 10
            shown = diff_messages[:max_lines]
            extra = len(diff_messages) - len(shown)

            message = (
                "\n\n".join(shown)
            )
            if extra > 0:
                message += f"\n\n...and {extra} more change(s)."

            self._notify(f"LibStaffer Update", message)
        else:
            print("No schedule change detected.")

        return changed

    def run_once(self) -> None:
        try:
            self.check_once()
        except requests.HTTPError as exc:
            self._notify("HTTP error while checking URL", str(exc))
        except requests.RequestException as exc:
            self._notify("Network error while checking URL", str(exc))
        except Exception as exc:
            self._notify("Unexpected watcher error", str(exc))


def parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    return dt.astimezone(EASTERN)

def format_dt(value):
    dt = parse_dt(value)
    if not dt:
        return None

    now_et = datetime.now(EASTERN)

    # Today: "3:30 PM"
    if dt.date() == now_et.date():
        return dt.strftime("%I:%M %p").lstrip("0")

    # Same year: "May 31 10:00 AM"
    if dt.year == now_et.year:
        return dt.strftime("%b %d %I:%M %p").replace(" 0", " ").lstrip("0")

    # Fallback if needed
    return dt.strftime("%b %d %Y %I:%M %p").replace(" 0", " ").lstrip("0")

def format_range(start_value, end_value):
    start_dt = parse_dt(start_value)
    end_dt = parse_dt(end_value)

    if not start_dt and not end_dt:
        return "(no time)"
    if not start_dt:
        return format_dt(end_value)
    if not end_dt:
        return format_dt(start_value)

    now_et = datetime.now(EASTERN)

    if start_dt.date() == end_dt.date():
        start_ampm = start_dt.strftime("%p")
        end_ampm = end_dt.strftime("%p")

        if start_ampm == end_ampm:
            start_time = start_dt.strftime("%I:%M").lstrip("0")
        else:
            start_time = start_dt.strftime("%I:%M %p").lstrip("0")

        end_time = end_dt.strftime("%I:%M %p").lstrip("0")

        if start_dt.date() == now_et.date():
            return f"{start_time}–{end_time}"

        if start_dt.year == now_et.year:
            day = start_dt.strftime("%b %d").replace(" 0", " ")
            return f"{day} {start_time}–{end_time}"

        day = start_dt.strftime("%b %d %Y").replace(" 0", " ")
        return f"{day} {start_time}–{end_time}"

    return f"{format_dt(start_value)} -> {format_dt(end_value)}"

def parse_event(event):
    unfilled = "(Unfilled)" in event.get("summary")
    name = event.get("summary").replace("(Unfilled)", "").strip()
    people = re.sub(r'[\d\\]+', '', event.get("location")).strip()
    people_list = [x.strip() for x in people.split(',') if x.strip()]
    people_list.sort()
    parsed = event.copy()
    parsed["unfilled"] = unfilled
    parsed["name"] = name or "unnamed"
    parsed["people"] = people_list
    parsed["range"] = format_range(event.get('dtstart'), event.get('dtend'))
    return parsed

def summarize_differences(old_events_dict: dict, new_events_dict: dict) -> list[str]:
    
    old_events = list(map(parse_event, old_events_dict.values()))
    new_events = list(map(parse_event, new_events_dict.values()))

    messages: list[str] = []

    def key(e):
        return (e.get("name"), e.get("range"))

    # Group events by (name, range)
    old_map = defaultdict(list)
    new_map = defaultdict(list)

    for e in old_events:
        old_map[key(e)].append(e)

    for e in new_events:
        new_map[key(e)].append(e)

    all_keys = set(old_map) | set(new_map)

    for k in sorted(all_keys):
        old_list = old_map.get(k, [])
        new_list = new_map.get(k, [])

        matched_old = [False] * len(old_list)
        matched_new = [False] * len(new_list)

        # Step 1: match identical events (same people + unfilled)
        for i, o in enumerate(old_list):
            for j, n in enumerate(new_list):
                if matched_new[j]:
                    continue
                if (
                    o.get("people") == n.get("people")
                    and o.get("unfilled") == n.get("unfilled")
                ):
                    matched_old[i] = True
                    matched_new[j] = True
                    break

        # Step 2: match "changed" events (same name+range, different people/unfilled)
        for i, o in enumerate(old_list):
            if matched_old[i]:
                continue
            for j, n in enumerate(new_list):
                if matched_new[j]:
                    continue

                o_people = o.get('people')
                n_people = n.get('people')
                o_people_set = set(o_people)
                n_people_set = set(n_people)
                added = list(n_people_set - o_people_set)
                removed = list(o_people_set - n_people_set)
                added.sort()
                removed.sort()

                shift_name = f"{o.get('name')} @ {o.get('range')}"
                addendum = ""
                if n.get("unfilled"):
                    addendum = "\n(this shift is not fully filled)"

                if added and not removed:
                    messages.append(f"SHIFT CLAIMED\n{shift_name}\nAdded: {", ".join(added)}{addendum}")
                elif not added and removed:
                    messages.append(f"SHIFT DROPPED\n{shift_name}\nRemoved: {", ".join(removed)}")
                elif added and removed:
                    messages.append(f"SHIFT REASSIGNED\n{shift_name}\nAdded: {", ".join(added)}\nRemoved: {", ".join(removed)}{addendum}")

                matched_old[i] = True
                matched_new[j] = True
                break

    return messages

# --- Worker that processes tasks one at a time ---
def worker():
    print(f"Worker thread started in pid={os.getpid()}")
    while True:
        print("Worker waiting for task...")
        task = task_queue.get()
        print("Worker got task")
        try:
            task()
        except Exception as e:
            print(f"Error running task: {e}")
        finally:
            task_queue.task_done()

def ensure_worker_started():
    global worker_started
    if not worker_started:
        with worker_lock:
            if not worker_started:
                thread = Thread(target=worker, daemon=True)
                thread.start()
                worker_started = True


@app.before_request
def before_request():
    ensure_worker_started()

# --- API endpoint ---
@app.route("/sync", methods=["POST"])
def sync():
    # API key check
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        print("Unauthorized request")
        return jsonify({"status": "ignored"}), 401

    # Define the task
    def task():
        watcher = ICSChangeWatcher()
        watcher.run_once()

    # Add to queue
    print("Adding new task to queue")
    task_queue.put(task)

    return jsonify({"status": "queued"})

@app.route("/", methods=["GET"])
def health():
    ensure_worker_started()
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)