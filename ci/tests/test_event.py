"""
Tests for Event ci_status handling.

The Event.ci_status field stores Result.Status values directly.
The EventFeed sanitizer must handle both old (lowercase) and new (uppercase) formats.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.event import Event, EventFeed


def _make_event(ci_status, timestamp=None, pr_number=1, sha="abc123"):
    return Event(
        type=Event.Type.RUNNING,
        timestamp=timestamp or int(time.time()),
        sha=sha,
        result={},
        ci_status=ci_status,
        ext={"pr_number": pr_number, "repo_name": "test/repo"},
    )


def _sanitize_via_feed(event):
    """Run the sanitize logic by adding the event to a feed."""
    feed = EventFeed()
    feed.add(event)
    return feed.events[0]


def test_event_stores_status_directly():
    """ci_status should store the value as-is, no conversion."""
    e = _make_event("OK")
    assert e.ci_status == "OK"


def test_sanitize_stale_running_new_format():
    """Stale RUNNING events should be marked as FAIL."""
    old_ts = int(time.time()) - 13 * 3600  # 13 hours ago
    e = _sanitize_via_feed(_make_event("RUNNING", timestamp=old_ts))
    assert e.ci_status == "FAIL"
    assert e.ext.get("is_cancelled") is True


def test_sanitize_stale_pending_new_format():
    """Stale PENDING events should be marked as FAIL."""
    old_ts = int(time.time()) - 13 * 3600
    e = _sanitize_via_feed(_make_event("PENDING", timestamp=old_ts))
    assert e.ci_status == "FAIL"


def test_sanitize_stale_running_old_format():
    """Stale events with old lowercase format should also be handled."""
    old_ts = int(time.time()) - 13 * 3600
    e = _sanitize_via_feed(_make_event("running", timestamp=old_ts))
    assert e.ci_status == "FAIL"


def test_sanitize_fresh_running_untouched():
    """Recent RUNNING events should not be modified."""
    e = _sanitize_via_feed(_make_event("RUNNING"))
    assert e.ci_status == "RUNNING"


def test_sanitize_completed_untouched():
    """Completed events should not be modified regardless of age."""
    old_ts = int(time.time()) - 13 * 3600
    e = _sanitize_via_feed(_make_event("OK", timestamp=old_ts))
    assert e.ci_status == "OK"
