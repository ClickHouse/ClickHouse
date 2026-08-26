"""
Tests for Result.Status enum and Result helper methods.

Adding a new status to Result.Status is a significant change that affects
CI reports, CIDB statistics, GitHub commit statuses, Slack notifications,
and the event feed. Think twice before adding one — and update all mapping
tables (GH._STATUS_TO_GH, CIDB._STATUS_TO_CIDB) and these tests.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result


# The canonical set of all statuses.  If you add a new status, you MUST
# update this set, the GH and CIDB mapping tables, json.html rendering,
# the Slack lambda, and the event sanitizer.  The test below will remind you.
ALL_STATUSES = {
    Result.Status.OK,
    Result.Status.FAIL,
    Result.Status.SKIPPED,
    Result.Status.ERROR,
    Result.Status.UNKNOWN,
    Result.Status.XFAIL,
    Result.Status.XPASS,
    Result.Status.PENDING,
    Result.Status.RUNNING,
    Result.Status.DROPPED,
}


def _get_class_constants(cls):
    """Return set of all public string constants defined on a class."""
    return {
        getattr(cls, k)
        for k in dir(cls)
        if not k.startswith("_") and isinstance(getattr(cls, k), str)
    }


def test_all_statuses_accounted_for():
    """No status was added to Result.Status without updating ALL_STATUSES."""
    actual = _get_class_constants(Result.Status)
    assert actual == ALL_STATUSES, (
        f"Result.Status has changed! New: {actual - ALL_STATUSES}, "
        f"Removed: {ALL_STATUSES - actual}. "
        "Update ALL_STATUSES, GH/CIDB mappings, json.html, Slack lambda, and event sanitizer."
    )


def test_status_values_are_uppercase():
    for status in ALL_STATUSES:
        assert status == status.upper(), f"Status {status!r} must be uppercase"


def test_status_values_are_unique():
    values = list(ALL_STATUSES)
    assert len(values) == len(set(values)), "Duplicate status values"


# --- is_ok / is_success / is_failure / is_error ---

def test_is_ok():
    ok_statuses = {Result.Status.OK, Result.Status.SKIPPED, Result.Status.XFAIL}
    not_ok = ALL_STATUSES - ok_statuses
    for s in ok_statuses:
        assert Result("t", s).is_ok(), f"{s} should be ok"
    for s in not_ok:
        assert not Result("t", s).is_ok(), f"{s} should not be ok"


def test_is_success():
    success_statuses = {Result.Status.OK, Result.Status.XFAIL}
    for s in success_statuses:
        assert Result("t", s).is_success(), f"{s} should be success"
    for s in ALL_STATUSES - success_statuses:
        assert not Result("t", s).is_success(), f"{s} should not be success"


def test_is_failure():
    fail_statuses = {Result.Status.FAIL, Result.Status.XPASS}
    for s in fail_statuses:
        assert Result("t", s).is_failure(), f"{s} should be failure"
    for s in ALL_STATUSES - fail_statuses:
        assert not Result("t", s).is_failure(), f"{s} should not be failure"


def test_is_error():
    assert Result("t", Result.Status.ERROR).is_error()
    for s in ALL_STATUSES - {Result.Status.ERROR}:
        assert not Result("t", s).is_error(), f"{s} should not be error"


def test_is_pending():
    assert Result("t", Result.Status.PENDING).is_pending()
    for s in ALL_STATUSES - {Result.Status.PENDING}:
        assert not Result("t", s).is_pending(), f"{s} should not be pending"


def test_is_running():
    assert Result("t", Result.Status.RUNNING).is_running()
    for s in ALL_STATUSES - {Result.Status.RUNNING}:
        assert not Result("t", s).is_running(), f"{s} should not be running"


def test_is_dropped():
    assert Result("t", Result.Status.DROPPED).is_dropped()
    for s in ALL_STATUSES - {Result.Status.DROPPED}:
        assert not Result("t", s).is_dropped(), f"{s} should not be dropped"


def test_is_skipped():
    assert Result("t", Result.Status.SKIPPED).is_skipped()
    for s in ALL_STATUSES - {Result.Status.SKIPPED}:
        assert not Result("t", s).is_skipped(), f"{s} should not be skipped"


def test_is_completed():
    not_completed = {Result.Status.PENDING, Result.Status.RUNNING}
    for s in not_completed:
        assert not Result("t", s).is_completed(), f"{s} should not be completed"
    for s in ALL_STATUSES - not_completed:
        assert Result("t", s).is_completed(), f"{s} should be completed"


# --- create_from ---

def test_create_from_bool_true():
    r = Result.create_from(name="t", status=True)
    assert r.status == Result.Status.OK


def test_create_from_bool_false():
    r = Result.create_from(name="t", status=False)
    assert r.status == Result.Status.FAIL


def test_create_from_aggregates_ok():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.SKIPPED)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.OK


def test_create_from_aggregates_fail():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.FAIL)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.FAIL


def test_create_from_aggregates_error():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.ERROR)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.ERROR


def test_create_from_aggregates_xfail_is_ok():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.XFAIL)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.OK


def test_create_from_aggregates_xpass_is_fail():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.XPASS)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.FAIL


def test_create_from_aggregates_unknown_is_fail():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.UNKNOWN)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.FAIL


def test_create_from_error_takes_priority():
    subs = [Result("a", Result.Status.FAIL), Result("b", Result.Status.ERROR)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.ERROR


# --- set_success / set_failed / set_error ---

def test_set_success():
    r = Result("t", Result.Status.FAIL)
    r.set_success()
    assert r.status == Result.Status.OK


def test_set_failed():
    r = Result("t", Result.Status.OK)
    r.set_failed()
    assert r.status == Result.Status.FAIL


def test_set_error():
    r = Result("t", Result.Status.OK)
    r.set_error()
    assert r.status == Result.Status.ERROR
