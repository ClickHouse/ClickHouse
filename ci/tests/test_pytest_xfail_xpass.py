"""
Tests for XFAIL/XPASS handling in ResultTranslator.from_pytest_jsonl.

XFAIL (expected failure): test failed as expected → OK status, does not fail job.
XPASS (unexpected pass):  test passed unexpectedly → XPASS status, fails job.
"""

import json
import tempfile
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result, ResultTranslator


def _write_jsonl(entries: list) -> str:
    """Write JSONL entries to a temp file, return its path."""
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    )
    for entry in entries:
        f.write(json.dumps(entry) + "\n")
    f.close()
    return f.name


def _session(entries: list, exitstatus: int = 0) -> list:
    """Wrap test report entries with SessionStart/SessionFinish."""
    return (
        [{"pytest_version": "8.0.0", "$report_type": "SessionStart"}]
        + entries
        + [{"exitstatus": exitstatus, "$report_type": "SessionFinish"}]
    )


def _report(nodeid: str, when: str, outcome: str, wasxfail: str = None) -> dict:
    entry = {
        "$report_type": "TestReport",
        "nodeid": nodeid,
        "when": when,
        "outcome": outcome,
        "duration": 0.1,
        "sections": [],
    }
    # In pytest 8.x, xfail outcomes carry a "wasxfail" field:
    #   xfailed → outcome="skipped" + wasxfail=reason
    #   xpassed → outcome="passed"  + wasxfail=reason
    if wasxfail is not None:
        entry["wasxfail"] = wasxfail
    return entry


def test_xfailed_is_ok():
    """xfailed → XFAIL status, does not fail the overall result."""
    node = "test_mod.py::test_expected_failure"
    entries = _session(
        [
            _report(node, "setup", "passed"),
            # pytest 8.x: xfailed = outcome "skipped" + wasxfail field
            _report(node, "call", "skipped", wasxfail="expected to fail"),
            _report(node, "teardown", "passed"),
        ],
        exitstatus=0,
    )
    path = _write_jsonl(entries)
    try:
        r = ResultTranslator.from_pytest_jsonl(path)
        assert r.status == Result.Status.SUCCESS, f"job status: {r.status}"
        assert len(r.results) == 1
        test = r.results[0]
        assert test.status == Result.StatusExtended.XFAIL, f"test status: {test.status}"
        assert test.is_ok(), "xfailed test should be considered OK"
        assert not test.is_failure(), "xfailed test should not be a failure"
    finally:
        os.unlink(path)


def test_xpassed_fails_job():
    """xpassed → XPASS status, fails the overall result."""
    node = "test_mod.py::test_unexpected_pass"
    entries = _session(
        [
            _report(node, "setup", "passed"),
            # pytest 8.x: xpassed = outcome "passed" + wasxfail field
            _report(node, "call", "passed", wasxfail="expected to fail but actually passes"),
            _report(node, "teardown", "passed"),
        ],
        exitstatus=0,  # pytest itself exits 0 for non-strict xpass
    )
    path = _write_jsonl(entries)
    try:
        r = ResultTranslator.from_pytest_jsonl(path)
        assert r.status == Result.Status.FAILED, f"job status: {r.status}"
        assert len(r.results) == 1
        test = r.results[0]
        assert test.status == Result.StatusExtended.XPASS, f"test status: {test.status}"
        assert not test.is_ok(), "xpassed test should not be considered OK"
        assert test.is_failure(), "xpassed test should be a failure"
    finally:
        os.unlink(path)


def test_xfailed_and_passed_mix():
    """Mix of xfailed and regular passed tests → SUCCESS (xfailed does not fail job)."""
    entries = _session(
        [
            _report("test_mod.py::test_a", "setup", "passed"),
            _report("test_mod.py::test_a", "call", "passed"),
            _report("test_mod.py::test_a", "teardown", "passed"),
            _report("test_mod.py::test_b", "setup", "passed"),
            _report("test_mod.py::test_b", "call", "skipped", wasxfail="expected to fail"),
            _report("test_mod.py::test_b", "teardown", "passed"),
        ],
        exitstatus=0,
    )
    path = _write_jsonl(entries)
    try:
        r = ResultTranslator.from_pytest_jsonl(path)
        assert r.status == Result.Status.SUCCESS, f"job status: {r.status}"
        statuses = {t.name: t.status for t in r.results}
        assert statuses["test_mod.py::test_a"] == Result.StatusExtended.OK
        assert statuses["test_mod.py::test_b"] == Result.StatusExtended.XFAIL
    finally:
        os.unlink(path)


def test_xpassed_and_passed_mix():
    """Mix of xpassed and regular passed tests → FAILED (xpassed fails the job)."""
    entries = _session(
        [
            _report("test_mod.py::test_a", "setup", "passed"),
            _report("test_mod.py::test_a", "call", "passed"),
            _report("test_mod.py::test_a", "teardown", "passed"),
            _report("test_mod.py::test_b", "setup", "passed"),
            _report("test_mod.py::test_b", "call", "passed", wasxfail="expected to fail but actually passes"),
            _report("test_mod.py::test_b", "teardown", "passed"),
        ],
        exitstatus=0,
    )
    path = _write_jsonl(entries)
    try:
        r = ResultTranslator.from_pytest_jsonl(path)
        assert r.status == Result.Status.FAILED, f"job status: {r.status}"
        statuses = {t.name: t.status for t in r.results}
        assert statuses["test_mod.py::test_a"] == Result.StatusExtended.OK
        assert statuses["test_mod.py::test_b"] == Result.StatusExtended.XPASS
    finally:
        os.unlink(path)


if __name__ == "__main__":
    test_xfailed_is_ok()
    print("PASS test_xfailed_is_ok")
    test_xpassed_fails_job()
    print("PASS test_xpassed_fails_job")
    test_xfailed_and_passed_mix()
    print("PASS test_xfailed_and_passed_mix")
    test_xpassed_and_passed_mix()
    print("PASS test_xpassed_and_passed_mix")
    print("All tests passed.")
