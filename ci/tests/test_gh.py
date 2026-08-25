"""
Tests for GH.convert_to_gh_status mapping.

Adding a new Result.Status value requires updating GH._STATUS_TO_GH.
These tests verify that every status is mapped and the mapping is correct.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.gh import GH
from ci.tests.test_result import ALL_STATUSES, _get_class_constants


def test_all_statuses_mapped():
    """Every Result.Status value must have a GH mapping."""
    for status in ALL_STATUSES:
        gh = GH.convert_to_gh_status(status)
        assert gh, f"No GH mapping for {status}"


def test_mapping_values_are_valid_gh_statuses():
    """GH API only accepts these four strings."""
    valid = _get_class_constants(Result.GHStatus)
    for status in ALL_STATUSES:
        gh = GH.convert_to_gh_status(status)
        assert gh in valid, f"GH mapping for {status} is {gh!r}, not a valid GH status"


def test_expected_mappings():
    assert GH.convert_to_gh_status(Result.Status.OK) == "success"
    assert GH.convert_to_gh_status(Result.Status.FAIL) == "failure"
    assert GH.convert_to_gh_status(Result.Status.ERROR) == "error"
    assert GH.convert_to_gh_status(Result.Status.SKIPPED) == "success"
    assert GH.convert_to_gh_status(Result.Status.PENDING) == "pending"
    assert GH.convert_to_gh_status(Result.Status.RUNNING) == "pending"
    assert GH.convert_to_gh_status(Result.Status.DROPPED) == "error"
    assert GH.convert_to_gh_status(Result.Status.UNKNOWN) == "failure"
    assert GH.convert_to_gh_status(Result.Status.XFAIL) == "success"
    assert GH.convert_to_gh_status(Result.Status.XPASS) == "failure"


def test_idempotent():
    """Passing an already-converted GH string should return it unchanged."""
    for gh_val in _get_class_constants(Result.GHStatus):
        assert GH.convert_to_gh_status(gh_val) == gh_val


def test_invalid_status_asserts():
    """Unknown strings must raise."""
    try:
        GH.convert_to_gh_status("bogus")
        assert False, "Should have raised"
    except AssertionError:
        pass


def test_post_commit_status_converts_transparently(monkeypatch):
    """post_commit_status must accept Result.Status values and convert them."""
    captured = {}

    def fake_do_command(cmd):
        captured["cmd"] = cmd
        return True

    monkeypatch.setattr(GH, "do_command_with_retries", staticmethod(fake_do_command))
    monkeypatch.setenv("GITHUB_REPOSITORY", "test/repo")
    monkeypatch.setenv("SHA", "abc123")

    GH.post_commit_status(
        name="test", status=Result.Status.OK, description="ok", url="http://x",
        sha="abc123", repo="test/repo",
    )
    assert "state=success" in captured["cmd"], f"Expected state=success in command, got: {captured['cmd']}"
