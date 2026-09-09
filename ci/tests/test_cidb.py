"""
Tests for CIDB.convert_status mapping.

Adding a new Result.Status value requires updating CIDB._STATUS_TO_CIDB.
These tests verify that every status is mapped and TableRecord auto-converts.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.cidb import CIDB
from ci.tests.test_result import ALL_STATUSES


# The set of valid legacy CIDB check_status strings
VALID_CIDB_STATUSES = {"success", "failure", "error", "skipped", "pending", "running", "dropped"}


def test_all_statuses_mapped():
    """Every Result.Status value must have a CIDB mapping."""
    for status in ALL_STATUSES:
        legacy = CIDB.convert_status(status)
        assert legacy, f"No CIDB mapping for {status}"


def test_mapping_values_are_valid():
    """All mapped values must be known legacy strings."""
    for status in ALL_STATUSES:
        legacy = CIDB.convert_status(status)
        assert legacy in VALID_CIDB_STATUSES, (
            f"CIDB mapping for {status} is {legacy!r}, not a valid CIDB status"
        )


def test_expected_mappings():
    assert CIDB.convert_status(Result.Status.OK) == "success"
    assert CIDB.convert_status(Result.Status.FAIL) == "failure"
    assert CIDB.convert_status(Result.Status.ERROR) == "error"
    assert CIDB.convert_status(Result.Status.SKIPPED) == "skipped"
    assert CIDB.convert_status(Result.Status.PENDING) == "pending"
    assert CIDB.convert_status(Result.Status.RUNNING) == "running"
    assert CIDB.convert_status(Result.Status.DROPPED) == "dropped"
    assert CIDB.convert_status(Result.Status.UNKNOWN) == "failure"
    assert CIDB.convert_status(Result.Status.XFAIL) == "success"
    assert CIDB.convert_status(Result.Status.XPASS) == "failure"


def test_idempotent():
    """Passing an already-converted legacy string should return it unchanged."""
    for legacy in VALID_CIDB_STATUSES:
        assert CIDB.convert_status(legacy) == legacy


def test_invalid_status_asserts():
    """Unknown strings must raise."""
    try:
        CIDB.convert_status("bogus")
        assert False, "Should have raised"
    except AssertionError:
        pass


def test_table_record_auto_converts():
    """TableRecord.__post_init__ must convert Result.Status to legacy string."""
    record = CIDB.TableRecord(
        pull_request_number=0,
        commit_sha="abc",
        commit_url="",
        check_name="test",
        check_status=Result.Status.OK,
        check_duration_ms=0,
        check_start_time=0,
        report_url="",
        pull_request_url="",
        base_ref="",
        base_repo="",
        head_ref="",
        head_repo="",
        task_url="",
        instance_type="",
        instance_id="",
        test_name="",
        test_status="OK",
        test_duration_ms=None,
        test_context_raw="",
    )
    assert record.check_status == "success"


def test_table_record_preserves_legacy():
    """TableRecord must not break if check_status is already a legacy string."""
    record = CIDB.TableRecord(
        pull_request_number=0,
        commit_sha="abc",
        commit_url="",
        check_name="test",
        check_status="failure",
        check_duration_ms=0,
        check_start_time=0,
        report_url="",
        pull_request_url="",
        base_ref="",
        base_repo="",
        head_ref="",
        head_repo="",
        task_url="",
        instance_type="",
        instance_id="",
        test_name="",
        test_status="FAIL",
        test_duration_ms=None,
        test_context_raw="",
    )
    assert record.check_status == "failure"
