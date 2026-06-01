"""
Tests for `FTResultsProcessor.run` distinguishing a wall-clock budget timeout
from a server death.

Both a job-level budget timeout and a genuine mid-flight abort kill the runner
with SIGTERM, so they share the same exit code. Only the timeout case carries
`timed_out=True` (set by `Shell._check_timeout`). In that case the server is
healthy and the completed per-test results are authoritative, so the processor
must report a `Timeout` leaf and keep per-test results intact, rather than a
`Server died` leaf with the failures demoted to `UNKNOWN`.
"""

import os
import signal
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.praktika.result import Result


def _processor(tmp_path, lines):
    output_file = tmp_path / "test_result.txt"
    output_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return FTResultsProcessor(wd=str(tmp_path))


def _names(result):
    return [r.name for r in result.results]


def _by_name(result, name):
    return next(r for r in result.results if r.name == name)


def test_budget_timeout_reports_timeout_not_server_died(tmp_path):
    """A budget timeout (SIGTERM, `timed_out=True`) must surface as `Timeout`,
    never `Server died`, and must keep a genuine per-test failure visible.
    """
    proc = _processor(
        tmp_path,
        [
            "2026-06-01 10:00:00 00001_passing_test: [ OK ] 1.00 sec.",
            "2026-06-01 10:00:01 03748_join_on_complex_condition_result_memory: [ FAIL ] 1.23 sec.",
            "2026-06-01 10:00:01 Code: 241. MEMORY_LIMIT_EXCEEDED",
        ],
    )

    result = proc.run(runner_exit_code=-signal.SIGTERM, timed_out=True)

    assert "Timeout" in _names(result)
    assert "Server died" not in _names(result)
    assert result.status == Result.Status.FAIL
    # The genuine failure stays a FAIL (authoritative), not demoted to UNKNOWN.
    assert _by_name(result, "03748_join_on_complex_condition_result_memory").status == (
        Result.Status.FAIL
    )


def test_budget_timeout_with_no_failures_still_fails_as_timeout(tmp_path):
    """A timeout with only passing tests must not read as OK; it is a `Timeout`
    FAIL so the job is flagged and re-run.
    """
    proc = _processor(
        tmp_path,
        ["2026-06-01 10:00:00 00001_passing_test: [ OK ] 1.00 sec."],
    )

    result = proc.run(runner_exit_code=-signal.SIGTERM, timed_out=True)

    assert "Timeout" in _names(result)
    assert "Server died" not in _names(result)
    assert result.status == Result.Status.FAIL
    assert _by_name(result, "00001_passing_test").status == Result.Status.OK


def test_sigterm_without_timeout_still_reports_server_died(tmp_path):
    """A SIGTERM that is not a budget timeout (`timed_out=False`) keeps the
    existing abort behavior: a `Server died` leaf and ambiguous parallel
    failures demoted to `UNKNOWN`.
    """
    proc = _processor(
        tmp_path,
        [
            "2026-06-01 10:00:00 00001_test_a: [ FAIL ] 1.00 sec.",
            "2026-06-01 10:00:01 00002_test_b: [ FAIL ] 1.00 sec.",
        ],
    )

    result = proc.run(runner_exit_code=-signal.SIGTERM, timed_out=False)

    assert "Server died" in _names(result)
    assert "Timeout" not in _names(result)
    assert result.status == Result.Status.FAIL
    # Two failures during an abort are ambiguous -> demoted to UNKNOWN.
    assert _by_name(result, "00001_test_a").status == Result.Status.UNKNOWN
    assert _by_name(result, "00002_test_b").status == Result.Status.UNKNOWN


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
