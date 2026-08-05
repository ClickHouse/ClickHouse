"""
Unit tests for `FTResultsProcessor`, the job-side parser that turns
`clickhouse-test` output + its exit code into the report tree.

Focus: a run stopped early by `--max-failures` / `--max-failures-chain`
(exit code `MAX_FAILURES_EXIT_CODE`) must be reported as real failures plus a
"Too many test failures" leaf - NOT as "Server died" with the per-test
attribution demoted to UNKNOWN (which is what the aborted-run exit codes do).
"""

import os
import sys

# Repo root so `ci.*` resolves; the `ci` dir so the bare `from praktika...`
# import inside `functional_tests_results` resolves the same way it does when
# the praktika job runner imports it.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.functional_tests_results import (
    MAX_FAILURES_EXIT_CODE,
    STOP_TESTING_EXIT_CODE,
    FTResultsProcessor,
)
from ci.praktika.result import Result

# Two real failures from a parallel run that stopped early. No "All tests have
# finished" line, because the run was aborted before completing.
_TWO_FAILURES = (
    "00001_first_failing_test: [ FAIL ] 1.23 sec.\n"
    "some failure details\n"
    "00002_second_failing_test: [ FAIL ] 0.50 sec.\n"
    "more failure details\n"
)

# A single failure from a sequential run that crashed the server.
_ONE_FAILURE = (
    "04545_regression_test: [ FAIL ] 1.23 sec.\n"
    "server died with SIGABRT\n"
)


def _process(tmp_path, output, runner_exit_code, is_bugfix_validation=False):
    (tmp_path / "test_result.txt").write_text(output, encoding="utf-8")
    return FTResultsProcessor(wd=str(tmp_path)).run(
        runner_exit_code=runner_exit_code,
        is_bugfix_validation=is_bugfix_validation,
    )


def _named(result, name):
    return [r for r in result.results if r.name == name]


def test_max_failures_keeps_real_failures_and_adds_summary(tmp_path):
    result = _process(tmp_path, _TWO_FAILURES, MAX_FAILURES_EXIT_CODE)

    assert result.status == Result.Status.FAIL

    # The informational leaf is present...
    summary = _named(result, "Too many test failures")
    assert len(summary) == 1
    assert summary[0].status == Result.Status.FAIL

    # ...no synthetic "Server died" leaf is added...
    assert not _named(result, "Server died")

    # ...and both real failures stay FAIL (not demoted to UNKNOWN).
    for name in ("00001_first_failing_test", "00002_second_failing_test"):
        entries = _named(result, name)
        assert len(entries) == 1, name
        assert entries[0].status == Result.Status.FAIL, name


def test_aborted_run_still_reports_server_died(tmp_path):
    """Contrast: the same output with an aborted-run exit code keeps the old
    behavior - "Server died" plus the failures demoted to UNKNOWN."""
    result = _process(tmp_path, _TWO_FAILURES, STOP_TESTING_EXIT_CODE)

    assert result.status == Result.Status.FAIL
    assert len(_named(result, "Server died")) == 1
    assert not _named(result, "Too many test failures")
    for name in ("00001_first_failing_test", "00002_second_failing_test"):
        entries = _named(result, name)
        assert len(entries) == 1, name
        assert entries[0].status == Result.Status.UNKNOWN, name


def test_aborted_run_single_culprit_demoted_to_error(tmp_path):
    """A sequential run where exactly one test crashed the server: the test is
    the attributed culprit and is demoted to ERROR so it does not read as an
    ordinary test failure (e.g. in flaky reports)."""
    result = _process(tmp_path, _ONE_FAILURE, STOP_TESTING_EXIT_CODE)

    assert result.status == Result.Status.FAIL
    assert len(_named(result, "Server died")) == 1
    entries = _named(result, "04545_regression_test")
    assert len(entries) == 1
    assert entries[0].status == Result.Status.ERROR


def test_bugfix_validation_keeps_single_crash_culprit_as_fail(tmp_path):
    """In bugfix validation the regression test crashing the server on master
    HEAD is the expected reproduction of the bug. The culprit must stay FAIL:
    an ERROR row would make `invert_bugfix_validation_status` report the run
    inconclusive (its fail-closed guard against infra errors, #105789)."""
    result = _process(
        tmp_path, _ONE_FAILURE, STOP_TESTING_EXIT_CODE, is_bugfix_validation=True
    )

    assert result.status == Result.Status.FAIL
    assert len(_named(result, "Server died")) == 1
    entries = _named(result, "04545_regression_test")
    assert len(entries) == 1
    assert entries[0].status == Result.Status.FAIL


def test_bugfix_validation_parallel_crash_still_validates_via_server_died(tmp_path):
    """The >1-failed counterpart: in bugfix validation a parallel-run crash
    keeps the UNKNOWN demotion (attribution is genuinely unknown), and the
    validation still passes end-to-end through the inverter via the flipped
    `Server died` row, with the UNKNOWN rows left un-flipped."""
    from ci.jobs.functional_tests import invert_bugfix_validation_status

    result = _process(
        tmp_path, _TWO_FAILURES, STOP_TESTING_EXIT_CODE, is_bugfix_validation=True
    )

    for name in ("00001_first_failing_test", "00002_second_failing_test"):
        assert _named(result, name)[0].status == Result.Status.UNKNOWN, name

    no_repro = invert_bugfix_validation_status(result)

    assert no_repro is False
    assert result.status == Result.Status.OK
    assert _named(result, "Server died")[0].status == Result.Status.OK
    for name in ("00001_first_failing_test", "00002_second_failing_test"):
        assert _named(result, name)[0].status == Result.Status.UNKNOWN, name


def test_bugfix_validation_single_crash_counts_as_reproduction(tmp_path):
    """End-to-end #105789 scenario: processor output for a single-test server
    crash in bugfix-validation mode, fed through the inverter, must report a
    successful reproduction (OK), not an inconclusive ERROR."""
    from ci.jobs.functional_tests import invert_bugfix_validation_status

    result = _process(
        tmp_path, _ONE_FAILURE, STOP_TESTING_EXIT_CODE, is_bugfix_validation=True
    )

    no_repro = invert_bugfix_validation_status(result)

    assert no_repro is False
    assert result.status == Result.Status.OK
    culprit = _named(result, "04545_regression_test")[0]
    assert culprit.status == Result.Status.OK
    assert _named(result, "Server died")[0].status == Result.Status.OK
