"""
Unit tests for `FTResultsProcessor`, the job-side parser that turns
`clickhouse-test` output + its exit code into the report tree.

Focus: a run stopped early by `--max-failures` / `--max-failures-chain`
(exit code `MAX_FAILURES_EXIT_CODE`) must be reported as real failures plus a
"Too many test failures" leaf - NOT as "Server died" with the per-test
attribution demoted to UNKNOWN (which is what the aborted-run exit codes do).

Also: a run killed by a signal with no failure observed at all must not claim
the server died, since the exit code alone does not establish that.
"""

import os
import signal
import sys

# Repo root so `ci.*` resolves; the `ci` dir so the bare `from praktika...`
# import inside `functional_tests_results` resolves the same way it does when
# the praktika job runner imports it.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.functional_tests_results import (
    KILLED_BY_SIGNAL_EXIT_CODES,
    KILLED_BY_SIGNAL_RESULT_NAME,
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

# Tests were flowing normally when something killed the runner: every parsed
# row passed and there is no "All tests have finished" line.
_NO_FAILURES = (
    "00001_first_test: [ OK ] 1.23 sec.\n"
    "00002_second_test: [ OK ] 0.50 sec.\n"
    "00003_third_test: [ OK ] 0.75 sec.\n"
)

# A test on the blacklist that unexpectedly passed. It increments the parser's
# `failed` counter but is stored with a status string outside `is_failure()`.
_NOT_FAILED = "00001_blacklisted_test: [ NOT_FAILED ] 1.00 sec.\n"


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


def test_signal_killed_with_no_failures_is_not_labelled_server_died(tmp_path):
    """The exit code proves only that the run was killed, so with nothing
    reported as failed the leaf must not claim the server died."""
    result = _process(tmp_path, _NO_FAILURES, 128 + signal.SIGTERM)

    assert not _named(result, "Server died")

    leaves = _named(result, KILLED_BY_SIGNAL_RESULT_NAME)
    assert len(leaves) == 1
    assert leaves[0].status == Result.Status.ERROR
    assert result.status == Result.Status.ERROR

    # The `clickhouse-test` fallback leaf fires only when `state` is still OK.
    assert not _named(result, "clickhouse-test")

    # The aggregate keeps the counters that made the old label contradictory.
    assert result.info == "Failed: 0, Passed: 3, Skipped: 0"


def test_signal_killed_negative_exit_code_form(tmp_path):
    """`Popen.returncode` reports the wrapper bash dying from a signal as `-N`.
    Neither the runner's nor the server's fate is established there either, so
    the leaf name must not vary by encoding."""
    result = _process(tmp_path, _NO_FAILURES, -signal.SIGTERM)

    assert not _named(result, "Server died")
    leaves = _named(result, KILLED_BY_SIGNAL_RESULT_NAME)
    assert len(leaves) == 1
    assert leaves[0].status == Result.Status.ERROR
    assert str(-signal.SIGTERM) in leaves[0].info
    assert result.status == Result.Status.ERROR


def test_signal_killed_with_a_failure_keeps_server_died(tmp_path):
    """Narrowness guard: once any failure was observed, the branch keeps its
    old behaviour, including demoting the attributed culprit."""
    result = _process(tmp_path, _ONE_FAILURE, 128 + signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    assert len(_named(result, "Server died")) == 1
    assert _named(result, "Server died")[0].status == Result.Status.FAIL
    assert not _named(result, KILLED_BY_SIGNAL_RESULT_NAME)
    assert _named(result, "04545_regression_test")[0].status == Result.Status.ERROR


def test_stop_testing_exit_code_keeps_server_died_with_no_failures(tmp_path):
    """`STOP_TESTING_EXIT_CODE` is excluded by design: the parent reached its
    own `StopTesting` handler, which it raises on an observed server death."""
    assert STOP_TESTING_EXIT_CODE not in KILLED_BY_SIGNAL_EXIT_CODES

    result = _process(tmp_path, _NO_FAILURES, STOP_TESTING_EXIT_CODE)

    assert result.status == Result.Status.FAIL
    assert len(_named(result, "Server died")) == 1
    assert _named(result, "Server died")[0].status == Result.Status.FAIL
    assert not _named(result, KILLED_BY_SIGNAL_RESULT_NAME)


def test_bugfix_validation_signal_kill_is_inconclusive_not_a_reproduction(tmp_path):
    """A signal kill with nothing attributed must not read as a successful
    reproduction. `ERROR` routes it to the inverter's inconclusive guard; a
    `FAIL` leaf here would be flipped to OK and report a validation from an
    exit code alone."""
    from ci.jobs.functional_tests import invert_bugfix_validation_status

    result = _process(
        tmp_path, _NO_FAILURES, 128 + signal.SIGTERM, is_bugfix_validation=True
    )

    no_repro = invert_bugfix_validation_status(result)

    assert no_repro is False
    for r in result.results:
        assert r.has_label(Result.Label.XFAIL), r.name

    # Inconclusive, not a reproduction: the inverter left the aggregate at
    # ERROR instead of calling `set_success`, and did not flip the leaf to OK
    # the way it flips a FAIL row.
    assert result.status == Result.Status.ERROR
    assert _named(result, KILLED_BY_SIGNAL_RESULT_NAME)[0].status == Result.Status.ERROR


def test_bugfix_validation_signal_kill_with_a_blocker_fatal_still_reproduces(tmp_path):
    """`ERROR` loses no crash coverage: `reconcile_bugfix_crash_repro` runs
    first and a `BLOCKER` fatal in the server log downgrades the ERROR rows to
    FAIL, so a genuine crash still validates (#105789)."""
    from ci.jobs.functional_tests import (
        invert_bugfix_validation_status,
        reconcile_bugfix_crash_repro,
    )

    result = _process(
        tmp_path, _NO_FAILURES, 128 + signal.SIGTERM, is_bugfix_validation=True
    )
    assert _named(result, KILLED_BY_SIGNAL_RESULT_NAME)[0].status == Result.Status.ERROR

    fatal = Result(
        name="Sanitizer assert or Fatal messages in server logs",
        status=Result.Status.FAIL,
    )
    fatal.set_label(Result.Label.BLOCKER)

    assert reconcile_bugfix_crash_repro(result, [fatal]) is True
    assert _named(result, KILLED_BY_SIGNAL_RESULT_NAME)[0].status == Result.Status.FAIL

    no_repro = invert_bugfix_validation_status(result)

    assert no_repro is False
    assert result.status == Result.Status.OK
    assert _named(result, KILLED_BY_SIGNAL_RESULT_NAME)[0].status == Result.Status.OK


def test_not_failed_row_keeps_server_died(tmp_path):
    """A `[ NOT_FAILED ]` row increments the parser's `failed` counter but is
    stored with a status string outside `is_failure()`. It is an observed
    failure, so the gate must see it - which it does only because it reads the
    summary counters rather than filtering the rows."""
    result = _process(tmp_path, _NOT_FAILED, 128 + signal.SIGTERM)

    # Guard against a vacuous fixture: the row must really parse as NOT_FAILED.
    row = _named(result, "00001_blacklisted_test")
    assert len(row) == 1
    assert row[0].status == "NOT_FAILED"
    assert not row[0].is_failure()

    assert len(_named(result, "Server died")) == 1
    assert not _named(result, KILLED_BY_SIGNAL_RESULT_NAME)
