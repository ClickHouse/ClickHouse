"""
Tests for `FTResultsProcessor.run` exit-code classification in
`ci/jobs/scripts/functional_tests_results.py`.

The parser maps `clickhouse-test`'s exit code to a synthetic summary leaf.
The crucial distinction is between a genuine server death and the runner
merely being killed by a signal:

  * `STOP_TESTING_EXIT_CODE` (2) is the in-band signal that the server died
    or the hung-check tripped -> "Server died", partial per-test results are
    demoted because the crash may have caused them.
  * 143 / 137 / -15 / -9 mean `clickhouse-test` was terminated by a signal
    (job-level timeout, runner shutdown, the worker -> parent SIGTERM
    feedback loop) -> the runner did not finish, but the server did not
    necessarily die. These get a `clickhouse-test` leaf and the completed
    per-test results stay authoritative.

This split is the follow-up to ClickHouse/ClickHouse#105643, which had
folded every kill code into a single "Server died" branch and so produced
hundreds of spurious "Server died" rows from ordinary timeouts.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

# Importing `ci.praktika` first runs its package `__init__`, which appends
# `ci/` to `sys.path` - the parser module imports `from praktika.result`
# (bare, not `ci.praktika.result`), so that path entry must exist first.
from ci.praktika.result import Result
from ci.jobs.scripts.functional_tests_results import (  # noqa: E402
    FTResultsProcessor,
    RUNNER_ABORTED_EXIT_CODES,
    SERVER_DIED_EXIT_CODES,
)


def _processor(tmp_path, lines):
    (tmp_path / "test_result.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return FTResultsProcessor(wd=str(tmp_path))


def _leaf(result, name):
    return next((r for r in result.results if r.name == name), None)


def test_server_died_demotes_multiple_failures_and_adds_server_died_leaf(tmp_path):
    lines = [
        "00001_a: [ FAIL ] 1.00 sec.",
        "00002_b: [ FAIL ] 1.00 sec.",
    ]
    (exit_code,) = tuple(SERVER_DIED_EXIT_CODES)
    result = _processor(tmp_path, lines).run(runner_exit_code=exit_code)

    assert result.status == Result.Status.FAIL
    assert _leaf(result, "Server died") is not None
    # We cannot tell which test crashed the server: both are demoted to UNKNOWN.
    assert _leaf(result, "00001_a").status == Result.Status.UNKNOWN
    assert _leaf(result, "00002_b").status == Result.Status.UNKNOWN
    assert _leaf(result, "clickhouse-test") is None


def test_runner_aborted_keeps_results_and_does_not_report_server_died(tmp_path):
    lines = [
        "00001_a: [ OK ] 1.00 sec.",
        "00002_b: [ FAIL ] 1.00 sec.",
    ]
    for exit_code in sorted(RUNNER_ABORTED_EXIT_CODES):
        result = _processor(tmp_path, lines).run(runner_exit_code=exit_code)

        assert result.status == Result.Status.FAIL, exit_code
        # Not a server death.
        assert _leaf(result, "Server died") is None, exit_code
        # The runner-did-not-finish summary leaf is present.
        aborted_leaf = _leaf(result, "clickhouse-test")
        assert aborted_leaf is not None, exit_code
        assert aborted_leaf.status == Result.Status.FAIL, exit_code
        assert str(exit_code) in aborted_leaf.info, exit_code
        # Completed per-test results stay authoritative - the FAIL that
        # happened before the kill is NOT demoted to UNKNOWN.
        assert _leaf(result, "00001_a").status == Result.Status.OK, exit_code
        assert _leaf(result, "00002_b").status == Result.Status.FAIL, exit_code


def test_clean_finish_reports_ok(tmp_path):
    lines = [
        "00001_a: [ OK ] 1.00 sec.",
        "All tests have finished",
    ]
    result = _processor(tmp_path, lines).run(runner_exit_code=0)

    assert result.status == Result.Status.OK
    assert _leaf(result, "Server died") is None
    assert _leaf(result, "clickhouse-test") is None
