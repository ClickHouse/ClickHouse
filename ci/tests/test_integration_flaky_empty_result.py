"""
Tests for the empty-result handling of the integration flaky/targeted check in
`ci.jobs.integration_test_job`.

The flaky/targeted check runs every changed module under a soft time budget and is
allowed to finish best-effort: when a timeout (the graceful xdist `--session-timeout`
or the hard subprocess backstop) exhausts the budget before any test produces a result,
the empty run is reported as `SKIPPED` instead of blocking the PR.

That downgrade must apply *only* to the timeout path. If pytest produces no results for
some other reason - it crashed before writing the jsonl report, or exited with a
plugin/internal error and no test rows - `Result.from_pytest_run` returns an `ERROR` with
an empty `results` list, and the flaky/targeted check deliberately does not set
`has_error`. Without this distinction the empty `ERROR` would be silently downgraded to
`SKIPPED` and the job would report green, masking a real harness failure.

See ClickHouse/ClickHouse#107453 (review).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.integration_test_job import is_empty_best_effort_skip
from ci.praktika.result import Result


# --- is_empty_best_effort_skip truth table -------------------------------------------


def test_flaky_empty_after_timeout_is_best_effort_skip():
    """Flaky check, no results, a timeout fired -> best-effort SKIPPED."""
    assert (
        is_empty_best_effort_skip(
            is_flaky_check=True,
            is_targeted_check=False,
            has_results=False,
            timed_out=True,
        )
        is True
    )


def test_targeted_empty_after_timeout_is_best_effort_skip():
    """Targeted check, no results, a timeout fired -> best-effort SKIPPED."""
    assert (
        is_empty_best_effort_skip(
            is_flaky_check=False,
            is_targeted_check=True,
            has_results=False,
            timed_out=True,
        )
        is True
    )


def test_flaky_empty_without_timeout_is_not_skipped():
    """The regression: empty result with no timeout is a harness failure, not SKIPPED."""
    assert (
        is_empty_best_effort_skip(
            is_flaky_check=True,
            is_targeted_check=False,
            has_results=False,
            timed_out=False,
        )
        is False
    )


def test_targeted_empty_without_timeout_is_not_skipped():
    assert (
        is_empty_best_effort_skip(
            is_flaky_check=False,
            is_targeted_check=True,
            has_results=False,
            timed_out=False,
        )
        is False
    )


def test_flaky_with_results_is_not_skipped_even_after_timeout():
    """When some results were collected, the run is not an empty best-effort skip."""
    assert (
        is_empty_best_effort_skip(
            is_flaky_check=True,
            is_targeted_check=False,
            has_results=True,
            timed_out=True,
        )
        is False
    )


def test_regular_check_empty_after_timeout_is_not_skipped():
    """Best-effort downgrade is only for flaky/targeted checks, not regular runs."""
    assert (
        is_empty_best_effort_skip(
            is_flaky_check=False,
            is_targeted_check=False,
            has_results=False,
            timed_out=True,
        )
        is False
    )


# --- end-to-end status via Result.create_from ----------------------------------------


def _status_for_empty_run(is_flaky_check, is_targeted_check, timed_out):
    """Mirror the decision in `main`: feed the helper's output into `create_from`."""
    test_results = []
    empty_best_effort = is_empty_best_effort_skip(
        is_flaky_check, is_targeted_check, bool(test_results), timed_out
    )
    R = Result.create_from(
        name="Integration tests (flaky)",
        results=test_results,
        status=Result.Status.SKIPPED if empty_best_effort else "",
    )
    return R.status


def test_empty_timeout_run_reports_skipped():
    assert (
        _status_for_empty_run(
            is_flaky_check=True, is_targeted_check=False, timed_out=True
        )
        == Result.Status.SKIPPED
    )


def test_empty_non_timeout_run_reports_error():
    """A non-timeout empty pytest run stays ERROR (create_from defaults empty->ERROR)."""
    assert (
        _status_for_empty_run(
            is_flaky_check=True, is_targeted_check=False, timed_out=False
        )
        == Result.Status.ERROR
    )
