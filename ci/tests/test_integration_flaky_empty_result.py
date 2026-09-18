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

The decision must also survive the best-effort scope cap (`MAX_FLAKY_CHECK_MODULES`): when
a capped run appends synthetic `SKIPPED` entries for the dropped modules, `test_results` is
no longer empty, so the status must be derived from whether *pytest* produced real results
(captured before the append) - otherwise a capped timeout-empty run collapses to a green
top-level status and a capped harness failure reports green instead of `ERROR`.

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


def _status_and_results_for_empty_run(
    is_flaky_check, is_targeted_check, timed_out, skipped_flaky_modules=()
):
    """
    Mirror the empty-result status decision in `main`, including the scope-cap path.

    `skipped_flaky_modules` are the modules dropped by the best-effort scope cap
    (`MAX_FLAKY_CHECK_MODULES`); `main` appends a synthetic `SKIPPED` entry for each so the
    reduced coverage is explicit. The status decision must look only at whether pytest
    produced *real* results (captured before those synthetic entries are appended), so a
    capped run whose selected modules produced nothing is still classified correctly.

    Returns `(status, results)`.
    """
    # The selected modules produced no real pytest rows.
    test_results = []
    pytest_has_results = bool(test_results)  # captured before synthetic entries

    for skipped_module in skipped_flaky_modules:
        test_results.append(
            Result(
                name=skipped_module,
                status=Result.Status.SKIPPED,
                info="Skipped by flaky-check best-effort scope cap (MAX_FLAKY_CHECK_MODULES)",
            )
        )

    empty_best_effort = is_empty_best_effort_skip(
        is_flaky_check, is_targeted_check, pytest_has_results, timed_out
    )
    empty_harness_failure = (
        (is_flaky_check or is_targeted_check)
        and not pytest_has_results
        and not timed_out
    )
    R = Result.create_from(
        name="Integration tests (flaky)",
        results=test_results,
        status=(
            Result.Status.SKIPPED
            if empty_best_effort
            else Result.Status.ERROR if empty_harness_failure else ""
        ),
    )
    return R.status, R.results


def _status_for_empty_run(is_flaky_check, is_targeted_check, timed_out):
    status, _ = _status_and_results_for_empty_run(
        is_flaky_check, is_targeted_check, timed_out
    )
    return status


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


# --- scope-capped path: empty pytest output + synthetic skipped-module entries --------
#
# When the best-effort scope cap (MAX_FLAKY_CHECK_MODULES) drops some modules, `main`
# appends a synthetic `SKIPPED` entry per dropped module. Those entries make `test_results`
# non-empty, so the status decision must rely on whether *pytest* produced real results,
# captured before the append. Otherwise a timeout-empty capped run would collapse to a
# green top-level status, and a non-timeout empty (harness failure) capped run would report
# green instead of ERROR. See ClickHouse/ClickHouse#107453 (review).


def test_capped_empty_timeout_run_reports_skipped():
    """Capped flaky run, selected modules empty, timeout fired -> top-level SKIPPED."""
    status, results = _status_and_results_for_empty_run(
        is_flaky_check=True,
        is_targeted_check=False,
        timed_out=True,
        skipped_flaky_modules=["test_a/test.py", "test_b/test.py"],
    )
    assert status == Result.Status.SKIPPED
    # The skipped modules are still listed so the reduced coverage stays explicit.
    assert [r.name for r in results] == ["test_a/test.py", "test_b/test.py"]
    assert all(r.status == Result.Status.SKIPPED for r in results)


def test_capped_empty_non_timeout_run_reports_error():
    """Capped flaky run, selected modules empty, no timeout -> harness ERROR, not green.

    Regression guard: deciding on `bool(test_results)` after the synthetic skipped-module
    entries are appended would make this run report a green status.
    """
    status, results = _status_and_results_for_empty_run(
        is_flaky_check=True,
        is_targeted_check=False,
        timed_out=False,
        skipped_flaky_modules=["test_a/test.py"],
    )
    assert status == Result.Status.ERROR
    # The skipped module is still listed alongside the harness ERROR.
    assert [r.name for r in results] == ["test_a/test.py"]
