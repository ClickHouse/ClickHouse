"""
Unit tests for the `selected tests` functional test runs.

In pull requests the sanitizer flavors of `Stateless tests` do not run the whole
suite: they run only the tests selected for the change (changed tests, tests that
already failed in this pull request, tests covering the changed lines). See
ClickHouse/ClickHouse#114725.

Covered here:
  * resolving a selected test name back to its source file,
  * dropping the selected tests a `parallel`/`sequential` job flavor never runs,
  * reporting a run where `clickhouse-test` filtered out every selected test as
    SKIPPED instead of a failure,
  * the pull request workflow having no full-suite sanitizer stateless jobs left.
"""

import os
import runpy
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.functional_tests import filter_selected_tests_by_flavor
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.praktika.result import Result

_clickhouse_test = os.path.join(
    os.path.dirname(__file__), "../..", "tests", "clickhouse-test"
)
_TestSuite = runpy.run_path(_clickhouse_test)["TestSuite"]


def test_selected_test_name_resolves_to_source_file():
    assert (
        Targeting.functional_test_source_file("00001_select_1")
        == "00001_select_1.sql"
    )
    # Changed tests are selected with a trailing dot so that the name matches
    # that one test only.
    assert (
        Targeting.functional_test_source_file("00001_select_1.")
        == "00001_select_1.sql"
    )


def test_unknown_selected_test_name_has_no_source_file():
    # A stateful test, or a test removed or renamed since the selection data
    # (coverage / previous failures) was collected.
    assert Targeting.functional_test_source_file("99999_no_such_test") is None


def test_flavor_filter_splits_selection(monkeypatch):
    sources = {
        "01000_parallel": "01000_parallel.sql",
        "01001_sequential": "01001_sequential.sql",
    }
    monkeypatch.setattr(
        Targeting, "functional_test_source_file", lambda t: sources.get(t)
    )
    monkeypatch.setattr(
        Targeting,
        "is_sequential_functional_test",
        lambda f: f == "01001_sequential.sql",
    )
    tests = ["01000_parallel", "01001_sequential"]

    assert filter_selected_tests_by_flavor(tests, keep_sequential=False) == [
        "01000_parallel"
    ]
    assert filter_selected_tests_by_flavor(tests, keep_sequential=True) == [
        "01001_sequential"
    ]


def test_flavor_filter_keeps_unresolved_tests(monkeypatch):
    # A name that maps to no file cannot be classified - keep it for both
    # flavors and let `clickhouse-test` filter it out.
    monkeypatch.setattr(Targeting, "functional_test_source_file", lambda t: None)
    tests = ["00042_stateful_test"]

    assert filter_selected_tests_by_flavor(tests, keep_sequential=False) == tests
    assert filter_selected_tests_by_flavor(tests, keep_sequential=True) == tests


_NO_TESTS_OUTPUT = (
    "No tests were run because every explicitly requested test was filtered out.\n"
)


def _process(tmp_path, output, runner_exit_code, allow_no_tests):
    (tmp_path / "test_result.txt").write_text(output, encoding="utf-8")
    return FTResultsProcessor(wd=str(tmp_path)).run(
        runner_exit_code=runner_exit_code, allow_no_tests=allow_no_tests
    )


def test_no_tests_run_is_skipped_for_a_selected_tests_run(tmp_path):
    # Every selected test is filtered out by tags in this job flavor (e.g. all
    # of them are `no-tsan` in a TSan job) - `clickhouse-test` exits with 1.
    result = _process(tmp_path, _NO_TESTS_OUTPUT, 1, allow_no_tests=True)
    assert result.status == Result.Status.SKIPPED


def test_no_tests_run_is_a_failure_for_a_full_suite_run(tmp_path):
    # The full suite matching nothing means a broken filter - still a failure.
    result = _process(tmp_path, _NO_TESTS_OUTPUT, 1, allow_no_tests=False)
    assert result.status == Result.Status.FAIL


def test_failures_are_reported_even_when_no_tests_is_allowed(tmp_path):
    output = (
        "00001_first_failing_test: [ FAIL ] 1.23 sec.\n"
        "some failure details\n"
        "All tests have finished.\n"
    )
    result = _process(tmp_path, output, 1, allow_no_tests=True)
    assert result.status == Result.Status.FAIL


def test_generic_no_tests_banner_does_not_hide_runner_failure(tmp_path):
    output = "ERROR: Process Worker 1 was killed with exit code -9\nNo tests were run.\n"
    result = _process(tmp_path, output, 1, allow_no_tests=True)
    assert result.status == Result.Status.FAIL


def test_unmatched_explicit_selector_is_not_filtered_out(tmp_path):
    (tmp_path / "00001_existing.sql").touch()
    suite = _TestSuite.__new__(_TestSuite)
    suite.args = SimpleNamespace(test=["99999_no_such_test"])
    suite.suite_path = str(tmp_path)
    suite.render_test_template = lambda _env, _path, name: name
    suite.has_explicit_test_match = False

    assert list(suite.get_selected_tests(lambda _name: True)) == []
    assert not suite.has_explicit_test_match


def test_matching_explicit_selector_is_recorded_before_flavor_filter(tmp_path):
    (tmp_path / "00001_existing.sql").touch()
    suite = _TestSuite.__new__(_TestSuite)
    suite.args = SimpleNamespace(test=["00001_existing"])
    suite.suite_path = str(tmp_path)
    suite.render_test_template = lambda _env, _path, name: name
    suite.has_explicit_test_match = False

    assert list(suite.get_selected_tests(lambda _name: False)) == []
    assert suite.has_explicit_test_match


def test_pr_workflow_runs_no_full_suite_sanitizer_functional_tests():
    from ci.workflows.pull_request import (
        CORE_BLOCKING_JOB_NAMES,
        SANITIZERS,
        workflow,
    )

    job_names = [job.name for job in workflow.jobs]
    sanitizer_ft_jobs = [
        name
        for name in job_names
        if name.startswith("Stateless tests (")
        and any(sanitizer in name for sanitizer in SANITIZERS)
    ]
    assert sanitizer_ft_jobs, "no sanitizer functional test jobs in the PR workflow"
    for name in sanitizer_ft_jobs:
        # The flaky, targeted and azure jobs run their own selection or config.
        assert any(
            marker in name
            for marker in ("selected tests", "flaky check", "targeted", "azure")
        ), f"full-suite sanitizer functional test job in the PR workflow: {name}"

    # Every gating job must exist in the workflow - the trimmed jobs replaced
    # the full-suite ones the gate used to name.
    for name in CORE_BLOCKING_JOB_NAMES:
        assert name in job_names, f"unknown job in the blocking gate: {name}"
