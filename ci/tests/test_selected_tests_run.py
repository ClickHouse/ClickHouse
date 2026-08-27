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

import pytest

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


def test_rendered_selected_template_test_name_resolves_to_source_file():
    assert (
        Targeting.functional_test_source_file("00172_hits_joins.gen")
        == "00172_hits_joins.sql.j2"
    )
    assert (
        Targeting.functional_test_source_file("00172_hits_joins.gen.sql")
        == "00172_hits_joins.sql.j2"
    )


def test_selected_tests_normalize_rendered_template_test_names(monkeypatch):
    targeter = Targeting.__new__(Targeting)
    targeter.job_type = Targeting.STATELESS_JOB_TYPE

    monkeypatch.setattr(
        targeter,
        "get_previously_failed_tests_with_info",
        lambda strict: (["00172_hits_joins.gen"], None),
    )
    monkeypatch.setattr(
        targeter,
        "get_most_relevant_tests",
        lambda: (["00172_hits_joins.gen.sql"], None),
    )

    tests, _ = targeter.get_all_relevant_tests_with_info()
    assert tests == ["00172_hits_joins"]


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


def test_selected_tests_fail_when_previous_failure_lookup_fails(monkeypatch):
    targeter = Targeting.__new__(Targeting)
    targeter.job_type = Targeting.STATELESS_JOB_TYPE

    monkeypatch.setattr(
        targeter,
        "get_changed_or_new_tests_with_info",
        lambda strict=False, include_harness_smoke=False: ([], None),
    )

    def raise_cidb_error():
        raise ConnectionError("CIDB unavailable")

    monkeypatch.setattr(targeter, "get_previously_failed_tests", raise_cidb_error)

    with pytest.raises(RuntimeError, match="previously failed tests"):
        targeter.get_all_relevant_tests_with_info(include_changed_tests=True)


def test_selected_tests_fail_when_changed_file_lookup_fails():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False,
        get_changed_files=lambda: None,
    )

    with pytest.raises(RuntimeError, match="changed files"):
        targeter.get_changed_or_new_tests_with_info(strict=True)


def test_selected_tests_add_smoke_tests_for_harness_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False, get_changed_files=lambda: [], job_name="Stateless tests"
    )
    targeter._diff_text = "+++ b/ci/jobs/functional_tests.py\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1.",
        "01109_exchange_tables.",
    ]


def test_selected_tests_add_smoke_tests_for_store_data_hook_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False, get_changed_files=lambda: [], job_name="Stateless tests"
    )
    targeter._diff_text = "+++ b/ci/jobs/scripts/workflow_hooks/store_data.py\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1.",
        "01109_exchange_tables.",
    ]


def test_selected_tests_add_smoke_tests_for_info_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False, get_changed_files=lambda: [], job_name="Stateless tests"
    )
    targeter._diff_text = "+++ b/ci/praktika/info.py\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1.",
        "01109_exchange_tables.",
    ]


def test_selected_tests_add_smoke_tests_for_rendered_workflow_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False, get_changed_files=lambda: [], job_name="Stateless tests"
    )
    targeter._diff_text = "+++ b/.github/workflows/pull_request.yml\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1.",
        "01109_exchange_tables.",
    ]


def test_selected_tests_do_not_add_smoke_tests_for_query_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False, get_changed_files=lambda: [], job_name="Stateless tests"
    )
    targeter._diff_text = "+++ b/tests/queries/0_stateless/00001_select_1.sql\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1."
    ]


def test_selected_tests_add_feature_smoke_tests_for_harness_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False,
        get_changed_files=lambda: [],
        job_name="Stateless tests (amd_tsan, s3 storage, parallel, selected tests)",
    )
    targeter._diff_text = "+++ b/ci/jobs/scripts/workflow_hooks/filter_job.py\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1.",
        "01109_exchange_tables.",
        "02302_s3_file_pruning.",
        "03741_s3_glob_table_path_pushdown.",
    ]


def test_selected_tests_add_distributed_plan_smoke_tests_for_harness_change():
    targeter = Targeting.__new__(Targeting)
    targeter.info = SimpleNamespace(
        is_local_run=False,
        get_changed_files=lambda: [],
        job_name="Stateless tests (amd_asan_ubsan, distributed plan, parallel, selected tests)",
    )
    targeter._diff_text = "+++ b/ci/jobs/functional_tests.py\n"

    assert targeter.get_changed_tests(include_harness_smoke=True) == [
        "00001_select_1.",
        "01109_exchange_tables.",
        "04367_distributed_plan_merge_scatter_multishard.",
        "04648_distributed_plan_task_error_propagation.",
    ]


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
    suite.explicit_test_selectors_matched = set()

    assert list(suite.get_selected_tests(lambda _name: True)) == []
    assert not suite.explicit_test_selectors_matched


def test_matching_explicit_selector_is_recorded_before_flavor_filter(tmp_path):
    (tmp_path / "00001_existing.sql").touch()
    suite = _TestSuite.__new__(_TestSuite)
    suite.args = SimpleNamespace(test=["00001_existing"])
    suite.suite_path = str(tmp_path)
    suite.render_test_template = lambda _env, _path, name: name
    suite.explicit_test_selectors_matched = set()

    assert list(suite.get_selected_tests(lambda _name: False)) == []
    assert suite.explicit_test_selectors_matched == {"00001_existing"}


def test_each_explicit_selector_must_match_before_skipping(tmp_path):
    (tmp_path / "00001_existing.sql").touch()
    suite = _TestSuite.__new__(_TestSuite)
    suite.args = SimpleNamespace(test=["00001_existing", "99999_no_such_test"])
    suite.suite_path = str(tmp_path)
    suite.render_test_template = lambda _env, _path, name: name
    suite.explicit_test_selectors_matched = set()

    assert list(suite.get_selected_tests(lambda _name: False)) == []
    assert suite.explicit_test_selectors_matched == {"00001_existing"}


def test_pr_workflow_keeps_full_suite_msan_wasmedge_functional_tests():
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
    full_suite_msan_wasmedge_jobs = []
    for name in sanitizer_ft_jobs:
        if "amd_msan, WasmEdge" in name:
            full_suite_msan_wasmedge_jobs.append(name)
            continue
        # The flaky, targeted and azure jobs run their own selection or config.
        assert any(
            marker in name
            for marker in ("selected tests", "flaky check", "targeted", "azure")
        ), f"full-suite sanitizer functional test job in the PR workflow: {name}"

    assert len(full_suite_msan_wasmedge_jobs) == 5
    assert not any("selected tests" in name for name in full_suite_msan_wasmedge_jobs)

    selected_test_jobs = [
        job for job in workflow.jobs if "selected tests" in job.name
    ]
    assert selected_test_jobs

    # Every gating job must exist in the workflow - the trimmed jobs replaced
    # the full-suite ones the gate used to name.
    for name in CORE_BLOCKING_JOB_NAMES:
        assert name in job_names, f"unknown job in the blocking gate: {name}"
