"""
Tests for the unresolved-review-threads CI gate
(https://github.com/ClickHouse/ClickHouse/issues/114724).

While a PR has unresolved review threads (and no `ignore-unresolved-threads`
label), the PR pipeline runs only builds and the preliminary checks, and the
merge gate in `can_be_merged.py` blocks the merge. These tests pin the two
decision functions in `review_threads.py` and their integration into
`should_skip_job`, in particular:

- builds, the style check, the fast test and the `Code Review` job must keep
  running in the limited pipeline (the AI review resolving its own threads is
  what re-triggers the full suite), everything else must be skipped;
- the merge verdict must stay "blocked" when the pipeline was limited at
  config time, even if the threads were all resolved while it ran - the full
  suite did not run, so the PR must not become mergeable without a re-run;
- every verdict description must fit the 80-character commit-status budget of
  `GH.post_commit_status`.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on
# the path for `import praktika` to resolve to `ci/praktika`. CI runs
# configure this via the praktika runner; we replicate it here.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks import filter_job
from ci.defs.defs import JobNames
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.jobs.scripts.workflow_hooks.review_threads import (
    KV_FORCE_ALL,
    KV_OVERRIDE,
    KV_PIPELINE_LIMITED,
    KV_UNRESOLVED_COUNT,
    fetch_thread_state,
    get_unresolved_review_threads_count,
    merge_gate_verdict,
    review_threads_gate_bypassed,
    should_limit_pipeline,
)
from ci.praktika.gh import GH

# The 80-character truncation applied by GH.post_commit_status.
STATUS_DESCRIPTION_LIMIT = 80


class FakeInfo:
    def __init__(self, kv=None, labels=None):
        self.kv = kv or {}
        self.pr_labels = labels or []
        self.pr_body = ""
        self.pr_number = 12345
        self.repo_name = "ClickHouse/ClickHouse"
        self.notes = []

    def get_kv_data(self, key=None):
        if key:
            return self.kv.get(key)
        return self.kv

    def get_changed_files(self):
        return self.kv.get("changed_files")

    def add_workflow_note(self, message):
        self.notes.append(message)

    def get_report_url(self):
        return ""


@pytest.fixture
def fake_info():
    saved_info = filter_job._info_cache
    saved_notes = filter_job._pipeline_note_labels
    info = FakeInfo(
        kv={
            "changed_files": ["src/Core/Settings.cpp"],
            KV_UNRESOLVED_COUNT: 3,
            KV_OVERRIDE: False,
        }
    )
    filter_job._info_cache = info
    filter_job._pipeline_note_labels = set()
    yield info
    filter_job._info_cache = saved_info
    filter_job._pipeline_note_labels = saved_notes


def test_should_limit_pipeline():
    assert should_limit_pipeline(1, False)
    assert not should_limit_pipeline(0, False)
    assert not should_limit_pipeline(1, True)
    assert not should_limit_pipeline(0, True)


def test_only_dedicated_label_bypasses_the_gate():
    assert not review_threads_gate_bypassed([Labels.CI_FORCE_ALL])
    assert review_threads_gate_bypassed([Labels.IGNORE_UNRESOLVED_THREADS])
    assert not review_threads_gate_bypassed([])
    assert should_limit_pipeline(1, review_threads_gate_bypassed([Labels.CI_FORCE_ALL]))
    assert merge_gate_verdict(False, 1, False)[0]


def test_review_threads_workflows_preserve_override_and_infra_retry_behavior():
    repository_root = Path(__file__).resolve().parents[2]
    rerun_workflow = (
        repository_root / ".github/workflows/rerun_on_review_threads.yml"
    ).read_text()
    retry_workflow = (
        repository_root / ".github/workflows/retry_infra_failures.yml"
    ).read_text()

    assert "OVERRIDE_LABEL: ignore-unresolved-threads" in rerun_workflow
    assert "FORCE_ALL_LABEL" not in rerun_workflow
    assert "api_with_retries" in rerun_workflow
    assert "|| true" not in rerun_workflow
    assert 'pr_data=$(api_with_retries "repos/$GH_REPO/pulls/$pr")' in rerun_workflow
    assert "unresolved=$(api_with_retries graphql --paginate" in rerun_workflow
    assert 'runs=$(api_with_retries "repos/$GH_REPO/actions/runs?' in rerun_workflow
    assert 'pipeline_limited=false' in rerun_workflow
    assert '"running the limited CI suite"' in rerun_workflow
    assert 'Refreshed the review-thread status without rerunning full CI.' in rerun_workflow
    assert 'if [ "$desired_blocked" = "true" ]; then' in rerun_workflow
    assert 'post_status()' in rerun_workflow
    assert 'api_with_retries --method POST "repos/$GH_REPO/statuses/$head_sha"' in rerun_workflow
    assert 'review threads: $unresolved unresolved review thread(s)' in rerun_workflow
    assert 'last_pr_conclusion' in rerun_workflow
    assert 'last_pr_run_id' in rerun_workflow
    assert 'Failed: Workflow Post Hook' in rerun_workflow
    assert 'failed_workflow_jobs' in rerun_workflow
    assert '[ "$failed_workflow_jobs" = "Finish Workflow" ]' in rerun_workflow
    assert 'actions/runs/$run_id/rerun' in rerun_workflow
    assert 'Failed to verify re-run of $run_id' in rerun_workflow
    assert '[ "$failed_workflow_jobs" = "Finish Workflow" ]' in retry_workflow
    assert 'select(.created_at >= $finish_started_at)' in retry_workflow


def test_pipeline_limited_is_recorded_only_after_an_actual_review_thread_skip():
    repository_root = Path(__file__).resolve().parents[2]
    native_jobs = (repository_root / "ci/praktika/native_jobs.py").read_text()

    assert 'if "unresolved review thread" in reason:' in native_jobs
    assert 'review_threads_pipeline_limited = True' in native_jobs


def test_count_unresolved_threads(monkeypatch):
    threads = [
        {"isResolved": True},
        {"isResolved": False},
        {"isResolved": False},
    ]
    monkeypatch.setattr(
        GH, "list_pr_review_threads", lambda pr=None, repo=None: threads
    )
    assert get_unresolved_review_threads_count(pr=1, repo="a/b") == 2


def test_fetch_thread_state_uses_live_force_all_label_on_rerun(monkeypatch):
    """A rerun keeps the original event labels, so this must query GitHub."""
    info = FakeInfo(labels=[])
    monkeypatch.setattr(
        GH,
        "get_output_with_retries",
        lambda *args, **kwargs: '{"labels": [{"name": "ci-force-all"}]}',
    )
    monkeypatch.setattr(
        GH, "list_pr_review_threads", lambda **kwargs: [{"isResolved": False}]
    )

    assert fetch_thread_state(info) == (1, False, True)


# --- should_skip_job integration ---


def test_limited_pipeline_skips_test_jobs(fake_info):
    for job_name in (
        "Stateless tests (amd_debug, parallel)",
        "Integration tests (asan, old analyzer, 1/6)",
        "Stress test (amd_debug)",
        "Docs check",
        "Upgrade check (amd_msan)",
        "Performance Comparison (release, arm, 1/3)",
    ):
        skip, reason = filter_job.should_skip_job(job_name)
        assert skip, job_name
        assert "unresolved review thread" in reason, job_name


def test_limited_pipeline_keeps_builds_and_preliminary_jobs(fake_info):
    for job_name in (
        "Build (amd_debug)",
        "Build (arm_tidy)",
        "Style check",
        "Fast test",
        "Code Review",
    ):
        skip, reason = filter_job.should_skip_job(job_name)
        assert not skip, f"{job_name}: {reason}"


@pytest.mark.parametrize(
    "label",
    [
        Labels.CI_BUILD,
        Labels.DO_NOT_TEST,
        Labels.NO_FAST_TESTS,
        Labels.CI_INTEGRATION,
    ],
)
def test_limited_pipeline_keeps_allowlist_with_other_filter_labels(fake_info, label):
    fake_info.pr_labels.append(label)
    for job_name in (
        "Build (amd_debug)",
        "Style check",
        "Fast test",
        JobNames.CODE_REVIEW,
    ):
        skip, reason = filter_job.should_skip_job(job_name)
        assert not skip, f"{job_name}: {reason}"


def test_limited_pipeline_skips_build_profile_diff(fake_info):
    skip, reason = filter_job.should_skip_job(JobNames.BUILD_PROFILE_DIFF)
    assert skip
    assert "unresolved review thread" in reason


def test_limited_pipeline_adds_single_workflow_note(fake_info):
    filter_job.should_skip_job("Stateless tests (amd_debug, parallel)")
    filter_job.should_skip_job("Stress test (amd_debug)")
    assert len(fake_info.notes) == 1
    assert Labels.IGNORE_UNRESOLVED_THREADS in fake_info.notes[0]


def test_override_label_disables_the_gate(fake_info):
    fake_info.kv[KV_OVERRIDE] = True
    skip, reason = filter_job.should_skip_job("Stateless tests (amd_debug, parallel)")
    assert not skip, reason


def test_no_unresolved_threads_disables_the_gate(fake_info):
    fake_info.kv[KV_UNRESOLVED_COUNT] = 0
    skip, reason = filter_job.should_skip_job("Stateless tests (amd_debug, parallel)")
    assert not skip, reason


def test_missing_kv_data_disables_the_gate(fake_info):
    # The pre-hook failed to fetch the thread state (e.g. a GitHub API outage):
    # fail toward more testing and do not skip anything.
    del fake_info.kv[KV_UNRESOLVED_COUNT]
    del fake_info.kv[KV_OVERRIDE]
    skip, reason = filter_job.should_skip_job("Stateless tests (amd_debug, parallel)")
    assert not skip, reason


# --- merge gate verdict ---


def test_merge_gate_verdict_truth_table():
    # (config_limited, unresolved_now, override_now) -> blocked
    cases = {
        (False, 0, False): False,
        (False, 0, True): False,
        (False, 2, False): True,
        (False, 2, True): False,
        # A limited pipeline never becomes mergeable within the same run: the
        # full suite did not run, whatever happened to the threads meanwhile.
        (True, 0, False): True,
        (True, 0, True): True,
        (True, 2, False): True,
        (True, 2, True): True,
    }
    for (config_limited, unresolved_now, override_now), expected in cases.items():
        blocked, description = merge_gate_verdict(
            config_limited, unresolved_now, override_now
        )
        assert blocked == expected, (config_limited, unresolved_now, override_now)
        assert description
        assert len(description) <= STATUS_DESCRIPTION_LIMIT, description


def test_forced_full_pipeline_is_not_recorded_as_limited():
    info = FakeInfo(
        kv={
            "changed_files": ["src/Core/Settings.cpp"],
            KV_UNRESOLVED_COUNT: 3,
            KV_OVERRIDE: False,
            KV_PIPELINE_LIMITED: False,
            KV_FORCE_ALL: True,
        },
        labels=[Labels.CI_FORCE_ALL],
    )
    assert not info.get_kv_data(KV_PIPELINE_LIMITED)
    assert info.get_kv_data(KV_FORCE_ALL)
    assert not merge_gate_verdict(info.get_kv_data(KV_PIPELINE_LIMITED), 0, False)[0]


def test_merge_gate_descriptions_fit_status_budget():
    for config_limited in (False, True):
        for override_now in (False, True):
            _, description = merge_gate_verdict(config_limited, 100000, override_now)
            assert len(description) <= STATUS_DESCRIPTION_LIMIT, description


def test_check_review_threads_fails_when_the_status_write_fails(monkeypatch):
    from ci.jobs.scripts.workflow_hooks import can_be_merged

    info = FakeInfo(kv={KV_PIPELINE_LIMITED: False})
    monkeypatch.setattr(can_be_merged, "Info", lambda: info)
    monkeypatch.setattr(can_be_merged, "fetch_thread_state", lambda _: (0, False, False))
    monkeypatch.setattr(can_be_merged.GH, "post_commit_status", lambda **_: False)

    with pytest.raises(RuntimeError, match="Review Threads"):
        can_be_merged.check_review_threads()
