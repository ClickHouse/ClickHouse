"""
Tests for the unresolved-review-threads CI gate
(https://github.com/ClickHouse/ClickHouse/issues/114724).

While a PR has unresolved review threads (and no `ignore-unresolved-threads`
label), the PR pipeline runs only builds and the preliminary checks, and the
merge gate in `can_be_merged.py` blocks the merge. These tests pin the two
decision functions in `review_threads.py` and their integration into
`should_skip_job`, in particular:

- builds, the style check, the fast test and the `Code Review` job must keep
  running in the limited pipeline; everything else must be skipped;
- the merge verdict must stay "blocked" when the pipeline was limited at
  config time, even if the threads were all resolved while it ran - the full
  suite did not run, so the PR must not become mergeable without a re-run;
- every verdict description must fit the 80-character commit-status budget of
  `GH.post_commit_status`.
"""

import json
import os
import shutil
import subprocess
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
    record_limited_pipeline_status,
    review_threads_gate_bypassed,
    should_limit_pipeline,
    store_gate_state,
)
from ci.defs.job_configs import JobConfigs
from ci.praktika.gh import GH
from ci.praktika.result import Result

# The 80-character truncation applied by GH.post_commit_status.
STATUS_DESCRIPTION_LIMIT = 80


class FakeInfo:
    def __init__(self, kv=None, labels=None):
        self.kv = kv or {}
        self.pr_labels = labels or []
        self.pr_body = ""
        self.pr_number = 12345
        self.repo_name = "ClickHouse/ClickHouse"
        self.sha = "0000000000000000000000000000000000000000"
        self.notes = []

    def store_kv_data(self, key, value):
        self.kv[key] = value

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
def fake_info(monkeypatch):
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
    # The empty-merge-commit check for `Code Review` queries the GitHub API.
    monkeypatch.setattr(filter_job, "_is_empty_merge_commit", lambda sha: False)
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
    assert 'workflows: ["PR"]' in rerun_workflow
    assert "Review Threads Signal" not in rerun_workflow
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
    assert '"Failed: review threads only"' in rerun_workflow
    # `Finish Workflow` aggregates every post hook. It must never be used as
    # proof that the review-thread hook was the only failure, because that
    # would allow a resolved thread to clear another merge blocker.
    assert 'Failed: Workflow Post Hook' not in rerun_workflow
    assert 'failed_workflow_jobs' not in rerun_workflow
    assert 'last_pr_run_id' not in rerun_workflow
    assert 'actions/runs/$run_id/rerun' in rerun_workflow
    assert 'Failed to verify re-run of $run_id' in rerun_workflow
    assert '[ "$failed_workflow_jobs" = "Finish Workflow" ]' in retry_workflow
    assert 'select(.created_at >= $from and .created_at <= $to)' in retry_workflow
    assert 'finish_completed_at=$(echo "$finish_window" | cut -f2)' in retry_workflow
    pull_request_workflow = (repository_root / "ci/workflows/pull_request.py").read_text()
    assert 'can_be_merged.py --review-threads' in pull_request_workflow
    assert '"ci/jobs/scripts/workflow_hooks/can_be_merged.py --review-threads"' in (
        repository_root / "ci/praktika/native_jobs.py"
    ).read_text()
    assert '_REVIEW_THREADS_ONLY_POST_HOOK_FAILURE = "Failed: review threads only"' in (
        repository_root / "ci/praktika/native_jobs.py"
    ).read_text()


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
        "Build (wasm_parser)",
        "Style check",
        "Fast test",
        "Code Review",
    ):
        skip, reason = filter_job.should_skip_job(job_name)
        assert not skip, f"{job_name}: {reason}"


def test_limited_pipeline_allowlist_covers_every_pr_build_job():
    """`REVIEW_THREADS_BUILD_JOBS` must cover every build lane of the PR
    workflow - a lane missing from the allowlist (as `Build (wasm_parser)`
    once was) silently defers its breakage past the limited pipeline. The
    allowlist is job-config based on purpose, so the non-build `Build profile
    diff` job stays out; this cross-checks it against the workflow by name.
    """
    from ci.workflows.pull_request import workflow as pr_workflow

    build_jobs = [
        job.name
        for job in pr_workflow.jobs
        if job.name.startswith("Build (") or job.name.startswith("Build Toolchain")
    ]
    assert build_jobs, "no build jobs found in the PR workflow - update this test"
    missing = [
        name
        for name in build_jobs
        if name not in filter_job.REVIEW_THREADS_BUILD_JOBS
    ]
    assert not missing, f"build jobs missing from REVIEW_THREADS_BUILD_JOBS: {missing}"


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
    assert "before this run finishes" in fake_info.notes[0]
    assert "re-run CI manually" in fake_info.notes[0]


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


def test_limited_pipeline_status_write_failure_does_not_enable_filtering(monkeypatch):
    info = FakeInfo()
    monkeypatch.setattr(GH, "post_commit_status", lambda **_: False)

    assert not record_limited_pipeline_status(info, 1)


def test_review_threads_marker_is_independent_of_another_merge_gate(monkeypatch):
    from ci.jobs.scripts.workflow_hooks import can_be_merged

    info = FakeInfo(kv={KV_PIPELINE_LIMITED: False})
    posted = []
    monkeypatch.setattr(can_be_merged, "Info", lambda: info)
    monkeypatch.setattr(can_be_merged, "fetch_thread_state", lambda _: (1, False, False))
    monkeypatch.setattr(
        can_be_merged.GH,
        "post_commit_status",
        lambda **kwargs: posted.append(kwargs) or True,
    )

    assert not can_be_merged.check_review_threads()
    assert posted[0]["description"] == "1 unresolved review thread(s)"


def test_live_force_all_survives_a_failing_thread_count(monkeypatch):
    """A rerun with `ci-force-all` must bypass filters even if the thread query fails.

    `native_jobs.py` only falls back to the (stale on reruns) event payload
    when the kv data is missing, so the label state must be recorded before
    the independent unresolved-thread query that is allowed to fail.
    """
    info = FakeInfo(labels=[])
    monkeypatch.setattr(
        GH,
        "get_output_with_retries",
        lambda *args, **kwargs: '{"labels": [{"name": "ci-force-all"}]}',
    )

    def failing_threads(**kwargs):
        raise RuntimeError("GitHub API is down")

    monkeypatch.setattr(GH, "list_pr_review_threads", failing_threads)

    store_gate_state(info)

    assert info.get_kv_data(KV_FORCE_ALL) is True
    # The gate itself must not engage without the thread count.
    assert info.get_kv_data(KV_UNRESOLVED_COUNT) is None
    assert not should_limit_pipeline(
        info.get_kv_data(KV_UNRESOLVED_COUNT) or 0,
        bool(info.get_kv_data(KV_OVERRIDE)),
    )

    # Workflow filter hooks, changed-file filtering and the cache lookup all
    # resolve `force_all` from this kv data, and only consult the stale event
    # payload when it is missing.
    native_jobs = (
        Path(__file__).resolve().parents[2] / "ci/praktika/native_jobs.py"
    ).read_text()
    assert (
        native_jobs.count(
            'force_all_kv = Info().get_kv_data("unresolved_review_threads_force_all")'
        )
        == 3
    )
    assert native_jobs.count("if force_all_kv is None") == 3


def test_config_run_fails_when_the_label_state_is_unknown(monkeypatch):
    """Without the live labels there is no safe default - fail the config run.

    The event payload is stale on re-runs, so consulting it can keep narrowing
    labels (`do not test`, `ci-build`) that were removed and miss a
    `ci-force-all` / `ignore-unresolved-threads` that was added - the run could
    finish green without ever running the full suite. Impersonating
    `ci-force-all` instead would *widen* the run (opt-in jobs, ignored
    narrowing labels). The pre-hook must therefore propagate the error and no
    stale-consultable state may be left behind.
    """
    info = FakeInfo(labels=[])

    def failing_labels(*args, **kwargs):
        raise RuntimeError("GitHub API is down")

    monkeypatch.setattr(GH, "get_output_with_retries", failing_labels)
    monkeypatch.setattr(
        GH,
        "list_pr_review_threads",
        lambda **kwargs: pytest.fail("must not query threads without the labels"),
    )
    monkeypatch.setattr(
        GH, "post_commit_status", lambda **_: pytest.fail("must not post a marker")
    )

    with pytest.raises(RuntimeError, match="GitHub API is down"):
        store_gate_state(info)

    assert info.get_kv_data(KV_UNRESOLVED_COUNT) is None
    assert info.get_kv_data(KV_FORCE_ALL) is None
    assert info.get_kv_data(KV_OVERRIDE) is None


def test_force_all_resolution_is_strict_at_every_site():
    """`native_jobs.py` resolves `force_all` in three places (the workflow
    filter hooks, the changed-file filtering and the CI cache lookup). Each
    must trust the kv data recorded from the live labels by the
    `review_threads.py` pre-hook - which fails the config run when the fetch
    fails, so no "unknown" sentinel handling may reappear - and fall back to
    the event payload only when the kv data is missing (workflows without the
    pre-hook).
    """
    native_jobs = (
        Path(__file__).resolve().parents[2] / "ci/praktika/native_jobs.py"
    ).read_text()
    strict = "else force_all_kv is True\n"
    assert native_jobs.count(strict) == 3
    assert "unknown" not in native_jobs
    # No site is left with a truthiness-based resolution.
    assert "else bool(force_all_kv)" not in native_jobs


def _retry_marker_state(statuses, started_at, completed_at):
    """Run the retry-suppression predicate of `retry_infra_failures.yml`."""
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github/workflows/retry_infra_failures.yml"
    ).read_text()
    anchor = 'marker_state=$(echo "$statuses" | jq -r'
    assert anchor in workflow, "the retry suppression predicate moved - update this test"
    jq_filter = workflow[workflow.index(anchor) :].split("'")[1]
    result = subprocess.run(
        ["jq", "-r", "--arg", "from", started_at, "--arg", "to", completed_at, jq_filter],
        input=json.dumps(statuses),
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


@pytest.mark.skipif(shutil.which("jq") is None, reason="jq is not installed")
def test_retry_suppression_only_matches_the_failed_attempt():
    """A later status refresh must not be attributed to the failed attempt."""
    own = {
        "context": "Review Threads",
        "state": "failure",
        "created_at": "2026-08-20T10:00:30Z",
    }
    later = {
        "context": "Review Threads",
        "state": "failure",
        "created_at": "2026-08-20T11:30:00Z",
    }
    earlier = {
        "context": "Review Threads",
        "state": "failure",
        "created_at": "2026-08-20T09:00:00Z",
    }
    other = {
        "context": "Mergeable Check",
        "state": "failure",
        "created_at": "2026-08-20T10:00:30Z",
    }
    started, completed = "2026-08-20T10:00:00Z", "2026-08-20T10:01:00Z"

    assert _retry_marker_state([own], started, completed) == "failure"
    # A status written after the failed `Finish Workflow` completed belongs to
    # a later reconciliation, so an infra failure must still be retried.
    assert _retry_marker_state([later], started, completed) == ""
    assert _retry_marker_state([earlier], started, completed) == ""
    assert _retry_marker_state([other], started, completed) == ""
    assert _retry_marker_state([earlier, own, later], started, completed) == "failure"


def test_toolchain_builds_stay_opt_in_under_the_review_threads_gate(fake_info):
    """The limited pipeline may only shrink the PR surface, never widen it."""
    toolchain_job = JobConfigs.toolchain_build_jobs[0].name
    assert JobNames.BUILD_TOOLCHAIN in toolchain_job

    skip, reason = filter_job.should_skip_job(toolchain_job)
    assert skip
    assert Labels.CI_TOOLCHAIN in reason

    # ... and the label still opts in, gate or no gate.
    fake_info.pr_labels = [Labels.CI_TOOLCHAIN]
    skip, reason = filter_job.should_skip_job(toolchain_job)
    assert not skip, reason

    fake_info.pr_labels = []
    fake_info.kv[KV_UNRESOLVED_COUNT] = 0
    skip, reason = filter_job.should_skip_job(toolchain_job)
    assert skip
    assert Labels.CI_TOOLCHAIN in reason


def test_only_the_policy_verdict_is_rewritten_into_the_review_threads_marker():
    """An infrastructure failure of the same hook must keep the generic status.

    The reconciliation workflow clears `Failed: review threads only` once the
    threads are resolved; a failure that had nothing to do with the threads
    (a GitHub API outage inside the hook) must not be cleared that way.
    """
    from ci.praktika.native_jobs import (
        _REVIEW_THREADS_POLICY_FAILURE_MARKER,
        _REVIEW_THREADS_POST_HOOK,
        _review_threads_were_the_only_failed_post_hook,
    )
    from ci.jobs.scripts.workflow_hooks.review_threads import POLICY_FAILURE_MARKER

    assert _REVIEW_THREADS_POLICY_FAILURE_MARKER == POLICY_FAILURE_MARKER

    def result(name, ok, info=""):
        return Result.create_new(
            name=name,
            status=Result.Status.OK if ok else Result.Status.FAIL,
            info=info,
        )

    policy_failure = result(
        _REVIEW_THREADS_POST_HOOK,
        False,
        f"Review threads gate: ...\nWARNING: unresolved review threads, merge not "
        f"allowed\n{POLICY_FAILURE_MARKER}",
    )
    infra_failure = result(
        _REVIEW_THREADS_POST_HOOK,
        False,
        "Traceback (most recent call last):\nRuntimeError: GitHub API is down",
    )
    other_ok = result("ci/jobs/scripts/workflow_hooks/can_be_merged.py", True)
    other_failure = result("ci/jobs/scripts/workflow_hooks/can_be_merged.py", False)

    assert _review_threads_were_the_only_failed_post_hook([policy_failure, other_ok])
    assert not _review_threads_were_the_only_failed_post_hook([infra_failure, other_ok])
    assert not _review_threads_were_the_only_failed_post_hook(
        [policy_failure, other_failure]
    )
    assert not _review_threads_were_the_only_failed_post_hook([other_ok])


def test_the_policy_marker_is_printed_only_when_the_gate_blocks(monkeypatch, capsys):
    from ci.jobs.scripts.workflow_hooks import can_be_merged

    info = FakeInfo(kv={KV_PIPELINE_LIMITED: False})
    monkeypatch.setattr(can_be_merged, "Info", lambda: info)
    monkeypatch.setattr(can_be_merged.GH, "post_commit_status", lambda **_: True)

    monkeypatch.setattr(can_be_merged, "fetch_thread_state", lambda _: (1, False, False))
    assert not can_be_merged.check_review_threads()
    assert can_be_merged.POLICY_FAILURE_MARKER in capsys.readouterr().out

    monkeypatch.setattr(can_be_merged, "fetch_thread_state", lambda _: (0, False, False))
    assert can_be_merged.check_review_threads()
    assert can_be_merged.POLICY_FAILURE_MARKER not in capsys.readouterr().out

    # An infrastructure failure propagates instead of printing the marker.
    def failing_state(_):
        raise RuntimeError("GitHub API is down")

    monkeypatch.setattr(can_be_merged, "fetch_thread_state", failing_state)
    with pytest.raises(RuntimeError):
        can_be_merged.check_review_threads()
    assert can_be_merged.POLICY_FAILURE_MARKER not in capsys.readouterr().out
