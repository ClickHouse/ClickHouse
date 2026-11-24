"""
Tests for Result.Status enum and Result helper methods.

Adding a new status to Result.Status is a significant change that affects
CI reports, CIDB statistics, GitHub commit statuses, Slack notifications,
and the event feed. Think twice before adding one — and update all mapping
tables (GH._STATUS_TO_GH, CIDB._STATUS_TO_CIDB) and these tests.
"""

import json
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result, ResultInfo


# The canonical set of all statuses.  If you add a new status, you MUST
# update this set, the GH and CIDB mapping tables, json.html rendering,
# the Slack lambda, and the event sanitizer.  The test below will remind you.
ALL_STATUSES = {
    Result.Status.OK,
    Result.Status.FAIL,
    Result.Status.SKIPPED,
    Result.Status.ERROR,
    Result.Status.UNKNOWN,
    Result.Status.XFAIL,
    Result.Status.XPASS,
    Result.Status.PENDING,
    Result.Status.RUNNING,
    Result.Status.DROPPED,
}


def _get_class_constants(cls):
    """Return set of all public string constants defined on a class."""
    return {
        getattr(cls, k)
        for k in dir(cls)
        if not k.startswith("_") and isinstance(getattr(cls, k), str)
    }


def test_all_statuses_accounted_for():
    """No status was added to Result.Status without updating ALL_STATUSES."""
    actual = _get_class_constants(Result.Status)
    assert actual == ALL_STATUSES, (
        f"Result.Status has changed! New: {actual - ALL_STATUSES}, "
        f"Removed: {ALL_STATUSES - actual}. "
        "Update ALL_STATUSES, GH/CIDB mappings, json.html, Slack lambda, and event sanitizer."
    )


def test_status_values_are_uppercase():
    for status in ALL_STATUSES:
        assert status == status.upper(), f"Status {status!r} must be uppercase"


def test_status_values_are_unique():
    values = list(ALL_STATUSES)
    assert len(values) == len(set(values)), "Duplicate status values"


# --- is_ok / is_success / is_failure / is_error ---

def test_is_ok():
    ok_statuses = {Result.Status.OK, Result.Status.SKIPPED, Result.Status.XFAIL}
    not_ok = ALL_STATUSES - ok_statuses
    for s in ok_statuses:
        assert Result("t", s).is_ok(), f"{s} should be ok"
    for s in not_ok:
        assert not Result("t", s).is_ok(), f"{s} should not be ok"


def test_is_success():
    success_statuses = {Result.Status.OK, Result.Status.XFAIL}
    for s in success_statuses:
        assert Result("t", s).is_success(), f"{s} should be success"
    for s in ALL_STATUSES - success_statuses:
        assert not Result("t", s).is_success(), f"{s} should not be success"


def test_is_failure():
    fail_statuses = {Result.Status.FAIL, Result.Status.XPASS}
    for s in fail_statuses:
        assert Result("t", s).is_failure(), f"{s} should be failure"
    for s in ALL_STATUSES - fail_statuses:
        assert not Result("t", s).is_failure(), f"{s} should not be failure"


def test_is_error():
    assert Result("t", Result.Status.ERROR).is_error()
    for s in ALL_STATUSES - {Result.Status.ERROR}:
        assert not Result("t", s).is_error(), f"{s} should not be error"


def test_is_pending():
    assert Result("t", Result.Status.PENDING).is_pending()
    for s in ALL_STATUSES - {Result.Status.PENDING}:
        assert not Result("t", s).is_pending(), f"{s} should not be pending"


def test_is_running():
    assert Result("t", Result.Status.RUNNING).is_running()
    for s in ALL_STATUSES - {Result.Status.RUNNING}:
        assert not Result("t", s).is_running(), f"{s} should not be running"


def test_is_dropped():
    assert Result("t", Result.Status.DROPPED).is_dropped()
    for s in ALL_STATUSES - {Result.Status.DROPPED}:
        assert not Result("t", s).is_dropped(), f"{s} should not be dropped"


def test_is_skipped():
    assert Result("t", Result.Status.SKIPPED).is_skipped()
    for s in ALL_STATUSES - {Result.Status.SKIPPED}:
        assert not Result("t", s).is_skipped(), f"{s} should not be skipped"


def test_is_completed():
    not_completed = {Result.Status.PENDING, Result.Status.RUNNING}
    for s in not_completed:
        assert not Result("t", s).is_completed(), f"{s} should not be completed"
    for s in ALL_STATUSES - not_completed:
        assert Result("t", s).is_completed(), f"{s} should be completed"


# --- create_from ---

def test_create_from_bool_true():
    r = Result.create_from(name="t", status=True)
    assert r.status == Result.Status.OK


def test_create_from_bool_false():
    r = Result.create_from(name="t", status=False)
    assert r.status == Result.Status.FAIL


def test_create_from_aggregates_ok():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.SKIPPED)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.OK


def test_create_from_aggregates_fail():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.FAIL)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.FAIL


def test_create_from_aggregates_error():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.ERROR)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.ERROR


def test_create_from_aggregates_xfail_is_ok():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.XFAIL)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.OK


def test_create_from_aggregates_xpass_is_fail():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.XPASS)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.FAIL


def test_create_from_aggregates_unknown_is_fail():
    subs = [Result("a", Result.Status.OK), Result("b", Result.Status.UNKNOWN)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.FAIL


def test_create_from_error_takes_priority():
    subs = [Result("a", Result.Status.FAIL), Result("b", Result.Status.ERROR)]
    r = Result.create_from(name="t", results=subs)
    assert r.status == Result.Status.ERROR


# --- set_success / set_failed / set_error ---

def test_set_success():
    r = Result("t", Result.Status.FAIL)
    r.set_success()
    assert r.status == Result.Status.OK


def test_set_failed():
    r = Result("t", Result.Status.OK)
    r.set_failed()
    assert r.status == Result.Status.FAIL


def test_set_error():
    r = Result("t", Result.Status.OK)
    r.set_error()
    assert r.status == Result.Status.ERROR


# --- with_info_from_results: hook failure reason must reach the aggregate node ---
# Regression guard for the "Pre Hooks / Post Hooks" empty test_context_raw bug:
# CIDB reads Result.info from the aggregate node, so a failing sub-hook's error
# message (e.g. "ERROR: Change category is missing or invalid") must be lifted
# into the parent when with_info_from_results=True.

def test_create_from_lifts_child_info_when_requested():
    subs = [
        Result("hook_ok", Result.Status.OK),
        Result("hook_bad", Result.Status.FAIL, info="ERROR: something is wrong"),
    ]
    r = Result.create_from(name="Pre Hooks", results=subs, with_info_from_results=True)
    assert r.status == Result.Status.FAIL
    assert "ERROR: something is wrong" in r.info
    assert "hook_bad" in r.info


def test_create_from_omits_child_info_by_default():
    subs = [Result("hook_bad", Result.Status.FAIL, info="ERROR: something is wrong")]
    r = Result.create_from(name="Pre Hooks", results=subs)
    assert r.info == ""


def test_update_sub_result_drops_only_heavy_ext_keys():
    """Embedding a job as a workflow sub-result drops the heavy `metrics`
    timeline but keeps every lightweight key, so the workflow report and the
    embedded-node fallback in json.html keep their warnings/errors/notes/run_url."""
    workflow = Result("wf", Result.Status.PENDING, results=[Result("job", Result.Status.PENDING)])
    job = Result("job", Result.Status.OK)
    job.ext = {
        "labels": ["release"],
        "hlabels": [["flaky", "seen before"]],
        "storage_usage": {"uploaded": 42, "uploaded_details": {"a.deb": 42}},
        "metrics": {"heavy": list(range(1000))},
        "warnings": [{"message": "w", "from": "job"}],
        "errors": [{"message": "e", "from": "job"}],
        "notes": [{"message": "n", "from": "job"}],
        "run_url": "https://example/run",
    }

    workflow.update_sub_result(job, drop_nested_results=True)

    embedded_ext = workflow.results[0].ext
    assert "metrics" not in embedded_ext
    assert set(embedded_ext) == {
        "labels",
        "hlabels",
        "storage_usage",
        "warnings",
        "errors",
        "notes",
        "run_url",
    }
    # The job's own result is untouched - its full report is uploaded separately.
    assert "metrics" in job.ext


def test_update_sub_result_preserves_full_ext_without_dropping():
    """The default path (drop_nested_results=False) keeps the ext as is."""
    workflow = Result("wf", Result.Status.PENDING, results=[Result("job", Result.Status.PENDING)])
    job = Result("job", Result.Status.OK)
    job.ext = {"metrics": {"heavy": 1}, "labels": ["release"]}

    workflow.update_sub_result(job)

    assert workflow.results[0].ext == {"metrics": {"heavy": 1}, "labels": ["release"]}


def test_unfinished_job_carries_its_own_error_in_the_workflow_report(
    tmp_path, monkeypatch
):
    """A job that never uploaded a report gets the reason on its own node.

    The workflow report is the only place such a job appears, and json.html
    renders each node's ext["errors"], so the entry must reach the uploaded
    file - not merely the in-memory object.
    """
    import ci.praktika.native_jobs as native_jobs
    from ci.praktika.settings import Settings

    dead = "AST fuzzer (amd_debug, targeted)"
    healthy = "AST fuzzer (amd_debug)"
    workflow_result = Result(
        "PR",
        Result.Status.RUNNING,
        results=[
            Result(dead, Result.Status.RUNNING, start_time=1.0),
            Result(healthy, Result.Status.OK, start_time=1.0, duration=5.0),
        ],
    )

    # Result.dump() writes into Settings.TEMP_DIR.
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    # Absent status file -> no GitHub verdict for the job -> the error branch.
    monkeypatch.setattr(Settings, "WORKFLOW_STATUS_FILE", str(tmp_path / "absent.json"))

    workflow_errors = []

    class Env:
        PR_BODY = ""

        def get_needs_statuses(self):
            return {}

        def add_workflow_error(self, message, source=""):
            workflow_errors.append((message, source))

    uploads = []

    class ResultS3:
        @classmethod
        def copy_result_from_s3_with_version(cls, _path):
            return 7

        @classmethod
        def copy_result_to_s3_with_version(cls, result, version, no_strict=False):
            result.dump()
            with open(result.file_name(), "r", encoding="utf8") as f:
                uploads.append(json.load(f))
            return True

    class Workflow:
        name = "PR"
        post_hooks = []
        enable_merge_ready_status = False
        enable_open_issues_check = False

        def get_job(self, _name):
            return types.SimpleNamespace(allow_failure=False)

    monkeypatch.setattr(
        native_jobs, "_Environment", types.SimpleNamespace(get=lambda: Env())
    )
    monkeypatch.setattr(native_jobs, "_ResultS3", ResultS3)
    monkeypatch.setattr(
        Result, "from_fs", classmethod(lambda cls, name: workflow_result)
    )

    native_jobs._finish_workflow(Workflow(), "Finish Workflow")

    assert len(uploads) == 1, "the workflow report must be re-uploaded"
    nodes = {node["name"]: node for node in uploads[0]["results"]}
    assert nodes[dead]["status"] == Result.Status.ERROR
    assert nodes[dead]["ext"]["errors"] == [
        {"message": ResultInfo.NOT_FINALIZED, "from": dead}
    ]
    assert "errors" not in nodes[healthy]["ext"]
    # The workflow node keeps the summary attribution.
    assert workflow_errors == [(ResultInfo.NOT_FINALIZED, dead)]
