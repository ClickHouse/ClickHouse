"""
Tests for the `release_base` delta gate in `ci.jobs.performance_tests`.

The `release_base` perf comparison is cumulative since the release branch
point, so its status is gated on the growth of the "slower" count against the
previous master run instead of on an absolute count. Everything the gate
decides hinges on picking a *comparable* predecessor, which is what these
tests pin:

  * only a genuine `report.py` summary can serve as a baseline - sentinels
    ("No status in report.") and runs with errors must not silently become
    the left side of the delta;
  * a transport failure says nothing about the commit and must stop the walk
    (otherwise the delta is computed against an older run and red stops
    blaming the introducing commit);
  * a missing artifact is skipped only when the job provably never started at
    that commit - no `MasterCI` run for it (a push event covers a batch of
    commits and the workflow runs on the head only), or the job `SKIPPED` by
    change filtering. A predecessor whose job was scheduled but has no result
    yet (`PENDING`/`RUNNING`, `DROPPED`, or a lost artifact) stops the walk
    too;
  * a predecessor measured against a different release baseline (a release
    cut moved it) is not comparable either.

In all those cases the gate has to fall back to the absolute threshold rather
than invent a delta.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.performance_tests as m
from ci.jobs.performance_tests import (
    FETCH_ERROR,
    FETCH_MISSING,
    FETCH_OK,
    MASTER_RUN_INCOMPLETE,
    MASTER_RUN_NEVER_SCHEDULED,
    format_release_base_marker,
    is_perf_summary_message,
    parse_release_base,
    parse_slower_count,
)

BASE = "aabbccddeeff00112233445566778899"
OTHER_BASE = "99887766554433221100ffeeddccbbaa"

# The name the fake `Utils.normalize_string` maps the job name to, i.e. the
# `result_*.json` the walk asks for. Anything else is the workflow report.
JOB_RESULT_FILE = "result_job.json"

# The predecessor's own `MasterCI` report says the job was filtered out there,
# so that commit holds no measurement and the walk may look further back.
SKIPPED_JOB = json.dumps(
    {
        "results": [
            {"name": "Job", "status": "SKIPPED"},
            {"name": "Other", "status": "OK"},
        ]
    }
)

# The predecessor's job is still running: its result is yet to be published, so
# the walk must not step over the commit.
RUNNING_JOB = json.dumps({"results": [{"name": "Job", "status": "RUNNING"}]})


def _result(message):
    return json.dumps({"info": message})


def _summary(slower, base=BASE):
    return f"1 too long, 2 faster, {slower} slower, 0 unstable" + (
        format_release_base_marker(base) if base else ""
    )


def _find(
    monkeypatch, commits, responses, workflow_reports=None, release_base_sha=BASE
):
    """Run the predecessor lookup against canned S3 objects.

    `responses` holds the per-sha `result_<job>.json`, `workflow_reports` the
    per-sha `result_masterci.json` consulted when the former is missing; a sha
    absent from `workflow_reports` models a commit with no `MasterCI` run."""
    workflow_reports = workflow_reports or {}
    monkeypatch.setattr(
        m.Utils, "normalize_string", staticmethod(lambda s: "job"), raising=False
    )

    def fake_fetch(link):
        sha, file_name = link.split("/")[-2:]
        if file_name == JOB_RESULT_FILE:
            return responses[sha]
        assert file_name == m.MASTER_WORKFLOW_RESULT_FILE, file_name
        if sha not in workflow_reports:
            return FETCH_MISSING, None
        return FETCH_OK, workflow_reports[sha]

    monkeypatch.setattr(m, "fetch_prev_master_result", fake_fetch)
    return m.find_prev_master_slower_count("Job", commits, release_base_sha)


def test_summary_messages_are_accepted_as_a_baseline():
    assert is_perf_summary_message("see the report")
    assert is_perf_summary_message("18 slower")
    assert is_perf_summary_message("6 too long, 9 faster, 18 slower, 1 unstable")


def test_the_gates_own_suffixes_do_not_break_recognition():
    message = _summary(18) + "; delta vs prev master run (abcdef12): +2"
    assert is_perf_summary_message(message.lower())
    assert parse_slower_count(message.lower()) == 18


def test_sentinels_and_error_runs_are_not_a_baseline():
    for message in (
        "no status in report.",
        "no message in report.",
        "failed to parse the report.",
        "errors while building the report.",
        "3 errors, 18 slower",
        "",
    ):
        assert not is_perf_summary_message(message), message


def test_release_base_marker_roundtrip():
    message = _summary(4).lower()
    assert parse_release_base(message) == BASE[:12]
    assert parse_release_base("18 slower") is None


def test_previous_run_is_found_across_commits_that_never_ran_the_job(monkeypatch):
    # `never_ran` had no `MasterCI` run at all (it was not the head of its push
    # batch), `filtered` had one that skipped the job by change filtering.
    commits = ["never_ran", "filtered", "master1", "master2"]
    responses = {
        "never_ran": (FETCH_MISSING, None),
        "filtered": (FETCH_MISSING, None),
        "master1": (FETCH_OK, _result(_summary(7))),
        "master2": (FETCH_OK, _result(_summary(99))),
    }
    workflow_reports = {"filtered": SKIPPED_JOB}
    assert _find(monkeypatch, commits, responses, workflow_reports) == (7, "master1")


def test_a_missing_immediate_predecessor_that_is_still_running_stops_the_walk(
    monkeypatch,
):
    # The regression this pins: on a first-parent chain, a missing result can
    # mean "the predecessor's run has not published it yet". Stepping over such
    # a commit would compute the delta against `older_master`, so red would
    # blame the current commit for a regression `prev_master` introduced.
    commits = ["prev_master", "older_master"]
    responses = {
        "prev_master": (FETCH_MISSING, None),
        "older_master": (FETCH_OK, _result(_summary(7))),
    }
    workflow_reports = {"prev_master": RUNNING_JOB}
    assert _find(monkeypatch, commits, responses, workflow_reports) == (None, None)


def test_a_missing_result_of_a_finished_predecessor_stops_the_walk(monkeypatch):
    # The job reached a terminal status without publishing a result (it failed
    # before the upload, or lost the artifact) - not comparable either.
    commits = ["prev_master", "older_master"]
    responses = {
        "prev_master": (FETCH_MISSING, None),
        "older_master": (FETCH_OK, _result(_summary(7))),
    }
    for status in ("OK", "FAIL", "ERROR", "DROPPED", "PENDING", None):
        report = json.dumps({"results": [{"name": "Job", "status": status}]})
        assert _find(monkeypatch, commits, responses, {"prev_master": report}) == (
            None,
            None,
        ), status


def test_an_unreadable_predecessor_workflow_report_stops_the_walk(monkeypatch):
    commits = ["prev_master", "older_master"]
    responses = {
        "prev_master": (FETCH_MISSING, None),
        "older_master": (FETCH_OK, _result(_summary(7))),
    }
    assert _find(monkeypatch, commits, responses, {"prev_master": "{not json"}) == (
        None,
        None,
    )


def test_missing_run_classification(monkeypatch):
    def respond(state, body=None):
        monkeypatch.setattr(
            m, "fetch_prev_master_result", lambda link: (state, body), raising=True
        )

    # No `MasterCI` report for the commit: it never ran the workflow.
    respond(FETCH_MISSING)
    assert m.classify_missing_prev_master_run("Job", "sha") == (
        MASTER_RUN_NEVER_SCHEDULED
    )

    # The report cannot be fetched - fail closed.
    respond(FETCH_ERROR)
    assert m.classify_missing_prev_master_run("Job", "sha") == MASTER_RUN_INCOMPLETE

    respond(FETCH_OK, SKIPPED_JOB)
    assert m.classify_missing_prev_master_run("Job", "sha") == (
        MASTER_RUN_NEVER_SCHEDULED
    )

    respond(FETCH_OK, RUNNING_JOB)
    assert m.classify_missing_prev_master_run("Job", "sha") == MASTER_RUN_INCOMPLETE

    # The job is not part of that master run: it was not scheduled there.
    respond(FETCH_OK, json.dumps({"results": [{"name": "Other", "status": "OK"}]}))
    assert m.classify_missing_prev_master_run("Job", "sha") == (
        MASTER_RUN_NEVER_SCHEDULED
    )

    # A report without a job list at all, and a malformed job entry.
    respond(FETCH_OK, json.dumps({}))
    assert m.classify_missing_prev_master_run("Job", "sha") == (
        MASTER_RUN_NEVER_SCHEDULED
    )
    respond(FETCH_OK, json.dumps({"results": ["Job"]}))
    assert m.classify_missing_prev_master_run("Job", "sha") == (
        MASTER_RUN_NEVER_SCHEDULED
    )


def test_transport_failure_stops_the_walk(monkeypatch):
    commits = ["head1", "master1"]
    responses = {
        "head1": (FETCH_ERROR, None),
        "master1": (FETCH_OK, _result(_summary(7))),
    }
    assert _find(monkeypatch, commits, responses) == (None, None)


def test_malformed_body_stops_the_walk(monkeypatch):
    commits = ["head1", "master1"]
    responses = {
        "head1": (FETCH_OK, "{not json"),
        "master1": (FETCH_OK, _result(_summary(7))),
    }
    assert _find(monkeypatch, commits, responses) == (None, None)


def test_a_non_summary_predecessor_stops_the_walk(monkeypatch):
    commits = ["head1", "master1"]
    responses = {
        "head1": (FETCH_OK, _result("No status in report.")),
        "master1": (FETCH_OK, _result(_summary(7))),
    }
    assert _find(monkeypatch, commits, responses) == (None, None)


def test_a_different_release_baseline_is_not_comparable(monkeypatch):
    commits = ["head1"]
    responses = {"head1": (FETCH_OK, _result(_summary(7, base=OTHER_BASE)))}
    assert _find(monkeypatch, commits, responses) == (None, None)


def test_a_predecessor_without_a_baseline_marker_is_not_comparable(monkeypatch):
    commits = ["head1"]
    responses = {"head1": (FETCH_OK, _result(_summary(7, base="")))}
    assert _find(monkeypatch, commits, responses) == (None, None)


def test_no_predecessor_at_all(monkeypatch):
    commits = ["side1"]
    responses = {"side1": (FETCH_MISSING, None)}
    assert _find(monkeypatch, commits, responses) == (None, None)


def test_fetch_classifies_http_codes(monkeypatch, tmp_path):
    calls = {}

    def fake_get_output(command):
        calls["command"] = command
        out_path = command.split(" -o ")[1].split(" ")[0]
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(calls["body"])
        return calls["code"]

    monkeypatch.setattr(m.Shell, "get_output", staticmethod(fake_get_output))

    calls.update(code="200", body=_result(_summary(3)))
    state, body = m.fetch_prev_master_result("https://example/result.json")
    assert (state, json.loads(body)["info"]) == (FETCH_OK, _summary(3))

    calls.update(code="404", body="")
    assert m.fetch_prev_master_result("https://example/result.json") == (
        FETCH_MISSING,
        None,
    )

    # curl exited non-zero: `Shell.get_output` swallows the output.
    calls.update(code="", body="")
    assert m.fetch_prev_master_result("https://example/result.json") == (
        FETCH_ERROR,
        None,
    )

    # No response received at all.
    calls.update(code="000", body="")
    assert m.fetch_prev_master_result("https://example/result.json") == (
        FETCH_ERROR,
        None,
    )

    calls.update(code="500", body="")
    assert m.fetch_prev_master_result("https://example/result.json") == (
        FETCH_ERROR,
        None,
    )


def test_delta_threshold_is_smaller_than_the_absolute_one():
    # The whole point of the delta gate: a cumulative count far above the
    # absolute threshold must not be red by itself.
    assert m.SLOWER_QUERIES_DELTA_FAIL_THRESHOLD < m.SLOWER_QUERIES_FAIL_THRESHOLD
    assert m.too_many_slow("40 slower")
