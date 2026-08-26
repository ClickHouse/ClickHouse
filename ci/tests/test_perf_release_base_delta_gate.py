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
  * a missing artifact means "this merged side-branch commit never ran the
    job" and is skipped, while a transport failure says nothing about the
    commit and must stop the walk (otherwise the delta is computed against an
    older run and red stops blaming the introducing commit);
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
    format_release_base_marker,
    is_perf_summary_message,
    parse_release_base,
    parse_slower_count,
)

BASE = "aabbccddeeff00112233445566778899"
OTHER_BASE = "99887766554433221100ffeeddccbbaa"


def _result(message):
    return json.dumps({"info": message})


def _summary(slower, base=BASE):
    return f"1 too long, 2 faster, {slower} slower, 0 unstable" + (
        format_release_base_marker(base) if base else ""
    )


def _find(monkeypatch, commits, responses, release_base_sha=BASE):
    """Run the predecessor lookup with a canned response per sha."""
    monkeypatch.setattr(
        m.Utils, "normalize_string", staticmethod(lambda s: "job"), raising=False
    )

    def fake_fetch(link):
        sha = link.split("/")[-2]
        return responses[sha]

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


def test_previous_run_is_found_across_side_branch_commits(monkeypatch):
    commits = ["side1", "side2", "master1", "master2"]
    responses = {
        "side1": (FETCH_MISSING, None),
        "side2": (FETCH_MISSING, None),
        "master1": (FETCH_OK, _result(_summary(7))),
        "master2": (FETCH_OK, _result(_summary(99))),
    }
    assert _find(monkeypatch, commits, responses) == (7, "master1")


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
