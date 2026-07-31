import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import revert_ci_regressions as job

NOW = datetime(2026, 7, 31, 22, 0, 0, tzinfo=timezone.utc)


def make_failure(**overrides):
    row = {
        "failure_kind": "test",
        "test_name": "04611_join_runtime_filters",
        "check_name": "",
        "check_names": ["Stateless tests (amd_debug, parallel)"],
        "failure_count": 8,
        "first_failure_time": "2026-07-31 13:28:40",
        "last_failure_time": "2026-07-31 21:12:40",
        "commit_shas": ["a" * 40],
        "report_url": "https://example.invalid/report",
        "context": "boom",
    }
    row.update(overrides)
    return job.Failure.from_row(row)


def make_pull_request(**overrides):
    pull_request = {
        "number": 112345,
        "title": "Add a new setting",
        "body": "### Changelog category (leave one):\n- Improvement\n",
        "url": "https://github.com/ClickHouse/ClickHouse/pull/112345",
        "state": "MERGED",
        "mergedAt": "2026-07-31T10:00:00Z",
        "mergeCommit": {"oid": "b" * 40},
        "baseRefName": "master",
        "headRefName": "add-a-new-setting",
        "author": {"login": "somebody"},
    }
    pull_request.update(overrides)
    return pull_request


# --- the query that picks the failures up ------------------------------------


def test_failures_query_counts_tests_and_whole_check_failures():
    query = job.failures_query()
    # Test cases that failed inside a check that did not succeed ...
    assert "test_status LIKE 'F%' OR test_status LIKE 'E%'" in query
    assert "AND check_status != 'success'" in query
    # ... plus the failures that carry no test name at all.
    assert (
        "AND test_name = ''\n        AND check_status IN ('failure', 'error')" in query
    )
    assert "UNION ALL" in query


def test_failures_query_does_not_shadow_source_columns_with_group_aliases():
    """`'' AS test_name` next to a real `test_name` in the same SELECT makes the
    alias shadow the column, which silently empties the grouping. The group
    columns are therefore named apart and renamed only in the outer SELECT."""
    query = job.failures_query()
    assert "'' AS group_test" in query
    assert "'' AS group_check" in query
    assert "check_name AS seen_in_check" in query
    assert "'' AS test_name" not in query
    assert "'' AS check_name" not in query
    assert "GROUP BY failure_kind, group_test, group_check" in query


def test_failures_query_keeps_only_repeated_failures_on_master():
    query = job.failures_query(hours=24, min_failures=3)
    assert "HAVING failure_count >= 3" in query
    assert "check_start_time >= now() - INTERVAL 24 HOUR" in query
    assert "head_ref = 'master'" in query
    assert "startsWith(head_repo, 'ClickHouse/')" in query


def test_failures_query_avoids_json_quoting_of_counters():
    """ClickHouse quotes 64-bit integers in JSON by default, so the counters are
    narrowed in the query instead of being parsed out of strings."""
    assert "toUInt32(count()) AS failure_count" in job.failures_query()
    assert "toUInt8(max(action = 'reverted')) AS reverted" in (
        job.recent_investigations_query()
    )


# --- which failures are investigated in a given run ---------------------------


def test_failure_never_seen_before_is_investigated():
    assert job.skip_reason(make_failure(), {}, NOW) == ""


def test_failure_investigated_recently_waits_for_the_cooldown():
    failure = make_failure()
    prior = {
        failure.key: {
            "last_investigation_time": (NOW - timedelta(hours=1)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "reverted": 0,
        }
    }
    assert "cooldown" in job.skip_reason(failure, prior, NOW)


def test_failure_investigated_before_the_cooldown_is_investigated_again():
    failure = make_failure()
    prior = {
        failure.key: {
            "last_investigation_time": (
                NOW - timedelta(hours=job.INVESTIGATION_COOLDOWN_HOURS + 1)
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "reverted": 0,
        }
    }
    assert job.skip_reason(failure, prior, NOW) == ""


def test_failure_already_reverted_is_left_alone_for_the_whole_window():
    """The occurrences from before the revert stay in the window for a day; the
    failure must not be investigated again over them."""
    failure = make_failure()
    prior = {
        failure.key: {
            "last_investigation_time": (
                NOW - timedelta(hours=job.INVESTIGATION_COOLDOWN_HOURS + 5)
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "reverted": 1,
        }
    }
    assert "already created" in job.skip_reason(failure, prior, NOW)


def test_test_level_and_check_level_failures_are_different_keys():
    assert (
        make_failure().key
        != make_failure(
            failure_kind="check",
            test_name="",
            check_name="Stateless tests (amd_debug, parallel)",
        ).key
    )


# --- reading the agent's verdict ---------------------------------------------


def test_verdict_is_read_and_normalized():
    verdict = job.parse_verdict(
        json.dumps(
            {
                "verdict": "regression",
                "confidence": "high",
                "offending_pull_request": "112345",
                "offending_commit": "b" * 40,
                "explanation": "The new setting is not registered.",
            }
        )
    )
    assert verdict == {
        "verdict": "regression",
        "confidence": "high",
        "offending_pull_request_number": 112345,
        "offending_commit_sha": "b" * 40,
        "explanation": "The new setting is not registered.",
    }


def test_verdict_wrapped_in_a_code_fence_is_read():
    verdict = job.parse_verdict(
        '```json\n{"verdict": "inconclusive", "confidence": "low", '
        '"offending_pull_request": 0, "explanation": "Could not tell."}\n```'
    )
    assert verdict["verdict"] == "inconclusive"


@pytest.mark.parametrize(
    "text",
    [
        "",
        "not json at all",
        '["a list"]',
        '{"verdict": "maybe", "confidence": "high", "explanation": "x"}',
        '{"verdict": "regression", "confidence": "certain", "explanation": "x"}',
        '{"verdict": "regression", "confidence": "high", "explanation": ""}',
        # A regression has to name the pull request it blames.
        '{"verdict": "regression", "confidence": "high", "offending_pull_request": 0,'
        ' "explanation": "x"}',
        '{"verdict": "regression", "confidence": "high", "offending_pull_request": -1,'
        ' "explanation": "x"}',
        '{"verdict": "regression", "confidence": "high", "offending_pull_request": "abc",'
        ' "explanation": "x"}',
        '{"verdict": "regression", "confidence": "high", "offending_pull_request": 1,'
        ' "offending_commit": "not-a-sha", "explanation": "x"}',
    ],
)
def test_unreadable_verdict_is_rejected(text):
    with pytest.raises(ValueError):
        job.parse_verdict(text)


def test_pull_request_named_by_a_non_regression_verdict_is_dropped():
    """Blaming a pull request while concluding this is not a regression is a
    contradiction; nothing downstream may act on the number."""
    verdict = job.parse_verdict(
        '{"verdict": "not_a_regression", "confidence": "high",'
        ' "offending_pull_request": 112345, "offending_commit": "abcdef1",'
        ' "explanation": "Flaky for months."}'
    )
    assert verdict["offending_pull_request_number"] == 0
    assert verdict["offending_commit_sha"] == ""
    assert verdict["explanation"] == "Flaky for months."


# --- what is allowed to be reverted ------------------------------------------


def test_high_confidence_regression_is_actionable():
    investigation = job.Investigation(
        failure=make_failure(),
        verdict="regression",
        confidence="high",
        offending_pull_request_number=112345,
    )
    assert investigation.is_actionable()


@pytest.mark.parametrize(
    "overrides",
    [
        {"verdict": "inconclusive"},
        {"verdict": "not_a_regression"},
        {"verdict": "error"},
        {"confidence": "medium"},
        {"confidence": "low"},
        {"offending_pull_request_number": 0},
    ],
)
def test_anything_short_of_a_certain_regression_is_not_actionable(overrides):
    fields = {
        "verdict": "regression",
        "confidence": "high",
        "offending_pull_request_number": 112345,
    }
    fields.update(overrides)
    assert not job.Investigation(failure=make_failure(), **fields).is_actionable()


def test_a_recently_merged_pull_request_may_be_reverted():
    assert job.culprit_guard(make_pull_request(), make_failure(), NOW) == ""


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"state": "OPEN"}, "not merged"),
        ({"state": "CLOSED"}, "not merged"),
        ({"baseRefName": "25.8"}, "not 'master'"),
        ({"mergeCommit": None}, "no merge commit"),
        ({"mergedAt": ""}, "no merge time"),
        # Older than the window an automatic revert covers: later changes are
        # likely to depend on it by now.
        ({"mergedAt": "2026-07-20T10:00:00Z"}, "days ago"),
        # Merged after the failure was last seen, so it cannot be the cause.
        ({"mergedAt": "2026-07-31T21:30:00Z"}, "after the last occurrence"),
        # Reverting a revert restores the breakage the revert removed.
        ({"title": 'Revert "Add a new setting"'}, "itself a revert"),
        ({"title": 'Reapply "Add a new setting"'}, "itself a revert"),
        ({"body": "Reverts ClickHouse/ClickHouse#1\n"}, "itself a revert"),
        ({"headRefName": "revert-112000"}, "automation branch"),
        ({"headRefName": "reapply-112000"}, "automation branch"),
    ],
)
def test_pull_requests_that_must_not_be_reverted_automatically(overrides, expected):
    guard = job.culprit_guard(make_pull_request(**overrides), make_failure(), NOW)
    assert expected in guard


# --- not reverting the same merge twice ---------------------------------------


def test_a_revert_already_on_master_stops_a_second_one(monkeypatch):
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "deadbeef\n")
    monkeypatch.setattr(job.Shell, "check", _unexpected("checked the remote branch"))
    monkeypatch.setattr(
        job.GH, "get_output_with_retries", _unexpected("listed pull requests")
    )
    assert "already on master" in job.already_handled(
        "b" * 40, "revert-112345", "ClickHouse/ClickHouse"
    )


def test_a_pushed_revert_branch_stops_a_second_one(monkeypatch):
    """The branch is what `.github/workflows/revert_broken_prs.yml` pushes too,
    so an in-flight revert by either automation is seen with no indexing lag."""
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "")
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(
        job.GH, "get_output_with_retries", _unexpected("listed pull requests")
    )
    assert "already exists on the remote" in job.already_handled(
        "b" * 40, "revert-112345", "ClickHouse/ClickHouse"
    )


def test_a_merged_revert_whose_branch_was_deleted_stops_a_second_one(monkeypatch):
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "")
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: False)
    monkeypatch.setattr(job.GH, "get_output_with_retries", lambda *a, **k: "9876\n")
    assert "already exists for revert-112345" in job.already_handled(
        "b" * 40, "revert-112345", "ClickHouse/ClickHouse"
    )


def test_an_unhandled_merge_is_revertable(monkeypatch):
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "")
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: False)
    monkeypatch.setattr(job.GH, "get_output_with_retries", lambda *a, **k: "")
    assert job.already_handled("b" * 40, "revert-112345", "ClickHouse/ClickHouse") == ""


def _unexpected(what):
    def _fail(*_args, **_kwargs):
        raise AssertionError(f"the guard should have stopped before it {what}")

    return _fail


# --- acting on a verdict ------------------------------------------------------


def _investigation():
    return job.Investigation(
        failure=make_failure(),
        verdict="regression",
        confidence="high",
        offending_pull_request_number=112345,
        offending_commit_sha="b" * 40,
        explanation="The new setting is not registered.",
    )


def test_a_guarded_pull_request_is_recorded_and_not_touched(monkeypatch):
    monkeypatch.setattr(
        job, "get_pull_request", lambda *a, **k: make_pull_request(state="OPEN")
    )
    monkeypatch.setattr(job.Shell, "check", _unexpected("touched git"))
    monkeypatch.setattr(job, "create_revert", _unexpected("started reverting"))

    investigation = _investigation()
    job.act(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.SKIPPED_GUARD
    assert investigation.revert_pull_request_number == 0
    assert "not merged" in investigation.explanation
    # The agent's reasoning is kept, the decision is appended to it.
    assert "The new setting is not registered." in investigation.explanation


def test_a_merge_commit_missing_from_master_is_not_reverted(monkeypatch):
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: False)
    monkeypatch.setattr(job, "create_revert", _unexpected("started reverting"))

    investigation = _investigation()
    job.act(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.SKIPPED_GUARD
    assert "not in the history of master" in investigation.explanation


def test_a_conflicting_revert_is_recorded_and_left_to_a_human(monkeypatch):
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job, "already_handled", lambda *a, **k: "")

    def conflict(*_args, **_kwargs):
        raise job.RevertConflict(
            "reverting bbbb conflicts with later changes on master"
        )

    monkeypatch.setattr(job, "create_revert", conflict)
    monkeypatch.setattr(job, "create_reintroduce", _unexpected("opened a reapply"))

    investigation = _investigation()
    job.act(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.REVERT_CONFLICT
    assert investigation.revert_pull_request_number == 0
    assert "conflicts with later changes" in investigation.explanation


def test_a_revert_is_merged_and_the_change_is_reopened_as_a_draft(monkeypatch):
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job, "already_handled", lambda *a, **k: "")
    monkeypatch.setattr(job, "create_revert", lambda *a, **k: ("c" * 40, 112400))
    reintroduced = {}

    def create_reintroduce(pull_request, revert_commit, revert_pull_request, *_args):
        reintroduced.update(
            {
                "commit": revert_commit,
                "revert": revert_pull_request,
                "reverted": pull_request["number"],
            }
        )
        return 112401

    monkeypatch.setattr(job, "create_reintroduce", create_reintroduce)

    investigation = _investigation()
    job.act(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.REVERTED
    assert investigation.revert_pull_request_number == 112400
    assert investigation.reintroduce_pull_request_number == 112401
    # The reapply is built on the revert commit, so its diff against master
    # shows the reintroduced change rather than nothing.
    assert reintroduced == {"commit": "c" * 40, "revert": 112400, "reverted": 112345}


def test_a_merged_revert_whose_reapply_failed_says_so_and_fails_the_job(monkeypatch):
    """The change is off master with nothing holding it at that point, so the
    row has to say that reopening it is now a manual job."""
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job, "already_handled", lambda *a, **k: "")
    monkeypatch.setattr(job, "create_revert", lambda *a, **k: ("c" * 40, 112400))

    def boom(*_args, **_kwargs):
        raise RuntimeError("failed to push reapply-112345")

    monkeypatch.setattr(job, "create_reintroduce", boom)

    investigation = _investigation()
    with pytest.raises(RuntimeError):
        job.act(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.REVERTED
    assert investigation.revert_pull_request_number == 112400
    assert investigation.reintroduce_pull_request_number == 0
    assert "has to be reopened by hand" in investigation.explanation


def test_a_failure_to_revert_is_recorded_before_it_fails_the_job(monkeypatch):
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job, "already_handled", lambda *a, **k: "")

    def boom(*_args, **_kwargs):
        raise RuntimeError("failed to push revert-112345")

    monkeypatch.setattr(job, "create_revert", boom)

    investigation = _investigation()
    with pytest.raises(RuntimeError):
        job.act(investigation, "ClickHouse/ClickHouse", NOW)
    assert investigation.action == job.Action.REVERT_FAILED
    assert "failed to push" in investigation.explanation


# --- what is written to the CI database ---------------------------------------


def test_every_investigation_is_recorded_including_the_negative_ones():
    investigation = job.Investigation(
        failure=make_failure(),
        verdict="not_a_regression",
        confidence="high",
        explanation="Flaky for months.",
    )
    record = investigation.to_record("2026-07-31 22:00:00", "https://task.invalid")

    assert record["verdict"] == "not_a_regression"
    assert record["action"] == job.Action.NONE
    assert record["offending_pull_request_number"] == 0
    assert record["revert_pull_request_number"] == 0
    # The columns that join back to `checks`.
    assert record["test_name"] == "04611_join_runtime_filters"
    assert record["check_names"] == ["Stateless tests (amd_debug, parallel)"]
    assert record["commit_shas"] == ["a" * 40]
    assert record["report_url"] == "https://example.invalid/report"
    # Every column of the table is filled in, so no INSERT relies on defaults.
    columns = {
        line.split("`")[1]
        for line in job.INVESTIGATION_TABLE_DDL.splitlines()
        if line.strip().startswith("`")
    }
    assert columns == set(record)


def test_the_record_is_json_serializable():
    record = _investigation().to_record("2026-07-31 22:00:00", "https://task.invalid")
    assert json.loads(json.dumps(record)) == record


def test_the_summary_reports_what_the_run_did():
    investigations = [
        job.Investigation(failure=make_failure(), verdict="not_a_regression"),
        job.Investigation(
            failure=make_failure(), verdict="regression", action=job.Action.REVERTED
        ),
        job.Investigation(failure=make_failure(), verdict="error"),
    ]
    assert job.summary(investigations) == (
        "3 investigated, 1 regressions, 1 reverted, 1 failed to investigate"
    )
    assert job.summary([]) == "No failures to investigate"


# --- pull request bodies ------------------------------------------------------


def test_the_revert_body_carries_the_marker_the_rest_of_ci_reads():
    body = job.revert_body(make_pull_request(), _investigation())
    assert body.startswith("Reverts ClickHouse/ClickHouse#112345")
    assert "CI Fix or Improvement" in body
    assert "The new setting is not registered." in body


def test_the_reintroduce_body_links_both_pull_requests():
    body = job.reintroduce_body(make_pull_request(), 112400, _investigation())
    assert "https://github.com/ClickHouse/ClickHouse/pull/112345" in body
    assert "https://github.com/ClickHouse/ClickHouse/pull/112400" in body
    # It must not read as a revert itself, or the next run would skip it as one
    # and the merge-readiness check would wave it through with no CI.
    assert "Reverts ClickHouse/" not in body


def test_the_pull_request_number_is_read_out_of_the_created_url():
    assert (
        job.pull_request_number_from_url(
            "https://github.com/ClickHouse/ClickHouse/pull/112400"
        )
        == 112400
    )
    with pytest.raises(RuntimeError):
        job.pull_request_number_from_url("")


# --- the prompt ---------------------------------------------------------------


def test_the_prompt_states_the_consequence_of_a_confident_verdict():
    """The agent has to know that `high` confidence merges a revert with no
    checks, or it has no reason to reserve it for unambiguous cases."""
    prompt = job.investigation_prompt(make_failure(), "/tmp/verdict.json")
    assert "without waiting for any checks" in prompt
    assert "/tmp/verdict.json" in prompt
    assert "04611_join_runtime_filters" in prompt
    assert "no commits, no pushes, no pull requests" in prompt
