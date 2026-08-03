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
        "test_name": "04611_join_runtime_filters",
        "check_names": [
            "Stateless tests (amd_debug, parallel)",
            "Stateless tests (amd_tsan, parallel)",
        ],
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


def test_failures_query_counts_failing_tests():
    query = job.failures_query()
    # A test case that failed inside a check that did not succeed. `SKIPPED` is
    # not a failure, and neither is a `skipped` check, though it is not a success.
    assert "AND test_status != 'SKIPPED'" in query
    assert "AND (test_status LIKE 'F%' OR test_status LIKE 'E%')" in query
    assert "AND check_status != 'success'" in query


def test_failures_query_does_not_count_what_a_check_says_about_itself():
    """A build that failed, a server that would not start or a job that ran out
    of time is recorded with no test name and no test status. Those are failures
    of a whole job rather than of one test, "why does this check fail" has no
    single answer to revert on, and the rows a check writes about itself would
    otherwise outrank the tests they summarize ("Failed: 1, Passed: 12096")."""
    query = job.failures_query()
    assert "check_status IN ('failure', 'error')" not in query
    assert "task_url" not in query


def test_failures_query_groups_by_test_name_across_the_checks():
    """One test failing in the debug and in the tsan build is one failure with
    one cause to look for. The checks it failed in are collected as evidence:
    the spread over builds is what tells a regression from a configuration
    problem, and grouping by the check too would split it apart."""
    query = job.failures_query()
    assert "GROUP BY test_name\n" in query
    # `test_name` is projected straight from `checks`, unaliased, so the recorded
    # rows join back to it on that column.
    assert query.startswith("SELECT\n    test_name,\n")
    assert "arraySort(groupUniqArray(50)(check_name)) AS check_names," in query
    assert "GROUP BY test_name\n" in job.recent_investigations_query()


def test_failures_query_keeps_only_repeated_failures_on_master():
    query = job.failures_query(hours=24, min_failures=2)
    assert "HAVING failure_count >= 2" in query
    assert "check_start_time >= now() - INTERVAL 24 HOUR" in query
    assert "head_ref = 'master'" in query
    assert "startsWith(head_repo, 'ClickHouse/')" in query


def test_a_failure_seen_once_is_not_investigated():
    """The threshold is what separates a regression from a sporadic failure, and
    it is the only thing standing between a flaky test and an automatic revert."""
    assert job.MIN_FAILURES == 2
    assert f"HAVING failure_count >= {job.MIN_FAILURES}" in job.failures_query()


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


def test_the_same_test_in_two_checks_is_one_failure():
    """The occurrences in the debug and in the tsan build are one finding, so
    reverting on it settles both: the second build must not bring the same
    investigation back an hour later."""
    debug = make_failure(check_names=["Stateless tests (amd_debug, parallel)"])
    tsan = make_failure(check_names=["Stateless tests (amd_tsan, parallel)"])
    assert debug.key == tsan.key

    prior = {
        debug.key: {
            "last_investigation_time": NOW.strftime("%Y-%m-%d %H:%M:%S"),
            "reverted": 1,
        }
    }
    assert job.skip_reason(debug, prior, NOW)
    assert job.skip_reason(tsan, prior, NOW)


def test_a_failure_is_keyed_and_titled_by_the_test_alone():
    failure = make_failure()
    assert failure.key == "04611_join_runtime_filters"
    assert failure.title == "04611_join_runtime_filters"


def test_a_failure_is_named_together_with_every_check_it_was_seen_in():
    """The checks are the evidence the investigation starts from, so they are in
    front of the reader of a revert pull request as well as of the agent."""
    assert make_failure().markdown == (
        "test `04611_join_runtime_filters` in "
        "`Stateless tests (amd_debug, parallel)`, "
        "`Stateless tests (amd_tsan, parallel)`"
    )
    assert make_failure(check_names=[]).markdown == (
        "test `04611_join_runtime_filters`"
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


def _shell(answers):
    """Answer a shell command with the value whose fragment occurs in it."""

    def _run(command, *_args, **_kwargs):
        for fragment, output in answers.items():
            if fragment in command:
                return output
        return ""

    return _run


def _ls_remote(*branches):
    return "".join(f"{'c' * 40}\trefs/heads/{branch}\n" for branch in branches)


def _handled(monkeypatch, ls_remote="", by_branch=(), by_marker=()):
    """`already_handled` for #112345, with the remote and the two GitHub
    searches -- by branch name and by the `Reverts ...` marker -- answered as
    the caller says."""
    monkeypatch.setattr(job.Shell, "get_output", _shell({"ls-remote": ls_remote}))
    monkeypatch.setattr(
        job.GH,
        "get_output_with_retries",
        _shell(
            {
                "head:": json.dumps(list(by_branch)),
                "Reverts": json.dumps(list(by_marker)),
            }
        ),
    )
    return job.already_handled("b" * 40, 112345, "ClickHouse/ClickHouse")


def test_a_revert_already_on_master_stops_a_second_one(monkeypatch):
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "deadbeef\n")
    monkeypatch.setattr(
        job.GH, "get_output_with_retries", _unexpected("listed pull requests")
    )
    assert "already on master" in job.already_handled(
        "b" * 40, 112345, "ClickHouse/ClickHouse"
    )


def test_a_pushed_revert_branch_stops_a_second_one(monkeypatch):
    """The branch is what `.github/workflows/revert_broken_prs.yml` pushes too,
    so an in-flight revert by either automation is seen with no indexing lag."""
    monkeypatch.setattr(
        job.Shell, "get_output", _shell({"ls-remote": _ls_remote("revert-112345")})
    )
    monkeypatch.setattr(
        job.GH, "get_output_with_retries", _unexpected("listed pull requests")
    )
    assert "already exists on the remote" in job.already_handled(
        "b" * 40, 112345, "ClickHouse/ClickHouse"
    )


def test_a_revert_branch_pushed_by_the_github_button_stops_a_second_one(monkeypatch):
    """The `Revert` button names the branch `revert-<pr>-<head branch>`, so a
    revert a human started by hand is matched by prefix, not by equality."""
    handled = _handled(
        monkeypatch, ls_remote=_ls_remote("revert-112345-add-a-new-setting")
    )
    assert "revert-112345-add-a-new-setting" in handled


def test_a_merged_revert_whose_branch_was_deleted_stops_a_second_one(monkeypatch):
    handled = _handled(
        monkeypatch, by_branch=[{"number": 9876, "headRefName": "revert-112345"}]
    )
    assert "already exists for revert-112345: 9876" in handled


def test_an_open_revert_pull_request_stops_a_second_one(monkeypatch):
    """A revert somebody opened by hand and left waiting for its checks: its
    branch is deleted from the remote by nobody yet, but nothing has been merged
    either, so the history says nothing."""
    handled = _handled(
        monkeypatch,
        by_branch=[{"number": 9876, "headRefName": "revert-112345-add-a-new-setting"}],
    )
    assert "already exists for revert-112345: 9876" in handled


def test_a_revert_on_a_branch_of_any_name_stops_a_second_one(monkeypatch):
    """`Reverts <repo>#<pr>` is what the `Revert` button writes into the body
    and what this job writes as well, so a revert is found however its branch
    was named."""
    handled = _handled(
        monkeypatch,
        by_marker=[
            {
                "number": 9876,
                "body": "Reverts ClickHouse/ClickHouse#112345\n\nBroke master.",
            }
        ],
    )
    assert "a pull request reverting #112345 already exists: 9876" in handled


def test_a_revert_of_another_pull_request_does_not_stop_this_one(monkeypatch):
    """A GitHub search matches a prefix and stops at no boundary, so the revert
    of #1123456 comes back when the revert of #112345 is searched for. It must
    not be taken for one."""
    handled = _handled(
        monkeypatch,
        by_branch=[{"number": 9876, "headRefName": "revert-1123456"}],
        by_marker=[{"number": 9877, "body": "Reverts ClickHouse/ClickHouse#1123456"}],
    )
    assert handled == ""


def test_an_unhandled_merge_is_revertable(monkeypatch):
    assert _handled(monkeypatch) == ""


def _unexpected(what):
    def _fail(*_args, **_kwargs):
        raise AssertionError(f"the guard should have stopped before it {what}")

    return _fail


# --- not reverting a failure somebody has already fixed -----------------------


class FakeCIDB:
    """The CI database, answering every query with the same rows."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, query, *_args, **_kwargs):
        self.queries.append(query)
        return "".join(json.dumps(row) + "\n" for row in self.rows)


def _runs(*failed, commits=None):
    """One row per run, newest first, each on a commit of its own unless the
    caller says otherwise."""
    commits = commits or [f"{index:040x}" for index in range(len(failed))]
    return [
        {
            "run_time": "2026-07-31 21:%02d:00" % (30 + index),
            "commit_sha": commits[index],
            "failed": int(value),
        }
        for index, value in enumerate(failed)
    ]


def test_a_failure_that_keeps_happening_is_reverted():
    assert job.already_fixed(FakeCIDB(_runs(0, 1, 0)), make_failure()) == ""


def test_a_failure_that_has_passed_ever_since_is_not_reverted():
    """The usual way a broken master is repaired is a follow-up commit, not a
    revert. Reverting then undoes a change nothing is wrong with any more."""
    fixed = job.already_fixed(FakeCIDB(_runs(0, 0, 0)), make_failure())
    assert "the failure is gone" in fixed
    assert "3 runs" in fixed
    assert "3 commits" in fixed


def test_one_passing_commit_is_not_enough_to_call_a_failure_fixed():
    """A failure that does not hit on every run would look fixed after any
    single passing one."""
    assert job.already_fixed(FakeCIDB(_runs(0)), make_failure()) == ""


def test_one_commit_tested_twice_is_one_piece_of_evidence():
    """A check is often re-run on the same commit. Two green runs of one commit
    say no more about a fix than one does."""
    green = _runs(0, 0, commits=["d" * 40, "d" * 40])
    assert job.already_fixed(FakeCIDB(green), make_failure()) == ""


def test_a_failure_nothing_has_run_since_is_reverted():
    """No news is not good news: the failure is still the newest thing known
    about this test in the checks it failed in."""
    assert job.already_fixed(FakeCIDB([]), make_failure()) == ""


def test_the_later_runs_are_looked_up_for_the_same_test_in_the_same_checks():
    cidb = FakeCIDB(_runs(1))
    job.already_fixed(cidb, make_failure())
    query = cidb.queries[0]
    assert "check_start_time > toDateTime('2026-07-31 21:12:40')" in query
    # Aliasing the timestamp back to `check_start_time` would shadow the column
    # the comparison above reads, and the query would not run at all.
    assert "AS run_time" in query
    assert "test_name = '04611_join_runtime_filters'" in query
    assert "head_ref = 'master'" in query
    # A test that did not run says nothing about whether it still fails.
    assert "test_status != 'SKIPPED'" in query


def test_only_the_checks_the_failure_was_seen_in_are_asked():
    """A test runs in many more checks than it failed in, and whether it passes
    in a build that never showed the failure says nothing about the failure."""
    cidb = FakeCIDB(_runs(1))
    job.already_fixed(cidb, make_failure())
    assert (
        "check_name IN ('Stateless tests (amd_debug, parallel)', "
        "'Stateless tests (amd_tsan, parallel)')" in cidb.queries[0]
    )


def test_a_name_that_holds_a_quote_does_not_break_the_query():
    cidb = FakeCIDB([])
    job.already_fixed(cidb, make_failure(test_name="it's a test"))
    assert "test_name = 'it\\'s a test'" in cidb.queries[0]


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
    assert record["check_names"] == [
        "Stateless tests (amd_debug, parallel)",
        "Stateless tests (amd_tsan, parallel)",
    ]
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


# --- a GitHub read that failed is not an answer -------------------------------


def test_a_search_that_found_nothing_is_an_empty_list(monkeypatch):
    monkeypatch.setattr(job.GH, "get_output_with_retries", lambda *a, **k: "[]\n")
    assert (
        job.search_pull_requests("ClickHouse/ClickHouse", "head:revert-1", "number")
        == []
    )


def test_a_search_that_could_not_be_read_is_not_an_empty_list(monkeypatch):
    """`GH.get_output_with_retries` returns nothing when `gh` kept failing. Read
    as "no revert exists" that would let the job open a second revert for a pull
    request somebody is already reverting."""
    monkeypatch.setattr(job.GH, "get_output_with_retries", lambda *a, **k: "")
    with pytest.raises(RuntimeError):
        job.search_pull_requests("ClickHouse/ClickHouse", "head:revert-1", "number")


def test_an_unreadable_already_reverted_guard_stands_the_revert_down(monkeypatch):
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "")
    monkeypatch.setattr(job.GH, "get_output_with_retries", lambda *a, **k: "")
    monkeypatch.setattr(job, "create_revert", _unexpected("started reverting"))

    investigation = _investigation()
    job.act(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.SKIPPED_GUARD
    assert investigation.revert_pull_request_number == 0
    assert "could not be established" in investigation.explanation


def test_an_unreadable_guard_stands_a_dry_run_down_as_well(monkeypatch):
    monkeypatch.setattr(job, "get_pull_request", lambda *a, **k: make_pull_request())
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "")
    monkeypatch.setattr(job.GH, "get_output_with_retries", lambda *a, **k: "")

    investigation = _investigation()
    job.dry_run_action(investigation, "ClickHouse/ClickHouse", NOW)

    assert investigation.action == job.Action.SKIPPED_GUARD
    assert "could not be established" in investigation.explanation


# --- a shallow checkout is not a checkout -------------------------------------


class FakeInfo:
    def __init__(self, branch="master"):
        self.git_branch = branch
        self.repo_name = "ClickHouse/ClickHouse"

    def get_job_url(self):
        return "https://example.invalid/job"


def test_a_checkout_that_cannot_be_unshallowed_stops_the_job(monkeypatch):
    """The agent walks the history of `master` and `git revert -m 1` needs the
    merge commit's parents; neither works on the one commit `actions/checkout`
    clones by default."""
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "true\n")
    monkeypatch.setattr(
        job.Shell, "check", _shell({"fetch --unshallow": False, "": True})
    )
    assert job.prepare(FakeInfo()) is False


def test_a_checkout_that_was_unshallowed_carries_on(monkeypatch):
    shallow = ["true\n"]
    monkeypatch.setattr(
        job.Shell, "get_output", lambda *a, **k: shallow.pop(0) if shallow else ""
    )
    monkeypatch.setattr(job.Shell, "check", lambda *a, **k: True)
    monkeypatch.setattr(job, "reset_worktree", lambda *a, **k: None)
    assert job.prepare(FakeInfo()) is True


def test_a_checkout_that_is_not_shallow_is_not_unshallowed(monkeypatch):
    monkeypatch.setattr(job.Shell, "get_output", lambda *a, **k: "false\n")

    def check(command, *_args, **_kwargs):
        assert "--unshallow" not in command
        return True

    monkeypatch.setattr(job.Shell, "check", check)
    monkeypatch.setattr(job, "reset_worktree", lambda *a, **k: None)
    assert job.prepare(FakeInfo()) is True


# --- the run budget is a deadline, not a stopwatch ----------------------------


def test_a_step_that_still_fits_into_the_budget_is_started():
    started = datetime.now(timezone.utc) - timedelta(seconds=60)
    assert job.budget_left(started, 60) is True


def test_a_step_that_would_overrun_the_budget_is_not_started():
    """An investigation started with a minute left still runs to its own
    timeout, so what is checked is whether the next step fits, not what was
    spent."""
    started = datetime.now(timezone.utc) - timedelta(seconds=job.RUN_BUDGET_SEC - 120)
    assert job.budget_left(started, 60) is True
    assert job.budget_left(started, 180) is False
    assert job.budget_left(started, job.INVESTIGATION_RESERVE_SEC) is False


def test_the_reserve_covers_the_worst_case_of_what_it_guards():
    assert (
        job.INVESTIGATION_RESERVE_SEC
        == job.MAX_AGENT_ATTEMPTS * job.AGENT_TIMEOUT_SEC + job.REVERT_RESERVE_SEC
    )
    # A run that reserves more than it has could never investigate anything.
    assert job.INVESTIGATION_RESERVE_SEC < job.RUN_BUDGET_SEC


# --- the token is refreshed, not reused ---------------------------------------


def test_every_agent_attempt_mints_a_fresh_token(monkeypatch):
    """`GHAuth.auth` mints once per process unless `force` is passed, so without
    it the "refresh before each attempt" is a no-op from the second attempt on
    and a long run can hit token expiry in the middle of a revert."""
    calls = []
    monkeypatch.setattr(
        job.GHAuth, "auth", lambda **kwargs: calls.append(kwargs) or True
    )
    monkeypatch.setattr(job, "reset_worktree", lambda *a, **k: None)
    monkeypatch.setattr(job, "run_agent", _unexpected("ran the agent"))

    investigation = job.investigate(make_failure(), 0)

    assert investigation.verdict == "error"
    assert len(calls) == job.MAX_AGENT_ATTEMPTS
    assert all(call.get("force") for call in calls)


# --- the dry run works before the job has ever run ----------------------------


class FakeDryRunCIDB:
    """The CI database as it is on the first dry run: the investigation table
    has not been created, because only a live run creates it."""

    def __init__(self, failures=(), table=False):
        self.failures = list(failures)
        self.table = table
        self.queries = []
        self.inserted = []

    def query(self, query, *_args, **_kwargs):
        self.queries.append(query)
        if query.startswith("EXISTS TABLE"):
            return "1\n" if self.table else "0\n"
        if job.INVESTIGATION_TABLE in query and not self.table:
            raise RuntimeError(f"Table {job.INVESTIGATION_TABLE} does not exist")
        if job.INVESTIGATION_TABLE in query:
            return ""
        return "".join(json.dumps(row) + "\n" for row in self.failures)

    def insert_rows(self, *args, **kwargs):
        self.inserted.append((args, kwargs))


def test_a_dry_run_selects_failures_before_the_table_exists():
    cidb = FakeDryRunCIDB(failures=[])
    assert job.select_failures(cidb, NOW, dry_run=True) == []
    assert any(q.startswith("EXISTS TABLE") for q in cidb.queries)


def test_a_dry_run_reads_the_prior_investigations_once_the_table_exists():
    cidb = FakeDryRunCIDB(failures=[], table=True)
    job.select_failures(cidb, NOW, dry_run=True)
    assert any("investigation_time" in q for q in cidb.queries)


def _dry_run(monkeypatch, cidb, investigate=None):
    monkeypatch.setattr(job, "Info", FakeInfo)
    monkeypatch.setattr(job, "prepare", lambda *a, **k: True)
    monkeypatch.setattr(job, "connect", lambda: cidb)
    monkeypatch.setattr(job, "reset_worktree", lambda *a, **k: None)
    monkeypatch.setattr(
        job, "investigate", investigate or _unexpected("investigated anything")
    )
    results = []
    job.run(results, dry_run=True)
    return results


def test_a_dry_run_ends_with_a_summary_instead_of_a_type_error(monkeypatch):
    """`Result` has no default `status`, so a result built without one raises
    and the dry run ends as a job error after doing all the work."""
    results = _dry_run(monkeypatch, FakeDryRunCIDB(failures=[]))

    assert results[-1].name == "Would record investigations"
    assert results[-1].is_ok()
    assert results[-1].info


def test_a_dry_run_writes_nothing_anywhere(monkeypatch):
    cidb = FakeDryRunCIDB(failures=[])
    _dry_run(monkeypatch, cidb)

    assert cidb.inserted == []
    assert not any("CREATE TABLE" in q for q in cidb.queries)


# --- a revert step that did not finish stops the run --------------------------


def _failure_row(test_name):
    return {
        "test_name": test_name,
        "check_names": ["Stateless tests (amd_debug, parallel)"],
        "failure_count": 8,
        "first_failure_time": "2026-07-31 13:28:40",
        "last_failure_time": "2026-07-31 21:12:40",
        "commit_shas": ["a" * 40],
        "report_url": "https://example.invalid/report",
        "context": "boom",
    }


def _live_run(monkeypatch, cidb, act):
    monkeypatch.setattr(job, "Info", FakeInfo)
    monkeypatch.setattr(job, "prepare", lambda *a, **k: True)
    monkeypatch.setattr(job, "connect", lambda: cidb)
    monkeypatch.setattr(job, "reset_worktree", lambda *a, **k: None)
    monkeypatch.setattr(job, "already_fixed", lambda *a, **k: "")
    monkeypatch.setattr(job, "investigate", lambda failure, index: _investigation())
    monkeypatch.setattr(job, "act", act)
    monkeypatch.setattr(job, "record", lambda *a, **k: None)
    results = []
    job.run(results)
    return results


def test_a_revert_that_threw_after_it_merged_stops_the_run(monkeypatch):
    """`step` turns the exception into a failed sub-result and returns False, so
    the loop has to look at it: the revert is merged, the draft reintroducing
    the change was never opened, and reverting one more pull request on top of
    that would leave a second half-finished revert behind."""
    reverted = []

    def act(investigation, *_args, **_kwargs):
        reverted.append(investigation)
        investigation.action = job.Action.REVERTED
        raise RuntimeError("failed to push reapply-112345")

    cidb = FakeDryRunCIDB(
        failures=[_failure_row("first_test"), _failure_row("second_test")], table=True
    )
    results = _live_run(monkeypatch, cidb, act)

    assert len(reverted) == 1
    assert not [r for r in results if r.name.startswith("Revert") and r.is_ok()]


def test_a_revert_that_finished_lets_the_run_carry_on(monkeypatch):
    reverted = []

    def act(investigation, *_args, **_kwargs):
        reverted.append(investigation)
        investigation.action = job.Action.REVERTED
        investigation.revert_pull_request_number = 112400

    cidb = FakeDryRunCIDB(
        failures=[_failure_row("first_test"), _failure_row("second_test")], table=True
    )
    _live_run(monkeypatch, cidb, act)

    assert len(reverted) == job.MAX_REVERTS_PER_RUN
