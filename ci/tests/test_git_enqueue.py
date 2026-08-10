"""
Unit tests for `Git.enqueue_pull_request`.

The release flow and `check_ci.py` both add a PR to `master`'s merge queue via
the `enqueuePullRequest` GraphQL mutation (the repo disables `gh pr merge
--auto`'s `enablePullRequestAutoMerge`). These tests pin the contract of the
shared helper: it uses the mutation, treats "already in the queue" as success,
surfaces real failures, and never sleeps against the real clock.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.git import Git

GIT_MOD = sys.modules[Git.__module__]


def _install_fake_shell(monkeypatch, *, node_id="PR_node", enqueue=(0, "", ""),
                        merge_state="QUEUED"):
    """Stub `Shell`/`time.sleep` in the `git` module. Returns the recorded
    `gh api graphql` enqueue commands so tests can assert on the mutation."""
    enqueue_cmds = []

    def fake_get_output(command, *_a, **_k):
        if "--json id" in command:
            return node_id
        if "mergeStateStatus" in command:
            return merge_state
        return ""

    def fake_get_res(command, *_a, **_k):
        enqueue_cmds.append(command)
        return enqueue

    monkeypatch.setattr(GIT_MOD.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(GIT_MOD.Shell, "get_res_stdout_stderr", staticmethod(fake_get_res))
    monkeypatch.setattr(GIT_MOD.time, "sleep", lambda *_a, **_k: None)
    return enqueue_cmds


def test_enqueue_uses_graphql_mutation(monkeypatch):
    cmds = _install_fake_shell(monkeypatch)
    assert Git.enqueue_pull_request(123, "ClickHouse/ClickHouse") is True
    assert len(cmds) == 1
    assert "enqueuePullRequest" in cmds[0]
    assert "id=PR_node" in cmds[0]


def test_enqueue_quotes_repo(monkeypatch):
    """The `repo` argument is shell-quoted in the `gh pr view` commands."""
    view_cmds = []

    def fake_get_output(command, *_a, **_k):
        view_cmds.append(command)
        return "PR_node" if "--json id" in command else "QUEUED"

    monkeypatch.setattr(GIT_MOD.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(
        GIT_MOD.Shell, "get_res_stdout_stderr", staticmethod(lambda *_a, **_k: (0, "", ""))
    )
    monkeypatch.setattr(GIT_MOD.time, "sleep", lambda *_a, **_k: None)
    assert Git.enqueue_pull_request(1, "owner/repo; rm -rf /") is True
    assert view_cmds and all("'owner/repo; rm -rf /'" in c for c in view_cmds)


def test_enqueue_dry_run_touches_nothing(monkeypatch):
    def unexpected(*_a, **_k):
        raise AssertionError("dry run must not call Shell")

    monkeypatch.setattr(GIT_MOD.Shell, "get_output", staticmethod(unexpected))
    monkeypatch.setattr(GIT_MOD.Shell, "get_res_stdout_stderr", staticmethod(unexpected))
    assert Git.enqueue_pull_request(1, "ClickHouse/ClickHouse", dry_run=True) is True


def test_enqueue_already_queued_is_success(monkeypatch):
    _install_fake_shell(
        monkeypatch,
        enqueue=(1, "", "Pull request is already in the queue"),
    )
    assert Git.enqueue_pull_request(7, "ClickHouse/ClickHouse") is True


def test_enqueue_real_failure_returns_false(monkeypatch):
    _install_fake_shell(monkeypatch, enqueue=(1, "", "some other error"))
    assert Git.enqueue_pull_request(7, "ClickHouse/ClickHouse") is False


def test_enqueue_retries_until_check_completes(monkeypatch):
    """A required check pending on the first attempts, then the enqueue succeeds:
    retry must wait it out and return True without sleeping against the clock."""
    calls = {"n": 0}

    def fake_get_output(command, *_a, **_k):
        if "--json id" in command:
            return "PR_node"
        return "QUEUED"

    def fake_get_res(command, *_a, **_k):
        calls["n"] += 1
        if calls["n"] < 3:
            return (1, "", 'Required status check "CH Inc sync" is pending.')
        return (0, "", "")

    monkeypatch.setattr(GIT_MOD.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(GIT_MOD.Shell, "get_res_stdout_stderr", staticmethod(fake_get_res))
    monkeypatch.setattr(GIT_MOD.time, "sleep", lambda *_a, **_k: None)
    assert Git.enqueue_pull_request(7, "ClickHouse/ClickHouse", retries=5, delay=1) is True
    assert calls["n"] == 3


def test_enqueue_exhausts_retries_returns_false(monkeypatch):
    _install_fake_shell(
        monkeypatch, enqueue=(1, "", 'Required status check "CH Inc sync" is pending.')
    )
    assert (
        Git.enqueue_pull_request(7, "ClickHouse/ClickHouse", retries=3, delay=1) is False
    )


def test_enqueue_missing_node_id_returns_false(monkeypatch):
    _install_fake_shell(monkeypatch, node_id="")
    assert Git.enqueue_pull_request(7, "ClickHouse/ClickHouse") is False


def test_enqueue_succeeds_even_when_not_yet_queued(monkeypatch):
    # A non-QUEUED merge state after a successful mutation is only a warning:
    # GitHub may still be updating the PR's merge state.
    _install_fake_shell(monkeypatch, merge_state="BEHIND")
    assert Git.enqueue_pull_request(7, "ClickHouse/ClickHouse") is True
