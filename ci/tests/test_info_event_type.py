"""
Tests for `Info.is_merge_queue_event` and its sibling event predicates.

`_Environment.EVENT_TYPE` always holds a `Workflow.Event` value, never GitHub's
event name. GitHub calls the merge-queue event `merge_group` while
`Workflow.Event.MERGE_QUEUE` is `"merge_queue"`, so a predicate comparing
`EVENT_TYPE` against `"merge_group"` is never true and every merge-queue
consumer silently takes the pull-request branch.

These tests drive the real chain - a GitHub event payload through
`_Environment.from_env()` into the real `Info` property - on purpose. The
merge-queue tests in `test_gh.py` build a `SimpleNamespace` with
`is_merge_queue_event=True`, i.e. they stub out the predicate itself, so they
cannot catch a broken predicate. Do not replace the payload below with a
mocked `Info`.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika._environment import _Environment
from ci.praktika.info import Info
from ci.praktika.workflow import Workflow

QUEUE_HEAD_REF = (
    "gh-readonly-queue/master/pr-110395-4a44dcefc451d3da3f7a9e7c8a38f74180b34ebc"
)

REPOSITORY = {"html_url": "https://github.com/ClickHouse/ClickHouse"}

# One payload per event praktika understands, shaped like the GitHub webhook.
PAYLOADS = {
    Workflow.Event.MERGE_QUEUE: {
        "merge_group": {"head_ref": QUEUE_HEAD_REF},
        "repository": REPOSITORY,
    },
    Workflow.Event.PULL_REQUEST: {
        "action": "opened",
        "pull_request": {
            "number": 110395,
            "head": {"sha": "a" * 40, "repo": {"full_name": "ClickHouse/ClickHouse"}},
            "html_url": "https://github.com/ClickHouse/ClickHouse/pull/110395",
            "body": "",
            "title": "",
            "labels": [],
            "user": {"login": "nobody"},
            "updated_at": "2026-07-28T00:00:00Z",
        },
    },
    Workflow.Event.PUSH: {
        "commits": [],
        "after": "b" * 40,
        "head_commit": {"url": "https://github.com/o/r/commit/b", "message": "m"},
        "repository": {"updated_at": "2026-07-28T00:00:00Z"},
    },
    Workflow.Event.SCHEDULE: {"schedule": "0 0 * * *", "repository": REPOSITORY},
    Workflow.Event.DISPATCH: {"inputs": {}, "repository": REPOSITORY},
}


def _info_for(event, tmp_path, monkeypatch):
    """Build a real `_Environment` from the event payload and hand it to `Info`.

    Only `_Environment.get` is stubbed, so the property under test and the
    `EVENT_TYPE` it reads are both the production ones.
    """
    event_file = tmp_path / f"event_{event}.json"
    event_file.write_text(json.dumps(PAYLOADS[event]), encoding="utf-8")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
    monkeypatch.setenv("GITHUB_SHA", "c" * 40)
    # Avoid the ec2metadata / IMDS lookups from_env falls back to.
    monkeypatch.setenv("INSTANCE_TYPE", "test-instance-type")
    monkeypatch.setenv("INSTANCE_ID", "test-instance-id")
    monkeypatch.setenv("INSTANCE_LIFE_CYCLE", "on-demand")
    env = _Environment.from_env()
    monkeypatch.setattr(_Environment, "get", classmethod(lambda cls: env))
    return Info()


def test_merge_group_payload_yields_merge_queue_event_type(tmp_path, monkeypatch):
    """The GitHub `merge_group` event is stored as `Workflow.Event.MERGE_QUEUE`."""
    info = _info_for(Workflow.Event.MERGE_QUEUE, tmp_path, monkeypatch)
    assert info.env.EVENT_TYPE == Workflow.Event.MERGE_QUEUE
    assert info.env.EVENT_TYPE != "merge_group"


def test_pull_request_payload_exposes_event_action(tmp_path, monkeypatch):
    info = _info_for(Workflow.Event.PULL_REQUEST, tmp_path, monkeypatch)
    assert info.event_action == "opened"


def test_is_merge_queue_event_true_in_merge_queue(tmp_path, monkeypatch):
    info = _info_for(Workflow.Event.MERGE_QUEUE, tmp_path, monkeypatch)
    assert info.is_merge_queue_event


def test_is_merge_queue_event_false_for_other_events(tmp_path, monkeypatch):
    for event in (
        Workflow.Event.PULL_REQUEST,
        Workflow.Event.PUSH,
        Workflow.Event.SCHEDULE,
        Workflow.Event.DISPATCH,
    ):
        info = _info_for(event, tmp_path, monkeypatch)
        assert info.env.EVENT_TYPE == event, event
        assert not info.is_merge_queue_event, event


def test_merge_queue_run_exposes_the_linked_pr(tmp_path, monkeypatch):
    """The predicate is only useful together with the linked PR number the
    merge-queue consumers query the GitHub API with."""
    info = _info_for(Workflow.Event.MERGE_QUEUE, tmp_path, monkeypatch)
    assert info.pr_number == 0
    assert info.linked_pr_number == 110395


def test_push_and_dispatch_predicates(tmp_path, monkeypatch):
    """The sibling predicates compare literals that happen to equal their enum
    values; pin that so a later rename of `Workflow.Event` cannot silently
    break them the way `MERGE_QUEUE` was broken."""
    assert Workflow.Event.PUSH == "push"
    assert Workflow.Event.DISPATCH == "dispatch"

    info = _info_for(Workflow.Event.PUSH, tmp_path, monkeypatch)
    assert info.is_push_event
    assert not info.is_dispatch_event

    info = _info_for(Workflow.Event.DISPATCH, tmp_path, monkeypatch)
    assert info.is_dispatch_event
    assert not info.is_push_event
