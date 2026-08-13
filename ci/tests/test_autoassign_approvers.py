"""
Tests for the approver discovery of `ci/jobs/autoassign_approvers.py`.

The job assigns the approver of an unassigned PR, so it must look at the
*current* review state: an approval that was later replaced by
`CHANGES_REQUESTED`, or dismissed, must not make its author a candidate, while a
plain `COMMENTED` review left after an approval must not revoke it. The full,
paginated review stream is the only place where that is visible - the `reviews`
list embedded in `gh pr list` output is capped at the first 100 events.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import json

import ci.jobs.autoassign_approvers as aa


def _reviews(*events):
    """
    Render a review stream the way `gh api .../reviews --paginate --jq ...` does:
    one JSON object per line, in chronological order. `COMMENTED` and `PENDING`
    reviews are dropped by the `jq` filter, so they never reach the parser.
    """
    return "\n".join(
        json.dumps({"login": login, "state": state, "at": at})
        for login, state, at in events
        if state not in ("COMMENTED", "PENDING")
    )


def _patch_reviews(monkeypatch, output):
    monkeypatch.setattr(
        aa.Shell, "get_output_or_raise", staticmethod(lambda *_, **__: output)
    )


def test_approval_replaced_by_changes_requested_is_not_an_approval(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("alice", "APPROVED", "2026-01-01T00:00:00Z"),
            ("alice", "CHANGES_REQUESTED", "2026-01-02T00:00:00Z"),
        ),
    )
    assert aa.fetch_approvers(1, {"alice"}) == {}


def test_dismissed_approval_is_not_an_approval(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(("alice", "DISMISSED", "2026-01-01T00:00:00Z")),
    )
    assert aa.fetch_approvers(1, {"alice"}) == {}


def test_reapproval_after_changes_requested_counts(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("alice", "APPROVED", "2026-01-01T00:00:00Z"),
            ("alice", "CHANGES_REQUESTED", "2026-01-02T00:00:00Z"),
            ("alice", "APPROVED", "2026-01-03T00:00:00Z"),
        ),
    )
    approvers = aa.fetch_approvers(1, {"alice"})
    assert list(approvers) == ["alice"]
    # The latest approval, not the first one - it is compared against the time of
    # the latest unassignment.
    assert approvers["alice"] == aa.parse_timestamp("2026-01-03T00:00:00Z")


def test_comment_after_approval_does_not_revoke_it(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("alice", "APPROVED", "2026-01-01T00:00:00Z"),
            ("alice", "COMMENTED", "2026-01-02T00:00:00Z"),
        ),
    )
    assert list(aa.fetch_approvers(1, {"alice"})) == ["alice"]


def test_approvers_are_ordered_by_their_first_approval(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("alice", "CHANGES_REQUESTED", "2026-01-01T00:00:00Z"),
            ("bob", "APPROVED", "2026-01-02T00:00:00Z"),
            ("alice", "APPROVED", "2026-01-03T00:00:00Z"),
        ),
    )
    assert list(aa.fetch_approvers(1, {"alice", "bob"})) == ["bob", "alice"]


def test_non_org_reviewers_are_ignored(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("outsider", "APPROVED", "2026-01-01T00:00:00Z"),
            ("alice", "APPROVED", "2026-01-02T00:00:00Z"),
        ),
    )
    assert list(aa.fetch_approvers(1, {"alice"})) == ["alice"]


def test_stale_approver_is_not_assigned(monkeypatch):
    """
    The end-to-end property: a reviewer who approved and then requested changes is
    not assigned, even though nobody ever unassigned them.
    """
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("alice", "APPROVED", "2026-01-01T00:00:00Z"),
            ("alice", "CHANGES_REQUESTED", "2026-01-02T00:00:00Z"),
        ),
    )
    calls = []
    monkeypatch.setattr(
        aa, "fetch_removed_assignees", lambda pr_number: calls.append(pr_number) or {}
    )
    assert aa.select_approver_to_assign({"number": 1}, {"alice"}) is None
    # No approver, no reason to read the assignment history.
    assert calls == []


def test_current_approver_is_assigned(monkeypatch):
    _patch_reviews(
        monkeypatch,
        _reviews(
            ("alice", "CHANGES_REQUESTED", "2026-01-01T00:00:00Z"),
            ("alice", "APPROVED", "2026-01-02T00:00:00Z"),
        ),
    )
    monkeypatch.setattr(aa, "fetch_removed_assignees", lambda pr_number: {})
    assert aa.select_approver_to_assign({"number": 1}, {"alice"}) == "alice"
