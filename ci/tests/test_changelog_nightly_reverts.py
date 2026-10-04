"""
Tests for the revert resolution of the nightly changelog job
(`ci/jobs/changelog_nightly.py`).

A revert chain routinely spans the ~30 nightly increments of a release cycle:
the change lands on one day, the revert arrives on another, the revert of the
revert on a third. Only the last of them is in the raw blocks the run is
looking at, so `resolve_revert_targets` has to follow the chain through pull
requests it was not given. When it cannot, the verifier reads a revert of a
revert as a plain revert of a pull request without an entry and demands that
no trace of it remain, while the editing rules put its link on the entry it
brought back - the contradiction that wedged the 26.9 changelog for a week in
September 2026.
"""

import json
import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import changelog_nightly as cn

# `#100` is the original change, `#200` reverts it, `#300` reverts `#200`.
# `#300` is made by hand: its body quotes the commit instead of carrying the
# `Reverts owner/repo#N` marker, so its only trace of `#200` is the title it
# nests.
ORIGINAL = "100"
INNER_REVERT = "200"
OUTER_REVERT = "300"

METADATA = {
    ORIGINAL: ("Make the thing faster", "Speeds up the thing."),
    INNER_REVERT: ('Revert "Make the thing faster"', "This reverts commit abc123."),
    OUTER_REVERT: (
        'Revert "Revert "Make the thing faster""',
        "This reverts commit def456.",
    ),
}


def _fetch(prs):
    return {pr: METADATA[pr] for pr in prs if pr in METADATA}


def _search(title, before):
    return {
        pr: value
        for pr, value in METADATA.items()
        if value[0] == title and int(pr) < int(before)
    }


def _resolve(raw_prs, search=_search, **kwargs):
    with mock.patch.object(
        cn, "fetch_pull_requests", side_effect=_fetch
    ), mock.patch.object(cn, "search_pull_requests_by_title", side_effect=search):
        return cn.resolve_revert_targets(raw_prs, **kwargs)


def test_nested_revert_of_unknown_revert_is_followed():
    """The intermediate revert is neither in the raw blocks nor in the ledger:
    it has to be found by the title the outer revert nests, and the chain
    followed through it down to the original change."""
    targets, titles, unresolved = _resolve({OUTER_REVERT})
    assert unresolved == []
    assert targets == {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}
    assert titles[INNER_REVERT] == METADATA[INNER_REVERT][0]
    # The net effect of the chain: the original ships, so its entry is not
    # licensed for deletion; only the entry of the cancelled intermediate
    # revert is.
    credits, cancelled = cn.revert_net_effect(targets)
    assert credits == {INNER_REVERT: OUTER_REVERT}
    assert ORIGINAL not in credits
    assert cancelled == {INNER_REVERT}


def test_nested_revert_known_from_the_ledger_is_not_searched_for():
    """What an earlier night recorded is enough; no lookup by title happens."""

    def _no_search(title, before):
        raise AssertionError(f"searched for {title!r} although the ledger knows it")

    targets, _, unresolved = _resolve(
        {OUTER_REVERT},
        search=_no_search,
        known_titles={INNER_REVERT: METADATA[INNER_REVERT][0]},
        known_targets={INNER_REVERT: {ORIGINAL}},
    )
    assert unresolved == []
    assert targets[OUTER_REVERT] == {INNER_REVERT}


def test_ambiguous_title_stays_unresolved():
    """The same change reverted twice in a cycle gives two reverts with the
    same title; neither of them is the one the outer revert names, so the
    chain grants no deletion credit at all."""
    twin = "250"

    def _ambiguous(title, before):
        found = _search(title, before)
        if INNER_REVERT in found:
            found[twin] = METADATA[INNER_REVERT]
        return found

    targets, _, unresolved = _resolve({OUTER_REVERT}, search=_ambiguous)
    assert unresolved == [OUTER_REVERT]
    assert OUTER_REVERT not in targets


def test_plain_revert_binds_to_the_title_it_quotes():
    """The terminal link of the chain: a hand-made revert quotes only a commit
    in its body, so its target is found by the title it nests as well."""
    targets, _, unresolved = _resolve({INNER_REVERT})
    assert unresolved == []
    assert targets == {INNER_REVERT: {ORIGINAL}}
    credits, cancelled = cn.revert_net_effect(targets)
    assert credits == {ORIGINAL: INNER_REVERT}
    assert cancelled == set()


def test_a_title_with_too_many_hits_identifies_nothing():
    """The search is capped, so a title matching the cap may have a twin
    beyond it; nothing is bound rather than the one hit that came back."""
    original = cn.TITLE_SEARCH_LIMIT
    nodes = [
        {"number": 10 + i, "title": METADATA[ORIGINAL][0], "body": ""}
        for i in range(original)
    ]
    out = json.dumps({"data": {"search": {"nodes": nodes}}})
    with mock.patch.object(cn.Shell, "get_output", return_value=out), mock.patch.object(
        cn, "Info", return_value=mock.Mock(repo_name="o/n")
    ):
        assert (
            cn.search_pull_requests_by_title(METADATA[ORIGINAL][0], INNER_REVERT) == {}
        )


def test_marker_in_the_body_binds_without_a_search():
    """The `Reverts owner/repo#N` line GitHub writes into a revert opened with
    the web UI names the target outright."""

    def _no_search(title, before):
        if title == METADATA[INNER_REVERT][0]:
            raise AssertionError("searched although the body names the target")
        return _search(title, before)

    metadata = dict(METADATA)
    metadata[OUTER_REVERT] = (
        metadata[OUTER_REVERT][0],
        f"Reverts ClickHouse/ClickHouse#{INNER_REVERT}",
    )
    with mock.patch.object(
        cn,
        "fetch_pull_requests",
        side_effect=lambda prs: {p: metadata[p] for p in prs if p in metadata},
    ), mock.patch.object(cn, "search_pull_requests_by_title", side_effect=_no_search):
        targets, _, unresolved = cn.resolve_revert_targets({OUTER_REVERT})
    assert unresolved == []
    assert targets == {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
