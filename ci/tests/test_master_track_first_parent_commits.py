"""
Tests for the first-parent reconstruction of `master_track_commits_sha`
(`ci.jobs.scripts.workflow_hooks.store_data`).

`repos/ClickHouse/ClickHouse/commits?sha=master` lists every commit reachable
from master, so a merge of a long PR branch injects that branch's commits into
the listing. Consumers walk the stored list commit by commit looking for the
previous master run (the perf `release_base` delta gate), so a listing taken
as-is can be exhausted by one big merge and lose the predecessor that exists
just outside the fetched window. These tests pin that the hook stores the
first-parent chain instead: side-branch commits are dropped, the walk pages
until it has the requested number of master commits, and it stops loudly
instead of stitching a chain with a hole in it.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `store_data` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.jobs.scripts.workflow_hooks.store_data as m


def make_pages(chain, side_branches=None, page_size=100):
    """A fake `/commits` listing over a synthetic history.

    `chain` is the first-parent chain, newest first. `side_branches` maps a
    merge commit of the chain to the commits merged into it; those appear in
    the listing (as GitHub returns them) but must not appear in the result.
    """
    side_branches = side_branches or {}
    entries = {}
    order = []
    for i, sha in enumerate(chain):
        parent = chain[i + 1] if i + 1 < len(chain) else ""
        entries[sha] = parent
        order.append(sha)
        for j, side in enumerate(side_branches.get(sha, [])):
            side_parent = (
                side_branches[sha][j + 1] if j + 1 < len(side_branches[sha]) else parent
            )
            entries[side] = side_parent
            order.append(side)

    def list_page(anchor):
        assert anchor in entries, f"unexpected anchor {anchor}"
        start = order.index(anchor)
        return [(sha, entries[sha]) for sha in order[start : start + page_size]]

    return list_page


def test_side_branch_commits_are_not_part_of_the_chain():
    chain = [f"m{i}" for i in range(5)]
    list_page = make_pages(chain, side_branches={"m1": ["s1", "s2", "s3"]})
    assert m.get_master_first_parent_commits("m0", 5, list_page) == chain


def test_chain_is_truncated_to_the_requested_count():
    chain = [f"m{i}" for i in range(10)]
    list_page = make_pages(chain)
    assert m.get_master_first_parent_commits("m0", 3, list_page) == ["m0", "m1", "m2"]


def test_walk_pages_past_a_merge_that_fills_the_first_page():
    # One merge brings in more side-branch commits than a page holds: the
    # single-page listing the hook used before could not see any older master
    # commit at all, which is exactly the case the walk has to survive.
    chain = [f"m{i}" for i in range(30)]
    list_page = make_pages(
        chain, side_branches={"m1": [f"s{i}" for i in range(150)]}, page_size=100
    )
    assert m.get_master_first_parent_commits("m0", 30, list_page) == chain


def test_walk_stops_at_the_root_commit():
    chain = ["m0", "m1"]
    list_page = make_pages(chain)
    assert m.get_master_first_parent_commits("m0", 50, list_page) == chain


def test_a_listing_without_its_own_anchor_stops_the_walk():
    # A failing `gh api` returns an empty output: the chain built so far is
    # returned instead of skipping to whatever the next listing happens to
    # contain, so no consumer ever sees a chain with a hole in it.
    chain = [f"m{i}" for i in range(5)]
    full_page = make_pages(chain, page_size=2)
    calls = []

    def list_page(anchor):
        calls.append(anchor)
        return full_page(anchor) if len(calls) == 1 else []

    assert m.get_master_first_parent_commits("m0", 5, list_page) == ["m0", "m1"]
    assert calls == ["m0", "m2"]


def test_page_budget_bounds_the_walk():
    chain = [f"m{i}" for i in range(200)]
    list_page = make_pages(chain, page_size=1)
    got = m.get_master_first_parent_commits("m0", 200, list_page)
    assert got == chain[: m.MASTER_TRACK_MAX_PAGES]


def test_parsing_of_the_commits_listing(monkeypatch):
    output = "\n".join(
        [
            "aaa\tbbb",
            "bbb\tccc",
            "",  # a blank line in the `gh` output is ignored
            "ccc\t",  # a root commit has no first parent
        ]
    )
    monkeypatch.setattr(m.Shell, "get_output", lambda *args, **kwargs: output)
    assert m._list_master_commits_page("aaa") == [
        ("aaa", "bbb"),
        ("bbb", "ccc"),
        ("ccc", ""),
    ]


def test_a_failing_fetch_yields_no_entries(monkeypatch):
    # `Shell.get_output` returns an empty string when the command fails.
    monkeypatch.setattr(m.Shell, "get_output", lambda *args, **kwargs: "")
    assert m._list_master_commits_page("aaa") == []
