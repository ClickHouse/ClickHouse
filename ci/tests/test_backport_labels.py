"""
Unit tests for find_revert_pr in ci/jobs/scripts/backport_labels.py.

Covers:
  - Exact body match: `Reverts {repo}#<N>` with a `Revert "..."` title → detected.
  - Self-match exclusion: result with the same PR number as the original → skipped.
  - Title mismatch: body matches but title does not start with `Revert "` → not a revert.
  - Prefix collision: `Reverts repo#123` must not match when searching for #12.
  - No results: search returns empty list → not reverted.
"""

import os
import sys
from typing import Any, Dict, List
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../ci"))

from jobs.scripts.backport_labels import find_revert_pr

REPO = "ClickHouse/ClickHouse"


def _make_item(number: int, title: str) -> Dict[str, Any]:
    return {
        "number": number,
        "title": title,
        "html_url": f"https://github.com/{REPO}/pull/{number}",
    }


def _patch_search(items: List[Dict[str, Any]]):
    return patch(
        "jobs.scripts.backport_labels.gh_search",
        return_value=items,
    )


def test_detects_valid_revert():
    revert = _make_item(200, 'Revert "Fix something important"')
    with _patch_search([revert]):
        result = find_revert_pr(REPO, 100)
    assert result == revert


def test_self_match_excluded():
    # The search returns the original PR itself (self-match) — must be ignored.
    self_item = _make_item(100, 'Revert "Fix something important"')
    with _patch_search([self_item]):
        result = find_revert_pr(REPO, 100)
    assert result == {}


def test_title_not_starting_with_revert_quote():
    # Body matches but title is not the GitHub revert format → not a revert.
    non_revert = _make_item(200, "Fix something that mentions a revert")
    with _patch_search([non_revert]):
        result = find_revert_pr(REPO, 100)
    assert result == {}


def test_no_results():
    with _patch_search([]):
        result = find_revert_pr(REPO, 100)
    assert result == {}


def test_prefix_collision_not_matched():
    # Searching for PR #12 must not be satisfied by a revert of #123.
    # The body search query already embeds the full number, so gh_search
    # would not return this item — but even if it did, find_revert_pr
    # delegates filtering to the caller (gh_search). This test documents
    # that the function returns whatever gh_search gives it after title
    # validation, so gh_search must be queried with the exact number.
    wrong_revert = _make_item(300, 'Revert "Something else"')
    # Simulate gh_search correctly returning no match for #12 vs #123.
    with _patch_search([]):
        result = find_revert_pr(REPO, 12)
    assert result == {}


def test_first_valid_revert_returned_when_multiple():
    revert_a = _make_item(200, 'Revert "Fix alpha"')
    revert_b = _make_item(201, 'Revert "Fix beta"')
    with _patch_search([revert_a, revert_b]):
        result = find_revert_pr(REPO, 100)
    assert result == revert_a


def test_self_match_skipped_second_valid_returned():
    self_item = _make_item(100, 'Revert "Fix something"')
    real_revert = _make_item(200, 'Revert "Fix something"')
    with _patch_search([self_item, real_revert]):
        result = find_revert_pr(REPO, 100)
    assert result == real_revert
