"""Tests for changelog-entry validation in the PR body hook."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks import pr_body_check


@pytest.mark.parametrize(
    "misspelling",
    [
        "Clickhouse",
        "clickHouse",
        "click_house",
        "click-house",
        "CLICK_HOUSE",
        "Click House",
    ],
)
def test_check_changelog_entry_rejects_noncanonical_clickhouse_spelling(misspelling):
    body = """### Changelog entry:

- Add %s support.
""" % misspelling

    assert pr_body_check.check_changelog_entry("New Feature", body) == (
        "The product name is spelled `ClickHouse`: " + misspelling
    )


def test_check_changelog_entry_resumes_checking_after_a_url():
    body = """### Changelog entry:

- Add ClickHouse support; see https://example.test/Clickhouse; fix click-house support.
"""

    assert pr_body_check.check_changelog_entry("New Feature", body) == (
        "The product name is spelled `ClickHouse`: click-house"
    )


@pytest.mark.parametrize("spelling", ["ClickHouse", "clickhouse", "CLICKHOUSE"])
def test_check_changelog_entry_allows_canonical_clickhouse_spelling(spelling):
    body = f"""### Changelog entry:

- Add {spelling} support.
"""

    assert pr_body_check.check_changelog_entry("New Feature", body) == ""


def test_check_changelog_entry_does_not_join_unrelated_words():
    body = """### Changelog entry:

- Improve mouse click. Household settings are unchanged.
"""

    assert pr_body_check.check_changelog_entry("New Feature", body) == ""
