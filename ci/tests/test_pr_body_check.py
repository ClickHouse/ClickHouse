"""Tests for changelog-entry validation in the PR body hook."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks import pr_body_check


def test_check_changelog_entry_rejects_noncanonical_clickhouse_spelling():
    misspelling = "Click" "house"
    body = """### Changelog entry:

- Add %s support.
""" % misspelling

    assert pr_body_check.check_changelog_entry("New Feature", body) == (
        "The product name is spelled `ClickHouse`: " + misspelling
    )


def test_check_changelog_entry_allows_canonical_spelling_and_urls():
    misspelling_in_url = "Click" "house"
    body = """### Changelog entry:

- Add ClickHouse support; see https://example.test/%s.
""" % misspelling_in_url

    assert pr_body_check.check_changelog_entry("New Feature", body) == ""
