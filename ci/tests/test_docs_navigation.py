"""Regression tests for documentation navigation completeness."""

import json
import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.docs import mintlify_docs_check, navigation_check


def write_page(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("---\ntitle: Test\n---\n", encoding="utf-8")


def test_finds_only_publishable_pages_missing_from_navigation(tmp_path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    (docs_root / "docs.json").write_text(
        json.dumps({"navigation": {"pages": ["index"]}}),
        encoding="utf-8",
    )
    navigation = docs_root / "section/navigation.json"
    navigation.parent.mkdir()
    navigation.write_text(
        json.dumps(
            {
                "groups": [
                    {
                        "root": "section/landing",
                        "pages": ["section/listed", "section/nested"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    for page in (
        "index.mdx",
        "section/landing.mdx",
        "section/listed.md",
        "section/nested/index.mdx",
        "section/orphan.mdx",
        "get-started/quickstarts/explorer-entry.mdx",
        "clickstack/managed-onboarding/guide.mdx",
        "snippets/partial.mdx",
        "ja/section/orphan.mdx",
        "changelogs/legacy.md",
        "section/_partial.mdx",
        "section/README.mdx",
    ):
        write_page(docs_root / page)

    assert [
        path.as_posix()
        for path in navigation_check.find_unlisted_pages(docs_root)
    ] == ["section/orphan.mdx"]


def test_navigation_check_runs_for_aggregator_and_client_docs():
    assert mintlify_docs_check.NAVIGATION_CHECK in mintlify_docs_check.DEFAULT_CHECKS
    assert mintlify_docs_check.NAVIGATION_CHECK in mintlify_docs_check.CLIENT_CHECKS
    assert mintlify_docs_check.client_checks(True, ["example"])[0] == (
        mintlify_docs_check.NAVIGATION_CHECK
    )
