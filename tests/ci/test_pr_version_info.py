#!/usr/bin/env python3
"""
Unit tests for the pure helpers in `pr_version_info.py`. Run with
`python -m unittest` from `tests/ci/`, or with
`pytest tests/ci/test_pr_version_info.py` from the repo root.

`pr_version_info` keeps its GitHub / CIDB imports inside `main()`, so importing
the helpers here pulls in no heavyweight dependencies.
"""

import unittest

from pr_version_info import (
    SECTION_END,
    SECTION_START,
    MergedPR,
    has_backport_label,
    has_ignore_label,
    original_pr_number_from_backport_ref,
    partition_merged_prs,
    release_from_backport_ref,
    render_issue_section,
    render_section,
    upsert_section,
    version_key,
)


class TestRenderSection(unittest.TestCase):
    def test_merged_only(self):
        self.assertEqual(
            render_section("26.6.1.1", []),
            "### Version info\n- Merged into: `26.6.1.1`",
        )

    def test_merged_and_backported(self):
        self.assertEqual(
            render_section("26.6.1.1", ["25.12.1.100", "25.8.1.200"]),
            "### Version info\n"
            "- Merged into: `26.6.1.1`\n"
            "- Backported to: `25.12.1.100`, `25.8.1.200`",
        )

    def test_default_branch_version_names_the_first_release(self):
        # An original PR's version comes from the default branch's commit
        # counter (master's `26.2.1.1291` is not a `release/26.2` build); it
        # means the change landed before the `release/26.2` branch was cut,
        # so the release series that first includes it is spelled out.
        self.assertEqual(
            render_section("26.2.1.1291", [], from_default_branch=True),
            "### Version info\n"
            "- Merged into: `26.2.1.1291` (included in `26.2` and later)",
        )

    def test_default_branch_version_with_backports(self):
        self.assertEqual(
            render_section("26.2.1.1291", ["25.8.1.500"], from_default_branch=True),
            "### Version info\n"
            "- Merged into: `26.2.1.1291` (included in `26.2` and later)\n"
            "- Backported to: `25.8.1.500`",
        )

    def test_unparseable_version_gets_no_annotation(self):
        self.assertEqual(
            render_section("not-a-version", [], from_default_branch=True),
            "### Version info\n- Merged into: `not-a-version`",
        )

    def test_backported_only(self):
        self.assertEqual(
            render_section(None, ["25.8.1.200"]),
            "### Version info\n- Backported to: `25.8.1.200`",
        )


class TestUpsertSection(unittest.TestCase):
    def test_appends_when_absent(self):
        body = "Original description."
        section = render_section("26.6.1.1", [])
        result = upsert_section(body, section)
        # Two blank lines separate the section from the preceding description.
        self.assertTrue(result.startswith("Original description.\n\n\n"))
        self.assertIn(SECTION_START, result)
        self.assertIn(SECTION_END, result)
        self.assertIn("Merged into: `26.6.1.1`", result)

    def test_empty_body(self):
        result = upsert_section("", render_section("26.6.1.1", []))
        self.assertTrue(result.startswith(SECTION_START))

    def test_none_body(self):
        result = upsert_section(None, render_section("26.6.1.1", []))
        self.assertTrue(result.startswith(SECTION_START))

    def test_idempotent(self):
        section = render_section("26.6.1.1", ["25.12.1.100"])
        once = upsert_section("Hello.", section)
        twice = upsert_section(once, section)
        self.assertEqual(once, twice)

    def test_replaces_existing_section(self):
        section_v1 = render_section("26.6.1.1", [])
        section_v2 = render_section("26.6.1.1", ["25.12.1.100"])
        body = upsert_section("Description.", section_v1)
        updated = upsert_section(body, section_v2)
        # Exactly one section, with the new content.
        self.assertEqual(updated.count(SECTION_START), 1)
        self.assertEqual(updated.count(SECTION_END), 1)
        self.assertIn("Backported to: `25.12.1.100`", updated)
        self.assertTrue(updated.startswith("Description."))

    def test_preserves_surrounding_text(self):
        section = render_section("26.6.1.1", [])
        body = "Before.\n\n" + f"{SECTION_START}\nold\n{SECTION_END}" + "\n\nAfter."
        updated = upsert_section(body, section)
        self.assertTrue(updated.startswith("Before."))
        self.assertTrue(updated.endswith("After."))
        self.assertNotIn("old", updated)


class TestRenderIssueSection(unittest.TestCase):
    def test_inserts_resolved_by_after_header(self):
        section = render_section("26.6.1.1", ["25.12.1.100"])
        issue_section = render_issue_section(12345, section)
        # No delimiters here -- `upsert_section` adds them, as for PR bodies.
        self.assertNotIn(SECTION_START, issue_section)
        self.assertNotIn(SECTION_END, issue_section)
        # `Resolved by` is the first list item, right after the header.
        self.assertEqual(
            issue_section,
            "### Version info\n"
            "- Resolved by: #12345\n"
            "- Merged into: `26.6.1.1`\n"
            "- Backported to: `25.12.1.100`",
        )

    def test_upserts_into_issue_body_idempotently(self):
        section = render_issue_section(1, render_section("26.6.1.1", []))
        once = upsert_section("Issue description.", section)
        twice = upsert_section(once, section)
        self.assertEqual(once, twice)
        self.assertEqual(once.count(SECTION_START), 1)
        self.assertTrue(once.startswith("Issue description."))
        self.assertIn("- Resolved by: #1", once)


class TestVersionKey(unittest.TestCase):
    def test_orders_numerically(self):
        versions = ["25.8.1.200", "26.6.1.1", "25.12.1.100"]
        ordered = sorted(versions, key=version_key, reverse=True)
        self.assertEqual(ordered, ["26.6.1.1", "25.12.1.100", "25.8.1.200"])


class TestBackportRefParsing(unittest.TestCase):
    def test_valid_ref(self):
        self.assertEqual(
            original_pr_number_from_backport_ref("backport/25.12/92538"), 92538
        )

    def test_release_prefix_ref(self):
        self.assertEqual(
            original_pr_number_from_backport_ref("backport/release/25.12/92538"),
            92538,
        )

    def test_non_backport_ref(self):
        self.assertIsNone(original_pr_number_from_backport_ref("my-feature-branch"))
        self.assertIsNone(original_pr_number_from_backport_ref("cherrypick/25.12/1"))

    def test_release_extraction(self):
        self.assertEqual(release_from_backport_ref("backport/25.12/92538"), "25.12")
        self.assertEqual(
            release_from_backport_ref("backport/release/26.5/58548"), "release/26.5"
        )
        self.assertIsNone(release_from_backport_ref("my-feature-branch"))


class TestHasBackportLabel(unittest.TestCase):
    def test_plain_labels(self):
        self.assertTrue(has_backport_label(["pr-must-backport"]))
        self.assertTrue(has_backport_label(["pr-backports-created"]))

    def test_version_specific_label(self):
        self.assertTrue(has_backport_label(["v25.12-must-backport"]))

    def test_no_backport_label(self):
        self.assertFalse(has_backport_label(["pr-feature", "documentation"]))


class TestPartitionMergedPRs(unittest.TestCase):
    def test_original_and_its_backport_in_window(self):
        prs = [
            MergedPR(100, "feature-branch", "master", ["pr-must-backport"]),
            MergedPR(200, "backport/25.12/100", "25.12", ["pr-backport"]),
        ]
        backports, originals, need_scan = partition_merged_prs(prs, "master")
        self.assertEqual(backports, {200})
        self.assertEqual(originals, {100})
        self.assertEqual(need_scan, {100})

    def test_backport_merged_later_pulls_in_original(self):
        # Only the backport is in the lookback window; the original (#100)
        # merged long ago and is not present. It must still be pulled into the
        # originals/need_scan sets so its `Backported to` list gets refreshed.
        prs = [MergedPR(200, "backport/release/25.8/100", "release/25.8", [])]
        backports, originals, need_scan = partition_merged_prs(prs, "master")
        self.assertEqual(backports, {200})
        self.assertEqual(originals, {100})
        self.assertEqual(need_scan, {100})

    def test_original_without_backport_label_is_not_scanned(self):
        prs = [MergedPR(100, "feature-branch", "master", ["pr-feature"])]
        backports, originals, need_scan = partition_merged_prs(prs, "master")
        self.assertEqual(backports, set())
        self.assertEqual(originals, {100})
        self.assertEqual(need_scan, set())

    def test_pr_to_release_branch_directly_is_ignored(self):
        # A non-backport PR targeting a release branch is neither an original
        # (not the default branch) nor a backport.
        prs = [MergedPR(300, "some-fix", "25.12", [])]
        backports, originals, need_scan = partition_merged_prs(prs, "master")
        self.assertEqual((backports, originals, need_scan), (set(), set(), set()))

    def test_ignore_label_pr_is_dropped(self):
        # The automated periodic upstream-sync PR merges to master but must not
        # get a version-info section.
        prs = [
            MergedPR(
                400, "sync-upstream/master", "master", ["pr-periodic-sync-upstream"]
            ),
            MergedPR(401, "feature-branch", "master", ["pr-must-backport"]),
        ]
        backports, originals, need_scan = partition_merged_prs(prs, "master")
        self.assertEqual(backports, set())
        self.assertEqual(originals, {401})
        self.assertEqual(need_scan, {401})


class TestHasIgnoreLabel(unittest.TestCase):
    def test_periodic_sync_label(self):
        self.assertTrue(has_ignore_label(["pr-periodic-sync-upstream"]))

    def test_other_labels(self):
        self.assertFalse(has_ignore_label(["pr-sync-upstream", "pr-backport"]))

    def test_no_labels(self):
        self.assertFalse(has_ignore_label([]))


if __name__ == "__main__":
    unittest.main()
