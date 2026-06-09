#!/usr/bin/env python3
"""
Unit tests for the backport branch-selection contract in
`cherry_pick_branches.py`. Run with `python -m unittest` from `tests/ci/`, or
with `pytest tests/ci/test_cherry_pick_branches.py` from the repo root.

The module under test has no GitHub / git / CI dependencies, so these tests run
anywhere. Label constants mirror `pr_info.Labels` and are kept local to avoid
importing the heavyweight `cherry_pick` module.
"""
import unittest
from typing import List, Set

from cherry_pick_branches import (
    backport_floor,
    branch_version,
    select_backport_branches,
    version_key,
)

# Mirror of the label constants used by `cherry_pick.process_pr`.
MUST_BACKPORT = "pr-must-backport"
MUST_BACKPORT_FORCE = "pr-must-backport-force"
CRITICAL_BUGFIX = "pr-critical-bugfix"  # member of Labels.AUTO_BACKPORT
GENERAL_BACKPORT_LABELS = {MUST_BACKPORT, MUST_BACKPORT_FORCE, CRITICAL_BUGFIX}

# Active release branches as `cherry_pick.py` sees them (private fork uses the
# `release/` prefix; the public repo uses bare names -- both are exercised).
RB = ["release/25.12", "release/26.1", "release/26.2", "release/26.3"]
RB_PUBLIC = ["25.12", "26.1", "26.2", "26.3"]


def select(pr_labels, release_branches, rolling_out=None):
    return select_backport_branches(
        pr_labels,
        release_branches,
        set(rolling_out or []),
        general_backport_labels=GENERAL_BACKPORT_LABELS,
        force_backport_label=MUST_BACKPORT_FORCE,
    )


class TestVersionKey(unittest.TestCase):
    def test_numeric_ordering(self):
        # The whole point of tuple keys: 26.10 must sort *after* 26.2 and 26.9,
        # which naive string comparison ("26.10" < "26.2") gets wrong.
        self.assertEqual(version_key("26.2"), (26, 2))
        self.assertLess(version_key("26.2"), version_key("26.9"))
        self.assertLess(version_key("26.9"), version_key("26.10"))
        self.assertLess(version_key("25.12"), version_key("26.1"))

    def test_branch_version_strips_prefix(self):
        self.assertEqual(branch_version("release/25.12"), (25, 12))
        self.assertEqual(branch_version("26.1"), (26, 1))


class TestBackportFloor(unittest.TestCase):
    def test_single_active_label(self):
        self.assertEqual(backport_floor(["v25.12-must-backport"], RB), (25, 12))

    def test_lowest_of_multiple_labels_wins(self):
        self.assertEqual(
            backport_floor(["v26.2-must-backport", "v25.12-must-backport"], RB),
            (25, 12),
        )

    def test_stale_inactive_label_does_not_lower_floor(self):
        # v25.8 is not an active release branch -> it must be ignored, so the
        # floor stays at the lowest *active* label (26.2), not 25.8.
        self.assertEqual(
            backport_floor(["v25.8-must-backport", "v26.2-must-backport"], RB),
            (26, 2),
        )

    def test_no_version_label(self):
        self.assertIsNone(backport_floor(["pr-must-backport"], RB))


class TestSelectVersionSpecific(unittest.TestCase):
    def test_floor_fans_out_to_all_newer_releases(self):
        # The headline scenario: a PR merged into the development branch with
        # only v25.12-must-backport must reach 25.12, 26.1, 26.2, 26.3.
        branches, skipped = select(["v25.12-must-backport"], RB)
        self.assertEqual(branches, RB)
        self.assertEqual(skipped, [])

    def test_floor_fans_out_public_repo(self):
        branches, skipped = select(["v25.12-must-backport"], RB_PUBLIC)
        self.assertEqual(branches, RB_PUBLIC)
        self.assertEqual(skipped, [])

    def test_mid_floor(self):
        branches, _ = select(["v26.2-must-backport"], RB)
        self.assertEqual(branches, ["release/26.2", "release/26.3"])

    def test_multiple_labels_lowest_wins(self):
        branches, _ = select(["v26.2-must-backport", "v25.12-must-backport"], RB)
        self.assertEqual(branches, RB)

    def test_numeric_ordering_in_selection(self):
        rb = ["release/26.2", "release/26.9", "release/26.10"]
        branches, _ = select(["v26.9-must-backport"], rb)
        # 26.10 must be included (>= 26.9); a string comparison would drop it.
        self.assertEqual(branches, ["release/26.9", "release/26.10"])

    def test_stale_label_does_not_widen_selection(self):
        branches, _ = select(["v25.8-must-backport", "v26.2-must-backport"], RB)
        self.assertEqual(branches, ["release/26.2", "release/26.3"])

    def test_rolling_out_ignored_for_version_specific(self):
        # A pure version-specific request always proceeds, even for a
        # rolling-out branch.
        branches, skipped = select(
            ["v25.12-must-backport"], RB, rolling_out={"release/26.2"}
        )
        self.assertEqual(branches, RB)
        self.assertEqual(skipped, [])

    def test_floor_is_always_an_active_branch_so_never_empty(self):
        branches, _ = select(["v26.3-must-backport"], RB)
        self.assertEqual(branches, ["release/26.3"])

    def test_no_backport_label_raises(self):
        with self.assertRaises(AssertionError):
            select(["pr-feature"], RB)


class TestSelectGeneral(unittest.TestCase):
    def test_all_branches_no_rolling_out(self):
        branches, skipped = select(["pr-must-backport"], RB)
        self.assertEqual(branches, RB)
        self.assertEqual(skipped, [])

    def test_critical_bugfix_is_general(self):
        branches, _ = select(["pr-critical-bugfix"], RB)
        self.assertEqual(branches, RB)

    def test_rolling_out_skipped_without_version_label(self):
        branches, skipped = select(
            ["pr-must-backport"], RB, rolling_out={"release/26.2"}
        )
        self.assertEqual(
            branches, ["release/25.12", "release/26.1", "release/26.3"]
        )
        self.assertEqual(skipped, ["release/26.2"])

    def test_rolling_out_overridden_by_lower_floor(self):
        # pr-must-backport + v25.12-must-backport: the version label covers
        # 26.2 (>= 25.12), so the rolling-out skip is overridden and nothing is
        # skipped. This is the case the docs promise and the general path must
        # honour.
        branches, skipped = select(
            ["pr-must-backport", "v25.12-must-backport"],
            RB,
            rolling_out={"release/26.2"},
        )
        self.assertEqual(branches, RB)
        self.assertEqual(skipped, [])

    def test_rolling_out_partial_override_by_floor(self):
        # Floor 26.3 covers only 26.3; the rolling-out 26.1/26.2 are not covered
        # and stay skipped.
        branches, skipped = select(
            ["pr-must-backport", "v26.3-must-backport"],
            RB,
            rolling_out={"release/26.1", "release/26.2"},
        )
        self.assertEqual(branches, ["release/25.12", "release/26.3"])
        self.assertEqual(skipped, ["release/26.1", "release/26.2"])

    def test_force_ignores_rolling_out(self):
        branches, skipped = select(
            ["pr-must-backport-force"],
            RB,
            rolling_out={"release/26.1", "release/26.2"},
        )
        self.assertEqual(branches, RB)
        self.assertEqual(skipped, [])


if __name__ == "__main__":
    unittest.main()
