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
    label_version,
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


def select(pr_labels, release_branches, rolling_out=None, explicit_branches=()):
    return select_backport_branches(
        pr_labels,
        release_branches,
        set(rolling_out or []),
        general_backport_labels=GENERAL_BACKPORT_LABELS,
        force_backport_label=MUST_BACKPORT_FORCE,
        explicit_branches=explicit_branches,
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


class TestLabelVersion(unittest.TestCase):
    def test_parses_version_specific_label(self):
        self.assertEqual(label_version("v25.12-must-backport"), (25, 12))
        self.assertEqual(label_version("v26.1-must-backport"), (26, 1))

    def test_rejects_other_labels(self):
        self.assertIsNone(label_version("pr-must-backport"))
        # A longer label must not match the version prefix.
        self.assertIsNone(label_version("v25.12-must-backport-synced"))


class TestBackportFloor(unittest.TestCase):
    def test_single_label(self):
        self.assertEqual(backport_floor(["v25.12-must-backport"]), (25, 12))

    def test_lowest_of_multiple_labels_wins(self):
        self.assertEqual(
            backport_floor(["v26.2-must-backport", "v25.12-must-backport"]),
            (25, 12),
        )

    def test_inactive_label_still_sets_floor(self):
        # A version label is honoured by its literal version regardless of
        # whether that release is still active. 25.8 lowers the floor below
        # 26.2 so the fix is pulled forward into every newer active release.
        self.assertEqual(
            backport_floor(["v25.8-must-backport", "v26.2-must-backport"]),
            (25, 8),
        )

    def test_no_version_label(self):
        self.assertIsNone(backport_floor(["pr-must-backport"]))


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

    def test_inactive_label_widens_selection(self):
        # 25.8 is older than every branch in RB and not active here, but its
        # label still sets the floor, so the fix fans out to all of RB.
        branches, _ = select(["v25.8-must-backport", "v26.2-must-backport"], RB)
        self.assertEqual(branches, RB)

    def test_eol_named_release_fans_out_to_newer_active(self):
        # The #101644 scenario: the named release (25.12) is end-of-life and not
        # in the active set, yet the fix must still reach every newer active
        # release (here 26.2, 26.3 -- e.g. the private fork keeps them alive).
        branches, skipped = select(
            ["v25.12-must-backport"], ["release/26.2", "release/26.3"]
        )
        self.assertEqual(branches, ["release/26.2", "release/26.3"])
        self.assertEqual(skipped, [])

    def test_floor_newer_than_all_active_yields_no_branches(self):
        # A label newer than every active release expands to nothing; the
        # function returns an empty list rather than raising.
        branches, skipped = select(["v27.1-must-backport"], RB)
        self.assertEqual(branches, [])
        self.assertEqual(skipped, [])

    def test_rolling_out_ignored_for_version_specific(self):
        # A pure version-specific request always proceeds, even for a
        # rolling-out branch.
        branches, skipped = select(
            ["v25.12-must-backport"], RB, rolling_out={"release/26.2"}
        )
        self.assertEqual(branches, RB)
        self.assertEqual(skipped, [])

    def test_floor_at_newest_branch_selects_only_it(self):
        branches, _ = select(["v26.3-must-backport"], RB)
        self.assertEqual(branches, ["release/26.3"])

    def test_no_backport_label_raises(self):
        with self.assertRaises(AssertionError):
            select(["pr-feature"], RB)


class TestExplicitNamedBranches(unittest.TestCase):
    # The #106466 scenario: the named release 26.2 is end-of-life (not in the
    # active set) but its branch still exists. The caller resolves that and
    # passes it as an explicit branch, so it is backported to in addition to the
    # floor fan-out over the newer active releases.
    ACTIVE = ["25.8", "26.3", "26.4", "26.5"]

    def test_eol_named_branch_added_to_floor_fanout(self):
        branches, skipped = select(
            ["v26.2-must-backport"], self.ACTIVE, explicit_branches=["26.2"]
        )
        # Ordered by version: the explicit EOL branch sorts in among the active.
        self.assertEqual(branches, ["26.2", "26.3", "26.4", "26.5"])
        self.assertEqual(skipped, [])

    def test_eol_branch_below_active_set(self):
        # Only the explicitly named existing branch is older than every active
        # release; the floor (24.3) fans out to all active and 24.3 is added.
        branches, _ = select(
            ["v24.3-must-backport"], self.ACTIVE, explicit_branches=["24.3"]
        )
        self.assertEqual(branches, ["24.3", "25.8", "26.3", "26.4", "26.5"])

    def test_explicit_branch_with_general_label(self):
        branches, skipped = select(
            ["pr-must-backport", "v26.2-must-backport"],
            self.ACTIVE,
            explicit_branches=["26.2"],
        )
        self.assertEqual(branches, ["25.8", "26.2", "26.3", "26.4", "26.5"])
        self.assertEqual(skipped, [])

    def test_explicit_branch_with_force_label(self):
        branches, _ = select(
            ["pr-must-backport-force", "v26.2-must-backport"],
            self.ACTIVE,
            explicit_branches=["26.2"],
        )
        self.assertEqual(branches, ["25.8", "26.2", "26.3", "26.4", "26.5"])

    def test_no_duplicate_when_explicit_also_active(self):
        # Defensive: even if an active branch is passed as explicit, it appears
        # once. (The caller filters active versions out, so this is belt-and-braces.)
        branches, _ = select(
            ["v26.3-must-backport"], self.ACTIVE, explicit_branches=["26.3"]
        )
        self.assertEqual(branches, ["26.3", "26.4", "26.5"])

    def test_only_eol_named_branch_no_newer_active(self):
        # Floor newer than every active release, but the named branch exists:
        # back-port only to it (the fan-out is empty).
        branches, _ = select(
            ["v26.6-must-backport"], ["25.8", "26.3"], explicit_branches=["26.6"]
        )
        self.assertEqual(branches, ["26.6"])

    def test_no_explicit_branches_is_unchanged(self):
        branches, _ = select(["v26.3-must-backport"], self.ACTIVE)
        self.assertEqual(branches, ["26.3", "26.4", "26.5"])


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
