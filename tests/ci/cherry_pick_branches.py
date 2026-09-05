#!/usr/bin/env python3
"""
Pure branch-selection logic for the backport automation (`cherry_pick.py`).

This module is intentionally free of GitHub / git / CI dependencies so the
branch-selection contract can be unit-tested directly (see
`test_cherry_pick_branches.py`). The label name constants live with `Labels`
in `cherry_pick.py` / `pr_info.py` and are passed in by the caller, so this
module stays the single source of truth for *which branches* a PR reaches
without duplicating *what the labels are called*.
"""
import re
from typing import List, Optional, Sequence, Set, Tuple

# Version-specific backport label, e.g. `v25.12-must-backport`.
VERSION_LABEL_RE = re.compile(r"^v(\d+)\.(\d+)-must-backport$")


def version_key(version: str) -> Tuple[int, ...]:
    """
    Turn a release version (e.g. `25.12`, `26.1`) into a tuple of integers so
    that versions compare numerically: `(25, 12) < (26, 1) < (26, 2) < (26, 10)`.
    Plain string comparison would order `26.10` before `26.2`, which is wrong.
    """
    return tuple(int(part) for part in version.split("."))


def branch_version(branch: str) -> Tuple[int, ...]:
    """Release branch name to a comparable version: `release/25.12` -> `(25, 12)`."""
    return version_key(branch.replace("release/", ""))


def label_version(label: str) -> Optional[Tuple[int, ...]]:
    """A version-specific backport label to its version: `v25.12-must-backport`
    -> `(25, 12)`. Returns `None` for any other label."""
    match = VERSION_LABEL_RE.fullmatch(label)
    if match is None:
        return None
    return (int(match.group(1)), int(match.group(2)))


def backport_floor(pr_labels: Sequence[str]) -> Optional[Tuple[int, ...]]:
    """
    The lowest version among the PR's version-specific backport labels
    (`v<MAJOR>.<MINOR>-must-backport`), or `None` if there are none.

    A version-specific label marks the OLDEST release the PR must reach, so the
    minimum of them is the floor: the PR is backported to that release and to
    every newer active release branch. The named release does NOT need to be
    active itself -- a label for an end-of-life release (e.g. `v25.12` when only
    the 26.x line and the 25.8 LTS are active) still pulls the fix forward into
    every active release after it, so upgrading from that release never silently
    loses the fix.
    """
    floors = [
        version
        for version in (label_version(label) for label in pr_labels)
        if version is not None
    ]
    return min(floors) if floors else None


def select_backport_branches(
    pr_labels: Sequence[str],
    release_branches: Sequence[str],
    rolling_out: Set[str],
    *,
    general_backport_labels: Set[str],
    force_backport_label: str,
) -> Tuple[List[str], List[str]]:
    """
    Decide which release branches a PR should be backported to.

    Returns `(branches, skipped)`, both in `release_branches` order:
    - `branches`: release branches the PR should be backported to.
    - `skipped`: rolling-out branches that were excluded, so the caller can
      close any stale cherry-pick / backport PRs for them.

    Rules:
    - `force_backport_label` -> all release branches, ignoring `rolling_out`.
    - any of `general_backport_labels` (`pr-must-backport`, `pr-critical-bugfix`,
      ...) -> all release branches, but a `rolling_out` branch is skipped unless
      a version-specific label covers it (its version is `>= floor`).
    - otherwise (version-specific labels only) -> the floor release and every
      newer active release branch. `rolling_out` does not apply here: an explicit
      version request always proceeds.
    """
    labels = set(pr_labels)
    floor = backport_floor(pr_labels)
    # The branches a version-specific label expands to: the floor release and
    # every newer active branch. Such a label overrides the `rolling_out` skip
    # for exactly these branches.
    covered_by_floor = {
        branch
        for branch in release_branches
        if floor is not None and branch_version(branch) >= floor
    }

    if force_backport_label in labels:
        return list(release_branches), []

    if labels & general_backport_labels:
        branches = [
            branch
            for branch in release_branches
            if branch not in rolling_out or branch in covered_by_floor
        ]
        skipped = [
            branch
            for branch in release_branches
            if branch in rolling_out and branch not in covered_by_floor
        ]
        return branches, skipped

    # Version-specific labels only. `covered_by_floor` is the floor release and
    # every newer active branch. It is normally non-empty -- the search that
    # feeds this function only selects PRs whose floor is not newer than the
    # newest active release -- but it may be empty if a PR carries only a label
    # newer than every active release; the caller skips such PRs gracefully.
    assert floor is not None, (
        "select_backport_branches called without a general backport label and "
        f"without a version-specific label; labels: {sorted(labels)}"
    )
    return [branch for branch in release_branches if branch in covered_by_floor], []
