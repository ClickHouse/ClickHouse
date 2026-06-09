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
from typing import List, Optional, Sequence, Set, Tuple


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


def must_backport_label(branch: str) -> str:
    """The version-specific backport label for a release branch."""
    return f"v{branch.replace('release/', '')}-must-backport"


def backport_floor(
    pr_labels: Sequence[str], release_branches: Sequence[str]
) -> Optional[Tuple[int, ...]]:
    """
    The lowest version among the PR's *active* version-specific backport labels
    (`v<MAJOR>.<MINOR>-must-backport`), or `None` if there are none.

    A version-specific label marks the OLDEST release the PR must reach, so the
    minimum of them is the floor: the PR is backported to that release and to
    every newer active release branch. Only labels that match an active release
    branch are considered, so a stale label for an end-of-life release can never
    widen the floor.
    """
    floors = [
        branch_version(branch)
        for branch in release_branches
        if must_backport_label(branch) in pr_labels
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
    floor = backport_floor(pr_labels, release_branches)
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

    # Version-specific labels only. The floor is one of the active release
    # branches, so `covered_by_floor` is never empty here.
    assert floor is not None, (
        "select_backport_branches called without a general backport label and "
        f"without an active version-specific label; labels: {sorted(labels)}"
    )
    return [branch for branch in release_branches if branch in covered_by_floor], []
