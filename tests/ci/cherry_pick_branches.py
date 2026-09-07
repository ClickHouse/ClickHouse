#!/usr/bin/env python3
"""
Pure branch-selection logic for the backport automation (`cherry_pick.py`).

This module is intentionally free of GitHub / git / CI dependencies so the
branch-selection contract can be unit-tested directly (see
`ci/tests/test_cherry_pick_branches.py`). The label name constants live with `Labels`
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
    *,
    general_backport_labels: Set[str],
    force_backport_label: str,
) -> List[str]:
    """
    Decide which release branches a PR should be backported to.

    Returns `branches` in `release_branches` order.

    Rules:
    - `force_backport_label` -> all release branches.
    - any of `general_backport_labels` (`pr-must-backport`, `pr-critical-bugfix`,
      ...) -> all release branches.
    - otherwise (version-specific labels only) -> the floor release and every
      newer active release branch.
    """
    labels = set(pr_labels)
    floor = backport_floor(pr_labels)
    # The branches a version-specific label expands to: the floor release and
    # every newer active branch.
    covered_by_floor = {
        branch
        for branch in release_branches
        if floor is not None and branch_version(branch) >= floor
    }

    if force_backport_label in labels:
        return list(release_branches)

    if labels & general_backport_labels:
        return list(release_branches)

    # Version-specific labels only. `covered_by_floor` is the floor release and
    # every newer active branch. It is normally non-empty -- the search that
    # feeds this function only selects PRs whose floor is not newer than the
    # newest active release -- but it may be empty if a PR carries only a label
    # newer than every active release; the caller skips such PRs gracefully.
    assert floor is not None, (
        "select_backport_branches called without a general backport label and "
        f"without a version-specific label; labels: {sorted(labels)}"
    )
    return [branch for branch in release_branches if branch in covered_by_floor]
