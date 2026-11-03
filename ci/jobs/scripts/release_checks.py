#!/usr/bin/env python3
"""
Dependency-light release helpers.

This module is deliberately kept free of the release toolchain's heavy imports
(`github`, `boto3`, `unidiff`, ...) and of the `ci.praktika` stack. The release
driver `tests/ci/create_release.py` drags in the former at import time, none of
which is shipped in the `CI Tests` docker image, so the pure release-decision
logic lives here -- importable as `ci.jobs.scripts.release_checks` -- where the
`ci/tests/` unit-test suite can exercise it hermetically.
"""


def is_empty_patch_release(patch: int, tweak: int) -> bool:
    """
    Whether a patch release would be empty and must be refused.

    For a patch release the tweak equals the number of commits since the
    previous release tag (see `Git.tweak`), so `tweak == 1` means the only
    commit on top of the previous release is the automated post-release
    version bump — there is nothing to release (e.g. `v25.8.28.1-lts`).

    The exception is `patch == 1`: that is the first user-facing
    `stable`/`lts` release of a freshly cut branch. Its previous tag is the
    non-user-facing `vX.Y.1.1-new`, and the single automated
    `testing -> stable/lts` version-update commit also yields `tweak == 1`.
    That release is legitimate and must be allowed. The post-release bump
    always increments `patch`, so an already-published branch is always at
    `patch >= 2` on a rerun.
    """
    return tweak == 1 and patch != 1
