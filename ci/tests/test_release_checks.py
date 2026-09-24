#!/usr/bin/env python3
"""
Unit tests for the pure release-decision helpers in
`ci/jobs/scripts/release_checks.py`. Run as part of the `ci/tests/` suite with
`pytest ci/tests/test_create_release.py` from the repo root.

`release_checks` is the dependency-light home of `is_empty_patch_release` (used
by `tests/ci/create_release.py`'s `ReleaseInfo.prepare`). It is kept free of the
release toolchain's heavy imports (`github`, `boto3`, `unidiff`, ...) and of the
`ci.praktika` stack, so this test imports it as a plain `ci.jobs.scripts` module
without the packages that the `CI Tests` docker image does not ship.
"""

import unittest

from ci.jobs.scripts.release_checks import is_empty_patch_release


class TestIsEmptyPatchRelease(unittest.TestCase):
    def test_rejects_empty_rerun(self):
        # Already-published branch: the only commit since the previous
        # stable/lts tag is the automated version bump (e.g. v25.8.28.1-lts).
        self.assertTrue(is_empty_patch_release(patch=28, tweak=1))

    def test_allows_first_release_of_new_branch(self):
        # First user-facing stable/lts release on a freshly cut branch. Its
        # previous tag is vX.Y.1.1-new and the single testing -> stable commit
        # also yields tweak == 1, but the release is legitimate.
        self.assertFalse(is_empty_patch_release(patch=1, tweak=1))

    def test_allows_non_empty_patch_release(self):
        # Real commits on top of the previous release -> tweak > 1.
        self.assertFalse(is_empty_patch_release(patch=28, tweak=42))

    def test_allows_non_empty_first_release(self):
        self.assertFalse(is_empty_patch_release(patch=1, tweak=2222))


if __name__ == "__main__":
    unittest.main()
