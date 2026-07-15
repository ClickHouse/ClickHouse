#!/usr/bin/env python3
"""
Unit tests for the pure helpers in `create_release.py`. Run with
`python -m unittest` from `tests/ci/`, or with
`pytest tests/ci/test_create_release.py` from the repo root.
"""

import unittest

from create_release import ReleaseInfo


class TestIsEmptyPatchRelease(unittest.TestCase):
    def test_rejects_empty_rerun(self):
        # Already-published branch: the only commit since the previous
        # stable/lts tag is the automated version bump (e.g. v25.8.28.1-lts).
        self.assertTrue(ReleaseInfo._is_empty_patch_release(patch=28, tweak=1))

    def test_allows_first_release_of_new_branch(self):
        # First user-facing stable/lts release on a freshly cut branch. Its
        # previous tag is vX.Y.1.1-new and the single testing -> stable commit
        # also yields tweak == 1, but the release is legitimate.
        self.assertFalse(ReleaseInfo._is_empty_patch_release(patch=1, tweak=1))

    def test_allows_non_empty_patch_release(self):
        # Real commits on top of the previous release -> tweak > 1.
        self.assertFalse(ReleaseInfo._is_empty_patch_release(patch=28, tweak=42))

    def test_allows_non_empty_first_release(self):
        self.assertFalse(ReleaseInfo._is_empty_patch_release(patch=1, tweak=2222))


if __name__ == "__main__":
    unittest.main()
