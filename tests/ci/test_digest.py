#!/usr/bin/env python

import unittest
from hashlib import md5
from pathlib import Path

import digest_helper as dh
from env_helper import ROOT_DIR

_12 = b"12\n"
_13 = b"13\n"
_14 = b"14\n"


# pylint:disable=protected-access
class TestDigests(unittest.TestCase):
    tests_dir = Path(ROOT_DIR) / "tests/ci/tests/digests"
    broken_link = tests_dir / "broken-symlink"
    empty_digest = "d41d8cd98f00b204e9800998ecf8427e"

    def test__digest_file(self):
        hash_tested = md5()
        with self.assertRaises(
            AssertionError, msg="_digest_file shouldn't work with dirs"
        ):
            dh._digest_file(self.tests_dir, hash_tested)
        with self.assertRaises(
            AssertionError, msg="_digest_file shouldn't work with broken links"
        ):
            dh._digest_file(self.broken_link, hash_tested)

        # file with content '12\n'
        hash_expected = md5()
        hash_expected.update(_12)
        dh._digest_file(self.tests_dir / "12", hash_tested)
        self.assertEqual(hash_expected.digest(), hash_tested.digest())
        # symlink to '12\n'
        hash_tested = md5()
        dh._digest_file(self.tests_dir / "symlink-12", hash_tested)
        self.assertEqual(hash_expected.digest(), hash_tested.digest())

    def test_digest_path(self):
        # test broken link does nothing
        self.assertEqual(
            self.empty_digest, dh.digest_path(self.broken_link).hexdigest()
        )
        # Test file works fine
        hash_expected = md5()
        hash_expected.update(_12)
        self.assertEqual(
            hash_expected.digest(), dh.digest_path(self.tests_dir / "12").digest()
        )
        # Test directory works fine
        hash_expected = md5()
        hash_expected.update(_12 + _14)
        self.assertEqual(
            hash_expected.digest(), dh.digest_path(self.tests_dir / "dir1").digest()
        )
        # Test existed hash is updated from symlink dir3
        hash_tested = hash_expected.copy()
        dh.digest_path(self.tests_dir / "dir3", hash_tested)
        hash_expected = md5()
        hash_expected.update(_12 + _14 + _12 + _13)
        self.assertEqual(hash_expected.digest(), hash_tested.digest())
        # Test the full content of the following structure
        # tests/digests
        # ├── 12
        # ├── dir1
        # │   ├── 12
        # │   └── subdir1_1
        # │       └── 14
        # ├── dir2
        # │   ├── 12
        # │   └── 13
        # ├── dir3 -> dir2
        # └── symlink-12 -> 12
        hash_expected = md5()
        hash_expected.update(_12 * 2 + _14 + (_12 + _13) * 2 + _12)
        self.assertEqual(
            hash_expected.hexdigest(), dh.digest_path(self.tests_dir).hexdigest()
        )

    def test_digest_paths(self):
        # test paths order matters
        hash_ordered = dh.digest_paths(
            (self.tests_dir / d for d in ("dir1", "dir2", "dir3"))
        )
        hash_reversed = dh.digest_paths(
            (self.tests_dir / d for d in ("dir3", "dir2", "dir1"))
        )
        hash_unordered = dh.digest_paths(
            (self.tests_dir / d for d in ("dir3", "dir1", "dir2"))
        )
        self.assertEqual(hash_ordered.digest(), hash_unordered.digest())
        self.assertEqual(hash_ordered.digest(), hash_reversed.digest())
        self.assertEqual(hash_unordered.digest(), hash_reversed.digest())

    @classmethod
    def setUpClass(cls):
        # create a broken symlink
        (TestDigests.broken_link).symlink_to("non-existent-link")

    @classmethod
    def tearDownClass(cls):
        (TestDigests.broken_link).unlink()
