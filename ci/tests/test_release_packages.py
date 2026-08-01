#!/usr/bin/env python3
"""
Unit tests for `ci/jobs/scripts/release_packages.py`, the single source of truth
for the release build artifacts `CreateRelease` publishes/downloads. Run as part
of the `ci/tests/` suite with `pytest ci/tests/` from the repo root.

`release_packages` is stdlib-only, so it imports as a plain `ci.jobs.scripts`
module without `boto3` / `ci.praktika`. The coupling test that ties it back to
`PackageDownloader` imports `create_release` lazily behind `importorskip` so it
is skipped where that dependency is absent, like `test_create_release.py`.
"""

import os
import sys
import unittest

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "../.."))
sys.path.insert(0, REPO_ROOT)

from ci.jobs.scripts import release_packages as rp  # noqa: E402


class TestReleasePackagesEnumeration(unittest.TestCase):
    def test_layout(self):
        # Releases use the `REFs/<branch>` prefix and `build_<arch>_release` /
        # `build_<arch>_darwin` job dirs.
        self.assertEqual(rp.s3_release_prefix("26.3"), "REFs/26.3")
        objs = rp.expected_s3_objects("26.3.9.100")
        self.assertEqual(
            set(objs),
            {
                "build_amd_release",
                "build_arm_release",
                "build_amd_darwin",
                "build_arm_darwin",
            },
        )
        # 6 packages * 4 files (deb/rpm/tgz/tgz.sha512) per build job.
        self.assertEqual(len(objs["build_amd_release"]), 24)
        self.assertEqual(objs["build_amd_darwin"], {rp.MACOS_S3_OBJECT})
        # 6*4*2 packages + 2 macOS binaries.
        self.assertEqual(sum(len(v) for v in objs.values()), 50)
        # Exact filenames for one package pin the deb/rpm/tgz naming pattern.
        amd = objs["build_amd_release"]
        self.assertIn("clickhouse-server_26.3.9.100_amd64.deb", amd)
        self.assertIn("clickhouse-server-26.3.9.100.x86_64.rpm", amd)
        self.assertIn("clickhouse-server-26.3.9.100-amd64.tgz", amd)
        self.assertIn("clickhouse-server-26.3.9.100-amd64.tgz.sha512", amd)
        arm = objs["build_arm_release"]
        self.assertIn("clickhouse-server_26.3.9.100_arm64.deb", arm)
        self.assertIn("clickhouse-server-26.3.9.100.aarch64.rpm", arm)


class _FakeS3:
    """Minimal `list_prefix` stand-in: returns the keys present under a prefix."""

    def __init__(self, keys):
        self._keys = list(keys)

    def list_prefix(self, prefix):
        return [k for k in self._keys if k.startswith(prefix)]


class TestReleaseBuildArtifactsReady(unittest.TestCase):
    def _all_keys(self, release, commit_sha, version):
        prefix = rp.s3_release_prefix(release)
        keys = []
        for job, files in rp.expected_s3_objects(version).items():
            for name in files:
                keys.append(f"{prefix}/{commit_sha}/{job}/{name}")
        return keys

    def test_ready_when_every_object_present(self):
        keys = self._all_keys("26.3", "deadbeef", "26.3.9.100")
        self.assertTrue(
            rp.release_build_artifacts_ready(
                _FakeS3(keys), "26.3", "deadbeef", "26.3.9.100"
            )
        )

    def test_fails_closed_on_a_single_missing_object(self):
        # Drop one object (e.g. a macOS binary a `skipped` darwin build never
        # uploaded) and the whole commit must be rejected.
        keys = self._all_keys("26.3", "deadbeef", "26.3.9.100")
        keys = [k for k in keys if not k.endswith("/build_arm_darwin/clickhouse")]
        self.assertFalse(
            rp.release_build_artifacts_ready(
                _FakeS3(keys), "26.3", "deadbeef", "26.3.9.100"
            )
        )


class TestPackageDownloaderCoupling(unittest.TestCase):
    """The producer (`PackageDownloader`) and the checker (`expected_s3_objects`)
    must enumerate the identical object set, or the `AutoReleases` gate would
    accept/reject the wrong commits."""

    def test_package_downloader_matches_expected_s3_objects(self):
        pytest.importorskip("boto3")
        from ci.jobs.scripts.create_release import PackageDownloader

        release, version = "26.3", "26.3.9.100"
        pd = PackageDownloader(release=release, commit_sha="deadbeef", version=version)
        # Rebuild PackageDownloader's S3-object view: deb/rpm/tgz files by
        # job, plus the fixed `clickhouse` object under each darwin job.
        pd_objects = {}  # type: dict[str, set]
        for package_file, job in pd.file_to_job_name.items():
            pd_objects.setdefault(job, set()).add(package_file)
        for job in pd.macos_binary_to_job_name.values():
            pd_objects.setdefault(job, set()).add(rp.MACOS_S3_OBJECT)
        self.assertEqual(
            pd_objects,
            rp.expected_s3_objects(version),
            f"PackageDownloader vs expected_s3_objects drift for {release}",
        )


if __name__ == "__main__":
    unittest.main()
