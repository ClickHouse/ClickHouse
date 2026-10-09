#!/usr/bin/env python3
"""Focused tests for the reference snapshot CI wrappers."""

import importlib.util
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch


HERE = Path(__file__).parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PUBLISH = load_module("publish_reference_snapshot_job", HERE / "publish_reference_snapshot.py")
EXPORT = load_module("reference_snapshot_job", HERE / "reference_snapshot.py")


class PublishReferenceSnapshotTest(unittest.TestCase):
    def test_current_base_branch_requires_remote_tip(self) -> None:
        with (
            patch.object(PUBLISH, "Info", return_value=Mock(git_branch="master")),
            patch.object(PUBLISH.Shell, "check", return_value=True) as check,
            patch.object(PUBLISH.Shell, "get_output", side_effect=["same", "same"]),
        ):
            self.assertTrue(PUBLISH.on_current_base_branch())

        check.assert_called_once_with("git fetch --no-tags origin master", verbose=True)

    def test_current_base_branch_rejects_stale_master_rerun(self) -> None:
        with (
            patch.object(PUBLISH, "Info", return_value=Mock(git_branch="master")),
            patch.object(PUBLISH.Shell, "check", return_value=True),
            patch.object(PUBLISH.Shell, "get_output", side_effect=["old", "current"]),
        ):
            self.assertFalse(PUBLISH.on_current_base_branch())


@unittest.skipUnless(shutil.which("zstd"), "zstd is required")
class DeterministicArchiveTest(unittest.TestCase):
    def test_equal_snapshot_content_produces_equal_archive_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archives = []
            for index in range(2):
                snapshot = root / f"input-{index}" / "reference-snapshot"
                (snapshot / "catalogs").mkdir(parents=True)
                (snapshot / "catalogs" / "settings.json").write_text('{"a": 1}\n')
                (snapshot / "manifest.json").write_text('{"catalogs": []}\n')
                timestamp = 1_000_000_000 + index
                for path in (snapshot, snapshot / "catalogs", snapshot / "catalogs" / "settings.json", snapshot / "manifest.json"):
                    os.utime(path, (timestamp, timestamp))
                archive = root / f"snapshot-{index}.tar.zst"
                EXPORT.create_deterministic_archive(snapshot, archive)
                archives.append(archive.read_bytes())

        self.assertEqual(archives[0], archives[1])


if __name__ == "__main__":
    unittest.main()
