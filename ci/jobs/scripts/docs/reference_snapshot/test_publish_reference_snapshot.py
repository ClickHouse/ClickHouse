#!/usr/bin/env python3
"""Focused tests for reference snapshot Vercel Blob publication."""

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


HERE = Path(__file__).parent
SPEC = importlib.util.spec_from_file_location("publish_reference_snapshot", HERE / "publish_reference_snapshot.py")
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PublishReferenceSnapshotTest(unittest.TestCase):
    def test_blob_upload_replaces_the_single_head_snapshot(self) -> None:
        requests = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "reference-snapshot.tar.zst"
            archive.write_bytes(b"snapshot")
            with patch.object(MODULE, "urlopen", side_effect=lambda request, timeout: requests.append((request, timeout)) or Response()):
                client = MODULE.VercelBlob("vercel_blob_rw_store_test_secret", "https://example.test/api/blob")
                client.put_head_snapshot(archive)

        request, timeout = requests[0]
        self.assertEqual(timeout, 60)
        self.assertEqual(request.full_url, "https://example.test/api/blob/?pathname=snapshots/head/reference-snapshot.tar.zst")
        self.assertEqual(request.get_header("X-vercel-blob-access"), "private")
        self.assertEqual(request.get_header("X-add-random-suffix"), "0")
        self.assertEqual(request.get_header("X-allow-overwrite"), "1")
        self.assertEqual(request.get_header("X-vercel-blob-store-id"), "store")

    def test_head_path_is_stable(self) -> None:
        self.assertEqual(MODULE.HEAD_SNAPSHOT_PATH, "snapshots/head/reference-snapshot.tar.zst")


if __name__ == "__main__":
    unittest.main()
