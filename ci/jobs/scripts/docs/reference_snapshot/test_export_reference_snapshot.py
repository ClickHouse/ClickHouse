#!/usr/bin/env python3
"""Focused tests for deterministic reference snapshot serialization."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


HERE = Path(__file__).parent
SPEC = importlib.util.spec_from_file_location("export_reference_snapshot", HERE / "export_reference_snapshot.py")
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExportReferenceSnapshotTest(unittest.TestCase):
    def test_canonical_json_sorts_object_keys(self) -> None:
        self.assertEqual(
            MODULE.canonical_json({"z": 1, "a": 2}),
            b'{\n  "a": 2,\n  "z": 1\n}\n',
        )

    def test_json_each_row_is_sorted(self) -> None:
        with patch.object(MODULE, "run_query", return_value='{"name":"z"}\n{"name":"a"}\n'):
            self.assertEqual(
                MODULE.read_json_each_row(Path("clickhouse"), Path("config.xml"), "SELECT 1"),
                [{"name": "a"}, {"name": "z"}],
            )

    def test_snapshot_includes_settings_change_history(self) -> None:
        self.assertIn("settings_changes", MODULE.CATALOG_QUERIES)

    def test_sha256_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "binary"
            path.write_bytes(b"ClickHouse")
            self.assertEqual(
                MODULE.sha256_file(path),
                "dee91ddb90597936302cfd4341e409c87779ed9e0e8c8fde580e0af6cdace179",
            )

    def test_manifest_shape_is_json_serializable(self) -> None:
        manifest = {
            "schemaVersion": MODULE.SNAPSHOT_SCHEMA_VERSION,
            "clickhouseVersion": "26.10.1.1",
            "sourceRevision": None,
            "binarySha256": "def456",
            "configSha256": "abc123",
            "catalogs": [],
        }
        self.assertEqual(json.loads(MODULE.canonical_json(manifest)), manifest)


if __name__ == "__main__":
    unittest.main()
