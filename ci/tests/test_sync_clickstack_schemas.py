import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.docs import sync_clickstack_schemas as sync


UPSTREAM_SCHEMA = """\
-- +goose Up
CREATE TABLE IF NOT EXISTS ${DATABASE}.otel_logs
(
    `Timestamp` DateTime64(9)
)
ENGINE = MergeTree
ORDER BY Timestamp;
"""


class TestSyncClickStackSchemas(unittest.TestCase):
    def test_extract_create_table_discards_goose_directives(self):
        self.assertEqual(
            sync.extract_create_table(UPSTREAM_SCHEMA, "otel_logs"),
            """\
CREATE TABLE IF NOT EXISTS ${DATABASE}.otel_logs
(
    `Timestamp` DateTime64(9)
)
ENGINE = MergeTree
ORDER BY Timestamp;""",
        )


    def test_extract_create_table_requires_the_named_schema(self):
        with self.assertRaisesRegex(RuntimeError, 'id="otel_logs"'):
            sync.extract_create_table(
                "CREATE TABLE ${DATABASE}.other (x UInt8);", "otel_logs"
            )


    def test_sync_file_rewrites_an_mdx_marker_and_caches_the_source(self):
        with tempfile.TemporaryDirectory() as directory:
            snippet = Path(directory) / "_schema_otel_logs.mdx"
            snippet.write_text(
                "before\n"
                "{/* sync-clickstack-schema: id=otel_logs source=https://example.test/schema.sql */}\n"
                "```sql\nCREATE TABLE old;\n```\n"
                "after\n",
                encoding="utf-8",
            )
            calls = []

            def fetch(source):
                calls.append(source)
                return UPSTREAM_SCHEMA

            with mock.patch.object(sync, "fetch_text", fetch):
                cache = {}
                self.assertTrue(sync.sync_file(snippet, cache))
                self.assertFalse(sync.sync_file(snippet, cache))

            self.assertEqual(calls, ["https://example.test/schema.sql"])
            self.assertEqual(
                snippet.read_text(encoding="utf-8"),
                """\
before
{/* sync-clickstack-schema: id=otel_logs source=https://example.test/schema.sql */}
```sql
CREATE TABLE IF NOT EXISTS ${DATABASE}.otel_logs
(
    `Timestamp` DateTime64(9)
)
ENGINE = MergeTree
ORDER BY Timestamp;
```
after
""",
            )


if __name__ == "__main__":
    unittest.main()
