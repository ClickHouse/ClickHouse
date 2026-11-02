import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` imports `praktika` from the `ci/` directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs import sync_clickstack_schemas as job
from ci.jobs.scripts.docs import sync_clickstack_schemas as sync
from ci.praktika.parser import WorkflowConfigParser
from ci.praktika.yaml_generator import PullRequestPushYamlGen
from ci.workflows.sync_clickstack_schemas import workflow
from praktika import Workflow


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
    def test_workflow_generates_nightly_github_actions_schedule(self):
        self.assertEqual(workflow.engine, Workflow.Engine.GH_ACTIONS)
        self.assertEqual(workflow.event, Workflow.Event.SCHEDULE)

        generated = PullRequestPushYamlGen(WorkflowConfigParser(workflow).parse()).generate()

        self.assertIn("name: SyncClickStackSchemas", generated)
        self.assertIn("  schedule:\n    - cron: 17 8 * * *", generated)

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

    @mock.patch.object(job, "Info")
    @mock.patch.object(job.Git, "push", return_value=True)
    @mock.patch.object(job.Shell, "check", return_value=True)
    def test_publish_sync_uses_workflow_repository_identity(self, check, push, info):
        info.return_value.repo_name = job.REPOSITORY

        self.assertTrue(job.publish_sync(existing_pr=None))

        commands = [call.args[0] for call in check.call_args_list]
        self.assertTrue(any("gh pr create" in command for command in commands))
        push.assert_called_once_with(job.REPOSITORY, f"HEAD:refs/heads/{job.BOT_BRANCH}", force=True)

    @mock.patch.object(job, "close_stale_bot_pull_request", return_value=True)
    @mock.patch.object(job, "open_bot_pull_request", return_value="42")
    @mock.patch.object(job, "has_changes", return_value=False)
    def test_publish_if_needed_closes_stale_bot_pull_request(self, has_changes, open_pr, close_pr):
        self.assertTrue(job.publish_if_needed())

        has_changes.assert_called_once_with()
        open_pr.assert_called_once_with()
        close_pr.assert_called_once_with("42")


if __name__ == "__main__":
    unittest.main()
