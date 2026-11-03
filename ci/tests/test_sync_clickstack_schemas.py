import os
import subprocess
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
    @staticmethod
    def run_git(*arguments: str, cwd: Path) -> str:
        return subprocess.run(
            ["git", *arguments],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

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

    def test_extract_create_table_ignores_semicolons_in_quotes_and_comments(self):
        sql = """\
CREATE TABLE IF NOT EXISTS ${DATABASE}.otel_logs
(
    Body String DEFAULT ';', -- semicolon ; in a line comment
    Message String DEFAULT 'it''s; still a string'
)
/* semicolon ; in a block comment */
ENGINE = MergeTree
ORDER BY tuple();
CREATE TABLE ${DATABASE}.other (x UInt8);
"""
        self.assertEqual(
            sync.extract_create_table(sql, "otel_logs"),
            """\
CREATE TABLE IF NOT EXISTS ${DATABASE}.otel_logs
(
    Body String DEFAULT ';', -- semicolon ; in a line comment
    Message String DEFAULT 'it''s; still a string'
)
/* semicolon ; in a block comment */
ENGINE = MergeTree
ORDER BY tuple();""",
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

    def test_bot_branch_merges_cleanly_after_shallow_checkout_is_deepened(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = root / "remote.git"
            source = root / "source"
            checkout = root / "checkout"
            snippet = "docs/snippets/clickstack/_schema_otel_logs.mdx"

            self.run_git("init", "--bare", remote, cwd=root)
            self.run_git("init", "--initial-branch=master", source, cwd=root)
            self.run_git("config", "user.name", "Test User", cwd=source)
            self.run_git("config", "user.email", "test@example.com", cwd=source)
            (source / snippet).parent.mkdir(parents=True)
            (source / snippet).write_text("schema A\n", encoding="utf-8")
            (source / "unrelated").write_text("before\n", encoding="utf-8")
            self.run_git("add", snippet, "unrelated", cwd=source)
            self.run_git("commit", "-m", "base", cwd=source)
            self.run_git("remote", "add", "origin", remote, cwd=source)
            self.run_git("push", "origin", "master", cwd=source)

            self.run_git("checkout", "-b", job.BOT_BRANCH, cwd=source)
            (source / snippet).write_text("schema B\n", encoding="utf-8")
            self.run_git("add", snippet, cwd=source)
            self.run_git("commit", "-m", "generated schema", cwd=source)
            self.run_git("push", "origin", job.BOT_BRANCH, cwd=source)

            self.run_git("checkout", "master", cwd=source)
            (source / "unrelated").write_text("after\n", encoding="utf-8")
            self.run_git("add", "unrelated", cwd=source)
            self.run_git("commit", "-m", "advance master", cwd=source)
            self.run_git("push", "origin", "master", cwd=source)

            self.run_git(
                "clone", "--depth=1", "--branch=master", remote.as_uri(), checkout, cwd=root
            )
            self.assertEqual(
                self.run_git("rev-parse", "--is-shallow-repository", cwd=checkout), "true"
            )

            original_directory = Path.cwd()
            try:
                os.chdir(checkout)
                with mock.patch.object(job, "Info") as info:
                    info.return_value.git_branch = job.BASE_BRANCH
                    self.assertTrue(job.checkout_current_base_branch())
                (checkout / snippet).write_text("schema B\n", encoding="utf-8")
                self.assertTrue(job.bot_branch_matches_generated_snippets())
                self.assertTrue(job.bot_branch_merges_cleanly())
            finally:
                os.chdir(original_directory)

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
    @mock.patch.object(job, "Info")
    def test_publish_if_needed_closes_stale_bot_pull_request(
        self, info, has_changes, open_pr, close_pr
    ):
        info.return_value.repo_name = job.REPOSITORY

        self.assertTrue(job.publish_if_needed())

        has_changes.assert_called_once_with()
        open_pr.assert_called_once_with()
        close_pr.assert_called_once_with("42")

    @mock.patch.object(job, "Info")
    @mock.patch.object(job, "open_bot_pull_request")
    @mock.patch.object(job.Shell, "get_output", side_effect=RuntimeError("git status failed"))
    def test_publish_if_needed_propagates_git_status_failure(self, get_output, open_pr, info):
        info.return_value.repo_name = job.REPOSITORY

        with self.assertRaisesRegex(RuntimeError, "git status failed"):
            job.publish_if_needed()

        get_output.assert_called_once_with(
            f"git status --porcelain -- {job.SNIPPETS_PATH}", strict=True
        )
        open_pr.assert_not_called()

    @mock.patch.object(job, "close_stale_bot_pull_request")
    @mock.patch.object(job, "open_bot_pull_request")
    @mock.patch.object(job, "has_changes")
    @mock.patch.object(job, "Info")
    def test_publish_if_needed_rejects_other_repositories(
        self, info, has_changes, open_pr, close_pr
    ):
        info.return_value.repo_name = "ClickHouse/clickhouse-private"

        self.assertFalse(job.publish_if_needed())

        has_changes.assert_not_called()
        open_pr.assert_not_called()
        close_pr.assert_not_called()

    @mock.patch.object(job, "Info")
    @mock.patch.object(job, "publish_sync", return_value=True)
    @mock.patch.object(job, "bot_branch_merges_cleanly", return_value=False)
    @mock.patch.object(job, "bot_branch_matches_generated_snippets", return_value=True)
    @mock.patch.object(job, "open_bot_pull_request", return_value="42")
    @mock.patch.object(job, "has_changes", return_value=True)
    def test_publish_if_needed_refreshes_conflicting_bot_branch(
        self, has_changes, open_pr, snippets_match, merges_cleanly, publish, info
    ):
        info.return_value.repo_name = job.REPOSITORY

        self.assertTrue(job.publish_if_needed())

        snippets_match.assert_called_once_with()
        merges_cleanly.assert_called_once_with()
        publish.assert_called_once_with("42")


if __name__ == "__main__":
    unittest.main()
