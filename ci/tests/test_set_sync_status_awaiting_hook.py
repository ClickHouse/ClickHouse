import unittest
from unittest.mock import patch

from ci.jobs.scripts.job_hooks import set_sync_status_awaiting_hook as hook
from ci.praktika.result import Result


class FakeInfo:
    repo_name = "ClickHouse/ClickHouse"

    def __init__(self, changed_files):
        self.changed_files = changed_files

    def get_changed_files(self):
        return self.changed_files


class TestCanSkipSync(unittest.TestCase):
    def test_docs_only_changes(self):
        cases = [
            ["docs/guides/example.mdx"],
            ["docs/images/example.png", "docs/guides/example.mdx"],
            ["./docs/guides/example.mdx"],
            ["/docs/guides/example.mdx"],
        ]
        for changed_files in cases:
            with self.subTest(changed_files=changed_files):
                self.assertTrue(hook.can_skip_sync(changed_files))

    def test_other_changes(self):
        cases = [
            None,
            [],
            ["README.md"],
            ["src/example.cpp"],
            ["docs/guides/example.mdx", "src/example.cpp"],
            ["docs/changelogs/v26.8.md"],
            ["docs/private-changelogs/v26.8.md"],
            ["docs/old-page.mdx", "src/new-file.cpp"],
        ]
        for changed_files in cases:
            with self.subTest(changed_files=changed_files):
                self.assertFalse(hook.can_skip_sync(changed_files))


class TestSetSyncStatusAwaitingHook(unittest.TestCase):
    def run_hook(self, changed_files, statuses=None):
        posted = []
        with (
            patch.object(hook, "Info", return_value=FakeInfo(changed_files)),
            patch.object(hook.GH, "get_commit_statuses", return_value=statuses or {}),
            patch.object(
                hook.GH,
                "post_commit_status",
                side_effect=lambda **kwargs: posted.append(kwargs),
            ),
        ):
            hook.main()
        return posted

    def test_docs_only_change_is_completed_without_requesting_sync(self):
        self.assertEqual(
            self.run_hook(["docs/guides/example.mdx"]),
            [
                {
                    "name": hook.SYNC,
                    "status": Result.Status.OK,
                    "description": hook.DOCS_ONLY_SYNC_STATUS_DESCRIPTION,
                    "url": "",
                }
            ],
        )

    def test_uncertain_or_non_docs_change_requests_sync(self):
        for changed_files in (None, [], ["src/example.cpp"]):
            with self.subTest(changed_files=changed_files):
                self.assertEqual(
                    self.run_hook(changed_files),
                    [
                        {
                            "name": hook.SYNC,
                            "status": Result.Status.PENDING,
                            "description": "awaiting",
                            "url": "",
                        }
                    ],
                )

    def test_existing_sync_status_is_not_replaced(self):
        existing_status = hook.GH.CommitStatus(
            state="pending", description="tests started", url="", context=hook.SYNC
        )
        self.assertEqual(
            self.run_hook(
                ["docs/guides/example.mdx"],
                statuses={hook.SYNC: existing_status},
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
