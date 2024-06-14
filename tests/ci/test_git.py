#!/usr/bin/env python

import os.path as p
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from git_helper import CWD, Git, Runner, git_runner


class TestRunner(unittest.TestCase):
    def test_init(self):
        runner = Runner()
        self.assertEqual(runner.cwd, p.realpath(p.dirname(__file__)))
        runner = Runner("/")
        self.assertEqual(runner.cwd, "/")

    def test_run(self):
        runner = Runner()
        output = runner.run("echo 1")
        self.assertEqual(output, "1")

    def test_one_time_writeable_cwd(self):
        runner = Runner()
        self.assertEqual(runner.cwd, CWD)
        runner.cwd = "/bin"
        self.assertEqual(runner.cwd, "/bin")
        runner.cwd = "/"
        self.assertEqual(runner.cwd, "/bin")
        runner = Runner("/")
        self.assertEqual(runner.cwd, "/")
        runner.cwd = "/bin"
        self.assertEqual(runner.cwd, "/")


class TestGit(unittest.TestCase):
    def setUp(self):
        """we use dummy git object"""
        # get the git_runner's cwd to set it properly before the Runner is patched
        _ = git_runner.cwd
        run_patcher = patch("git_helper.Runner.run", return_value="")
        run_mock = run_patcher.start()
        self.addCleanup(run_patcher.stop)
        update_patcher = patch("git_helper.Git.update")
        update_mock = update_patcher.start()
        self.addCleanup(update_patcher.stop)
        self.git = Git()
        update_mock.assert_called_once()
        self.git.run("test")
        run_mock.assert_called_once()
        self.git.branch = "old_branch"
        self.git.sha = ""
        self.git.sha_short = ""
        self.git.latest_tag = ""
        self.git.commits_since_latest = 0
        self.git.commits_since_new = 0

    def test_tags(self):
        self.git.new_tag = "v21.12.333.22222-stable"
        self.git.latest_tag = "v21.12.333.22222-stable"
        for tag_attr in ("new_tag", "latest_tag"):
            self.assertEqual(getattr(self.git, tag_attr), "v21.12.333.22222-stable")
            setattr(self.git, tag_attr, "")
            self.assertEqual(getattr(self.git, tag_attr), "")
            for tag in (
                "v21.12.333-stable",
                "v21.12.333-prestable",
                "21.12.333.22222-stable",
                "v21.12.333.22222-production",
            ):
                with self.assertRaises(Exception):
                    setattr(self.git, tag_attr, tag)

    def test_tweak(self):
        # tweak for the latest tag
        @dataclass
        class TestCase:
            tag: str
            commits: int
            tweak: int

        cases = (
            TestCase("", 0, 1),
            TestCase("", 2, 2),
            TestCase("v21.12.333.22222-stable", 0, 22222),
            TestCase("v21.12.333.22222-stable", 2, 2),
            TestCase("v21.12.333.22222-testing", 0, 22222),
            TestCase("v21.12.333.22222-testing", 2, 22224),
        )
        for tag, commits, tweak in (
            ("latest_tag", "commits_since_latest", "tweak"),
            ("new_tag", "commits_since_new", "tweak_to_new"),
        ):
            for tc in cases:
                setattr(self.git, tag, tc.tag)
                setattr(self.git, commits, tc.commits)
                self.assertEqual(
                    getattr(self.git, tweak),
                    tc.tweak,
                    f"Wrong tweak for tag {tc.tag} and commits {tc.commits} of {tag}",
                )
