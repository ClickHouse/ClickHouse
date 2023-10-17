#!/usr/bin/env python

from unittest.mock import patch
import os.path as p
import unittest

from git_helper import Git, Runner, CWD


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
        run_patcher = patch("git_helper.Runner.run", return_value="")
        self.run_mock = run_patcher.start()
        self.addCleanup(run_patcher.stop)
        update_patcher = patch("git_helper.Git.update")
        update_mock = update_patcher.start()
        self.addCleanup(update_patcher.stop)
        self.git = Git()
        update_mock.assert_called_once()
        self.git.run("test")
        self.run_mock.assert_called_once()
        self.git.new_branch = "NEW_BRANCH_NAME"
        self.git.new_tag = "v21.12.333.22222-stable"
        self.git.branch = "old_branch"
        self.git.sha = ""
        self.git.sha_short = ""
        self.git.latest_tag = ""
        self.git.description = ""
        self.git.commits_since_tag = 0

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
        self.git.commits_since_tag = 0
        self.assertEqual(self.git.tweak, 1)
        self.git.commits_since_tag = 2
        self.assertEqual(self.git.tweak, 2)
        self.git.latest_tag = "v21.12.333.22222-testing"
        self.assertEqual(self.git.tweak, 22224)
        self.git.commits_since_tag = 0
        self.assertEqual(self.git.tweak, 22222)
