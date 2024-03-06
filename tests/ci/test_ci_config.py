#!/usr/bin/env python3

import unittest
from ci_config import JobNames, CI_CONFIG, Runners


class TestCIConfig(unittest.TestCase):
    def test_runner_config(self):
        """check runner is provided w/o exception"""
        for job in JobNames:
            runner = CI_CONFIG.get_runner_type(job)
            self.assertIn(runner, Runners)
