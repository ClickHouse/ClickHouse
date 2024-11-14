#!/usr/bin/env python3

import unittest
from ci_config import CIStages, JobNames, CI_CONFIG, Runners


class TestCIConfig(unittest.TestCase):
    def test_runner_config(self):
        """check runner is provided w/o exception"""
        for job in JobNames:
            runner = CI_CONFIG.get_runner_type(job)
            self.assertIn(runner, Runners)

    def test_job_stage_config(self):
        """check runner is provided w/o exception"""
        for job in JobNames:
            stage = CI_CONFIG.get_job_ci_stage(job)
            if job in [
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                JobNames.JEPSEN_KEEPER,
                JobNames.BUILD_CHECK,
                JobNames.BUILD_CHECK_SPECIAL,
            ]:
                assert (
                    stage == CIStages.NA
                ), "These jobs are not in CI stages, must be NA"
            else:
                assert stage != CIStages.NA, f"stage not found for [{job}]"
            self.assertIn(stage, CIStages)
