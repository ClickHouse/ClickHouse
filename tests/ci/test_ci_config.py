#!/usr/bin/env python3

import unittest


class TestCiConfig(unittest.TestCase):
    def test_no_errors_in_ci_config(self):
        raised = None
        try:
            from ci_config import (  # pylint: disable=import-outside-toplevel
                CI_CONFIG as _,
            )
        except Exception as exc:
            raised = exc
        self.assertIsNone(raised, f"CI_CONFIG import raised error {raised}")
