#!/usr/bin/env python

import unittest
from argparse import ArgumentTypeError

import version_helper as vh


class TestFunctions(unittest.TestCase):
    def test_version_arg(self):
        cases = (
            ("0.0.0.0", vh.get_version_from_string("0.0.0.0")),
            ("1.1.1.2", vh.get_version_from_string("1.1.1.2")),
            ("v1.1.1.2-lts", vh.get_version_from_string("1.1.1.2")),
            ("v1.1.1.2-prestable", vh.get_version_from_string("1.1.1.2")),
            ("v1.1.1.2-stable", vh.get_version_from_string("1.1.1.2")),
            ("v1.1.1.2-testing", vh.get_version_from_string("1.1.1.2")),
            ("refs/tags/v1.1.1.2-testing", vh.get_version_from_string("1.1.1.2")),
        )
        for case in cases:
            version = vh.version_arg(case[0])
            self.assertEqual(case[1], version)
        error_cases = (
            "0.0.0",
            "1.1.1.a",
            "1.1.1.1.1",
            "1.1.1.2-testing",
            "v1.1.1.2-testin",
            "refs/tags/v1.1.1.2-testin",
        )
        for case in error_cases:
            with self.assertRaises(ArgumentTypeError):
                version = vh.version_arg(case[0])
