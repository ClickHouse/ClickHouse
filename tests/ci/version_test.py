#!/usr/bin/env python

import unittest
from argparse import ArgumentTypeError

import version_helper as vh


class TestFunctions(unittest.TestCase):
    def test_version_arg(self):
        cases = (
            ("0.0.0.0", vh.get_version_from_string("0.0.0.0")),
            ("1.1.1.2", vh.get_version_from_string("1.1.1.2")),
            ("v11.1.1.2-lts", vh.get_version_from_string("11.1.1.2")),
            ("v01.1.1.2-prestable", vh.get_version_from_string("1.1.1.2")),
            ("v21.1.1.2-stable", vh.get_version_from_string("21.1.1.2")),
            ("v31.1.1.2-testing", vh.get_version_from_string("31.1.1.2")),
            ("refs/tags/v31.1.1.2-testing", vh.get_version_from_string("31.1.1.2")),
        )
        for test_case in cases:
            version = vh.version_arg(test_case[0])
            self.assertEqual(test_case[1], version)
        error_cases = (
            "0.0.0",
            "1.1.1.a",
            "1.1.1.1.1",
            "1.1.1.2-testing",
            "v1.1.1.2-testing",
            "v1.1.1.2-testin",
            "refs/tags/v1.1.1.2-testin",
        )
        for error_case in error_cases:
            with self.assertRaises(ArgumentTypeError):
                version = vh.version_arg(error_case[0])
