#!/usr/bin/env python

import unittest

import docker_server as ds
from version_helper import get_version_from_string


class TestDockerServer(unittest.TestCase):
    def test_gen_tags(self):
        version = get_version_from_string("22.2.2.2")
        cases = (
            ("release-latest", ["latest", "22", "22.2", "22.2.2", "22.2.2.2"]),
            ("release", ["22", "22.2", "22.2.2", "22.2.2.2"]),
            ("head", ["head"]),
        )
        for case in cases:
            release_type = case[0]
            self.assertEqual(case[1], ds.gen_tags(version, release_type))
