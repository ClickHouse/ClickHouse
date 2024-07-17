#!/usr/bin/env python

import unittest
from unittest.mock import patch, MagicMock

from version_helper import get_version_from_string
import docker_server as ds

# di.logging.basicConfig(level=di.logging.INFO)


class TestDockerServer(unittest.TestCase):
    def test_gen_tags(self):
        version = get_version_from_string("22.2.2.2")
        cases = (
            ("latest", ["latest", "22", "22.2", "22.2.2", "22.2.2.2"]),
            ("major", ["22", "22.2", "22.2.2", "22.2.2.2"]),
            ("minor", ["22.2", "22.2.2", "22.2.2.2"]),
            ("patch", ["22.2.2", "22.2.2.2"]),
            ("head", ["head"]),
        )
        for case in cases:
            release_type = case[0]
            self.assertEqual(case[1], ds.gen_tags(version, release_type))

        with self.assertRaises(ValueError):
            ds.gen_tags(version, "auto")

    @patch("docker_server.get_tagged_versions")
    def test_auto_release_type(self, mock_tagged_versions: MagicMock) -> None:
        mock_tagged_versions.return_value = [
            get_version_from_string("1.1.1.1"),
            get_version_from_string("1.2.1.1"),
            get_version_from_string("2.1.1.1"),
            get_version_from_string("2.2.1.1"),
            get_version_from_string("2.2.2.1"),
        ]

        cases_less = (
            (get_version_from_string("1.0.1.1"), "minor"),
            (get_version_from_string("1.1.2.1"), "minor"),
            (get_version_from_string("1.3.1.1"), "major"),
            (get_version_from_string("2.1.2.1"), "minor"),
            (get_version_from_string("2.2.1.3"), "patch"),
            (get_version_from_string("2.2.3.1"), "latest"),
            (get_version_from_string("2.3.1.1"), "latest"),
        )
        for case in cases_less:
            release = ds.auto_release_type(case[0], "auto")
            self.assertEqual(case[1], release)

        cases_equal = (
            (get_version_from_string("1.1.1.1"), "minor"),
            (get_version_from_string("1.2.1.1"), "major"),
            (get_version_from_string("2.1.1.1"), "minor"),
            (get_version_from_string("2.2.1.1"), "patch"),
            (get_version_from_string("2.2.2.1"), "latest"),
        )
        for case in cases_equal:
            release = ds.auto_release_type(case[0], "auto")
            self.assertEqual(case[1], release)
