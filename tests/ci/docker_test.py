#!/usr/bin/env python

import os
import unittest

from pr_info import PRInfo
import docker_images_check as di


class TestDockerImageCheck(unittest.TestCase):
    docker_images_path = os.path.join(
        os.path.dirname(__file__), "tests/docker_images.json"
    )

    def test_get_changed_docker_images(self):
        pr_info = PRInfo()
        pr_info.changed_files = {
            "docker/test/stateless",
            "docker/test/base",
            "docker/docs/builder",
        }
        images = di.get_changed_docker_images(pr_info, "/", self.docker_images_path)
        expected = [
            ("docker/test/base", "clickhouse/test-base"),
            ("docker/docs/builder", "clickhouse/docs-builder"),
            ("docker/test/stateless", "clickhouse/stateless-test"),
            ("docker/test/integration/base", "clickhouse/integration-test"),
            ("docker/test/fuzzer", "clickhouse/fuzzer"),
            ("docker/test/keeper-jepsen", "clickhouse/keeper-jepsen-test"),
            ("docker/docs/check", "clickhouse/docs-check"),
            ("docker/docs/release", "clickhouse/docs-release"),
            ("docker/test/stateful", "clickhouse/stateful-test"),
            ("docker/test/unit", "clickhouse/unit-test"),
            ("docker/test/stress", "clickhouse/stress-test"),
        ]
        self.assertEqual(images, expected)


if __name__ == "__main__":
    unittest.main()
