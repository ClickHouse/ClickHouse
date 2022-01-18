#!/usr/bin/env python

import os
import unittest
from unittest.mock import patch

from pr_info import PRInfo
import docker_images_check as di

# di.logging.basicConfig(level=di.logging.INFO)


class TestDockerImageCheck(unittest.TestCase):
    docker_images_path = os.path.join(
        os.path.dirname(__file__), "tests/docker_images.json"
    )

    def test_get_changed_docker_images(self):
        pr_info = PRInfo(PRInfo.default_event.copy())
        pr_info.changed_files = {
            "docker/test/stateless",
            "docker/test/base",
            "docker/docs/builder",
        }
        images = di.get_changed_docker_images(pr_info, "/", self.docker_images_path)
        expected = {
            di.DockerImage("docker/test/base", "clickhouse/test-base"),
            di.DockerImage("docker/docs/builder", "clickhouse/docs-builder"),
            di.DockerImage("docker/test/stateless", "clickhouse/stateless-test"),
            di.DockerImage(
                "docker/test/integration/base", "clickhouse/integration-test"
            ),
            di.DockerImage("docker/test/fuzzer", "clickhouse/fuzzer"),
            di.DockerImage(
                "docker/test/keeper-jepsen", "clickhouse/keeper-jepsen-test"
            ),
            di.DockerImage("docker/docs/check", "clickhouse/docs-check"),
            di.DockerImage("docker/docs/release", "clickhouse/docs-release"),
            di.DockerImage("docker/test/stateful", "clickhouse/stateful-test"),
            di.DockerImage("docker/test/unit", "clickhouse/unit-test"),
            di.DockerImage("docker/test/stress", "clickhouse/stress-test"),
        }
        self.assertEqual(images, expected)

    def test_gen_version(self):
        pr_info = PRInfo(PRInfo.default_event.copy())
        versions, result_version = di.gen_versions(pr_info, None)
        self.assertEqual(versions, ["latest", "0", "0-HEAD"])
        self.assertEqual(result_version, "0-HEAD")
        versions, result_version = di.gen_versions(pr_info, "suffix")
        self.assertEqual(versions, ["latest-suffix", "0-suffix", "0-HEAD-suffix"])
        self.assertEqual(result_version, versions)
        pr_info.number = 1
        versions, result_version = di.gen_versions(pr_info, None)
        self.assertEqual(versions, ["1", "1-HEAD"])
        self.assertEqual(result_version, "1-HEAD")

    @patch("builtins.open")
    @patch("subprocess.Popen")
    def test_build_and_push_one_image(self, mock_popen, mock_open):
        mock_popen.return_value.__enter__.return_value.wait.return_value = 0
        image = di.DockerImage("path", "name", gh_repo_path="")

        result, _ = di.build_and_push_one_image(image, "version", True, True)
        mock_open.assert_called_once()
        mock_popen.assert_called_once()
        self.assertIn(
            "docker buildx build --builder default --build-arg FROM_TAG=version "
            "--build-arg BUILDKIT_INLINE_CACHE=1 --tag name:version --cache-from "
            "type=registry,ref=name:version --push --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertTrue(result)

        mock_open.reset()
        mock_popen.reset()
        mock_popen.return_value.__enter__.return_value.wait.return_value = 0
        result, _ = di.build_and_push_one_image(image, "version2", False, True)
        self.assertIn(
            "docker buildx build --builder default --build-arg FROM_TAG=version2 "
            "--build-arg BUILDKIT_INLINE_CACHE=1 --tag name:version2 --cache-from "
            "type=registry,ref=name:version2 --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertTrue(result)

        mock_popen.return_value.__enter__.return_value.wait.return_value = 1
        result, _ = di.build_and_push_one_image(image, "version2", False, False)
        self.assertIn(
            "docker buildx build --builder default "
            "--build-arg BUILDKIT_INLINE_CACHE=1 --tag name:version2 --cache-from "
            "type=registry,ref=name:version2 --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
