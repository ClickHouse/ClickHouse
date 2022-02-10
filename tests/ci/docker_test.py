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
        images = sorted(
            list(
                di.get_changed_docker_images(
                    pr_info, di.get_images_dict("/", self.docker_images_path)
                )
            )
        )
        self.maxDiff = None
        expected = sorted(
            [
                di.DockerImage("docker/test/base", "clickhouse/test-base"),
                di.DockerImage("docker/docs/builder", "clickhouse/docs-builder"),
                di.DockerImage(
                    "docker/test/stateless",
                    "clickhouse/stateless-test",
                    "clickhouse/test-base",
                ),
                di.DockerImage(
                    "docker/test/integration/base",
                    "clickhouse/integration-test",
                    "clickhouse/test-base",
                ),
                di.DockerImage(
                    "docker/test/fuzzer", "clickhouse/fuzzer", "clickhouse/test-base"
                ),
                di.DockerImage(
                    "docker/test/keeper-jepsen",
                    "clickhouse/keeper-jepsen-test",
                    "clickhouse/test-base",
                ),
                di.DockerImage(
                    "docker/docs/check",
                    "clickhouse/docs-check",
                    "clickhouse/docs-builder",
                ),
                di.DockerImage(
                    "docker/docs/release",
                    "clickhouse/docs-release",
                    "clickhouse/docs-builder",
                ),
                di.DockerImage(
                    "docker/test/stateful",
                    "clickhouse/stateful-test",
                    "clickhouse/stateless-test",
                ),
                di.DockerImage(
                    "docker/test/unit",
                    "clickhouse/unit-test",
                    "clickhouse/stateless-test",
                ),
                di.DockerImage(
                    "docker/test/stress",
                    "clickhouse/stress-test",
                    "clickhouse/stateful-test",
                ),
            ]
        )
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

    @patch("docker_images_check.build_and_push_one_image")
    def test_process_image_with_parents(self, mock_build):
        mock_build.side_effect = lambda w, x, y, z: (True, f"{w.repo}_{x}.log")
        im1 = di.DockerImage("path1", "repo1")
        im2 = di.DockerImage("path2", "repo2", im1)
        im3 = di.DockerImage("path3", "repo3", im2)
        im4 = di.DockerImage("path4", "repo4", im1)
        # We use list to have determined order of image builgings
        images = [im4, im1, im3, im2, im1]
        results = [
            di.process_image_with_parents(im, ["v1", "v2", "latest"], True)
            for im in images
        ]

        expected = [
            [  # repo4 -> repo1
                ("repo1:v1", "repo1_v1.log", "OK"),
                ("repo1:v2", "repo1_v2.log", "OK"),
                ("repo1:latest", "repo1_latest.log", "OK"),
                ("repo4:v1", "repo4_v1.log", "OK"),
                ("repo4:v2", "repo4_v2.log", "OK"),
                ("repo4:latest", "repo4_latest.log", "OK"),
            ],
            [],  # repo1 is built
            [  # repo3 -> repo2 -> repo1
                ("repo2:v1", "repo2_v1.log", "OK"),
                ("repo2:v2", "repo2_v2.log", "OK"),
                ("repo2:latest", "repo2_latest.log", "OK"),
                ("repo3:v1", "repo3_v1.log", "OK"),
                ("repo3:v2", "repo3_v2.log", "OK"),
                ("repo3:latest", "repo3_latest.log", "OK"),
            ],
            [],  # repo2 -> repo1 are built
            [],  # repo1 is built
        ]
        self.assertEqual(results, expected)


if __name__ == "__main__":
    unittest.main()
