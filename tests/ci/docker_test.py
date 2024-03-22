#!/usr/bin/env python

import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from env_helper import GITHUB_RUN_URL
from pr_info import PRInfo
from report import TestResult
import docker_images_check as di
from docker_images_helper import get_images_dict

from version_helper import get_version_from_string
import docker_server as ds

# di.logging.basicConfig(level=di.logging.INFO)


class TestDockerImageCheck(unittest.TestCase):
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
                    pr_info,
                    get_images_dict(
                        Path(__file__).parent,
                        Path("tests/docker_images_for_tests.json"),
                    ),
                )
            )
        )
        self.maxDiff = None
        expected = sorted(
            [
                di.DockerImage("docker/test/base", "clickhouse/test-base", False),
                di.DockerImage("docker/docs/builder", "clickhouse/docs-builder", True),
                di.DockerImage(
                    "docker/test/sqltest",
                    "clickhouse/sqltest",
                    False,
                    "clickhouse/test-base",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/stateless",
                    "clickhouse/stateless-test",
                    False,
                    "clickhouse/test-base",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/integration/base",
                    "clickhouse/integration-test",
                    False,
                    "clickhouse/test-base",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/fuzzer",
                    "clickhouse/fuzzer",
                    False,
                    "clickhouse/test-base",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/keeper-jepsen",
                    "clickhouse/keeper-jepsen-test",
                    False,
                    "clickhouse/test-base",  # type: ignore
                ),
                di.DockerImage(
                    "docker/docs/check",
                    "clickhouse/docs-check",
                    False,
                    "clickhouse/docs-builder",  # type: ignore
                ),
                di.DockerImage(
                    "docker/docs/release",
                    "clickhouse/docs-release",
                    False,
                    "clickhouse/docs-builder",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/stateful",
                    "clickhouse/stateful-test",
                    False,
                    "clickhouse/stateless-test",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/unit",
                    "clickhouse/unit-test",
                    False,
                    "clickhouse/stateless-test",  # type: ignore
                ),
                di.DockerImage(
                    "docker/test/stress",
                    "clickhouse/stress-test",
                    False,
                    "clickhouse/stateful-test",  # type: ignore
                ),
            ]
        )
        self.assertEqual(images, expected)

    def test_gen_version(self):
        pr_info = PRInfo(PRInfo.default_event.copy())
        pr_info.base_ref = "anything-else"
        versions, result_version = di.gen_versions(pr_info, None)
        self.assertEqual(versions, ["0", "0-HEAD"])
        self.assertEqual(result_version, "0-HEAD")
        pr_info.base_ref = "master"
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

    @patch("docker_images_check.TeePopen")
    @patch("platform.machine")
    def test_build_and_push_one_image(self, mock_machine, mock_popen):
        mock_popen.return_value.__enter__.return_value.wait.return_value = 0
        image = di.DockerImage("path", "name", False, gh_repo="")

        result, _ = di.build_and_push_one_image(image, "version", [], True, True)
        mock_popen.assert_called_once()
        mock_machine.assert_not_called()
        self.assertIn(
            f"docker buildx build --builder default --label build-url={GITHUB_RUN_URL} "
            "--build-arg FROM_TAG=version "
            f"--build-arg CACHE_INVALIDATOR={GITHUB_RUN_URL} "
            "--tag name:version --cache-from type=registry,ref=name:version "
            "--cache-from type=registry,ref=name:latest "
            "--cache-to type=inline,mode=max --push --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertTrue(result)
        mock_popen.reset_mock()
        mock_machine.reset_mock()

        mock_popen.return_value.__enter__.return_value.wait.return_value = 0
        result, _ = di.build_and_push_one_image(image, "version2", [], False, True)
        mock_popen.assert_called_once()
        mock_machine.assert_not_called()
        self.assertIn(
            f"docker buildx build --builder default --label build-url={GITHUB_RUN_URL} "
            "--build-arg FROM_TAG=version2 "
            f"--build-arg CACHE_INVALIDATOR={GITHUB_RUN_URL} "
            "--tag name:version2 --cache-from type=registry,ref=name:version2 "
            "--cache-from type=registry,ref=name:latest "
            "--cache-to type=inline,mode=max --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertTrue(result)

        mock_popen.reset_mock()
        mock_machine.reset_mock()
        mock_popen.return_value.__enter__.return_value.wait.return_value = 1
        result, _ = di.build_and_push_one_image(image, "version2", [], False, False)
        mock_popen.assert_called_once()
        mock_machine.assert_not_called()
        self.assertIn(
            f"docker buildx build --builder default --label build-url={GITHUB_RUN_URL} "
            f"--build-arg CACHE_INVALIDATOR={GITHUB_RUN_URL} "
            "--tag name:version2 --cache-from type=registry,ref=name:version2 "
            "--cache-from type=registry,ref=name:latest "
            "--cache-to type=inline,mode=max --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertFalse(result)

        mock_popen.reset_mock()
        mock_machine.reset_mock()
        mock_popen.return_value.__enter__.return_value.wait.return_value = 1
        result, _ = di.build_and_push_one_image(
            image, "version2", ["cached-version", "another-cached"], False, False
        )
        mock_popen.assert_called_once()
        mock_machine.assert_not_called()
        self.assertIn(
            f"docker buildx build --builder default --label build-url={GITHUB_RUN_URL} "
            f"--build-arg CACHE_INVALIDATOR={GITHUB_RUN_URL} "
            "--tag name:version2 --cache-from type=registry,ref=name:version2 "
            "--cache-from type=registry,ref=name:latest "
            "--cache-from type=registry,ref=name:cached-version "
            "--cache-from type=registry,ref=name:another-cached "
            "--cache-to type=inline,mode=max --progress plain path",
            mock_popen.call_args.args,
        )
        self.assertFalse(result)

        mock_popen.reset_mock()
        mock_machine.reset_mock()
        only_amd64_image = di.DockerImage("path", "name", True)
        mock_popen.return_value.__enter__.return_value.wait.return_value = 0

        result, _ = di.build_and_push_one_image(
            only_amd64_image, "version", [], True, True
        )
        mock_popen.assert_called_once()
        mock_machine.assert_called_once()
        self.assertIn(
            "docker pull ubuntu:20.04; docker tag ubuntu:20.04 name:version; "
            "docker push name:version",
            mock_popen.call_args.args,
        )
        self.assertTrue(result)
        result, _ = di.build_and_push_one_image(
            only_amd64_image, "version", [], False, True
        )
        self.assertIn(
            "docker pull ubuntu:20.04; docker tag ubuntu:20.04 name:version; ",
            mock_popen.call_args.args,
        )
        with self.assertRaises(AssertionError):
            result, _ = di.build_and_push_one_image(image, "version", [""], False, True)

    @patch("docker_images_check.build_and_push_one_image")
    def test_process_image_with_parents(self, mock_build):
        mock_build.side_effect = lambda v, w, x, y, z: (True, Path(f"{v.repo}_{w}.log"))
        im1 = di.DockerImage("path1", "repo1", False)
        im2 = di.DockerImage("path2", "repo2", False, im1)
        im3 = di.DockerImage("path3", "repo3", False, im2)
        im4 = di.DockerImage("path4", "repo4", False, im1)
        # We use list to have determined order of image builgings
        images = [im4, im1, im3, im2, im1]
        test_results = [
            di.process_image_with_parents(im, ["v1", "v2", "latest"], [], True)
            for im in images
        ]
        # The time is random, so we check it's not None and greater than 0,
        # and then set to 1
        for results in test_results:
            for result in results:
                self.assertIsNotNone(result.time)
                self.assertGreater(result.time, 0)  # type: ignore
                result.time = 1

        self.maxDiff = None
        expected = [
            [  # repo4 -> repo1
                TestResult("repo1:v1", "OK", 1, [Path("repo1_v1.log")]),
                TestResult("repo1:v2", "OK", 1, [Path("repo1_v2.log")]),
                TestResult("repo1:latest", "OK", 1, [Path("repo1_latest.log")]),
                TestResult("repo4:v1", "OK", 1, [Path("repo4_v1.log")]),
                TestResult("repo4:v2", "OK", 1, [Path("repo4_v2.log")]),
                TestResult("repo4:latest", "OK", 1, [Path("repo4_latest.log")]),
            ],
            [],  # repo1 is built
            [  # repo3 -> repo2 -> repo1
                TestResult("repo2:v1", "OK", 1, [Path("repo2_v1.log")]),
                TestResult("repo2:v2", "OK", 1, [Path("repo2_v2.log")]),
                TestResult("repo2:latest", "OK", 1, [Path("repo2_latest.log")]),
                TestResult("repo3:v1", "OK", 1, [Path("repo3_v1.log")]),
                TestResult("repo3:v2", "OK", 1, [Path("repo3_v2.log")]),
                TestResult("repo3:latest", "OK", 1, [Path("repo3_latest.log")]),
            ],
            [],  # repo2 -> repo1 are built
            [],  # repo1 is built
        ]
        self.assertEqual(test_results, expected)


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
