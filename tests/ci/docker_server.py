#!/usr/bin/env python

# here
import argparse
import json
import logging
import subprocess
import time
from os import path as p, makedirs
from typing import List, Tuple

from github import Github

from build_check import get_release_or_pr
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import post_commit_status
from docker_images_check import DockerImage
from env_helper import CI, GITHUB_RUN_URL, RUNNER_TEMP, S3_BUILDS_BUCKET
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from git_helper import Git
from pr_info import PRInfo
from s3_helper import S3Helper
from stopwatch import Stopwatch
from upload_result_helper import upload_results
from version_helper import (
    ClickHouseVersion,
    get_tagged_versions,
    get_version_from_repo,
    version_arg,
)

TEMP_PATH = p.join(RUNNER_TEMP, "docker_images_check")
BUCKETS = {"amd64": "package_release", "arm64": "package_aarch64"}
git = Git(ignore_no_tags=True)


class DelOS(argparse.Action):
    def __call__(self, _, namespace, __, option_string=None):
        no_build = self.dest[3:] if self.dest.startswith("no_") else self.dest
        if no_build in namespace.os:
            namespace.os.remove(no_build)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="A program to build clickhouse-server image, both alpine and "
        "ubuntu versions",
    )

    parser.add_argument(
        "--version",
        type=version_arg,
        default=get_version_from_repo(git=git).string,
        help="a version to build, automaticaly got from version_helper, accepts either "
        "tag ('refs/tags/' is removed automatically) or a normal 22.2.2.2 format",
    )
    parser.add_argument(
        "--release-type",
        type=str,
        choices=("auto", "latest", "major", "minor", "patch", "head"),
        default="head",
        help="version part that will be updated when '--version' is set; "
        "'auto' is a special case, it will get versions from github and detect the "
        "release type (latest, major, minor or patch) automatically",
    )
    parser.add_argument(
        "--image-path",
        type=str,
        default="docker/server",
        help="a path to docker context directory",
    )
    parser.add_argument(
        "--image-repo",
        type=str,
        default="clickhouse/clickhouse-server",
        help="image name on docker hub",
    )
    parser.add_argument(
        "--bucket-prefix",
        help="if set, then is used as source for deb and tgz files",
    )
    parser.add_argument("--reports", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-reports",
        action="store_false",
        dest="reports",
        default=argparse.SUPPRESS,
        help="don't push reports to S3 and github",
    )
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push-images",
        action="store_false",
        dest="push",
        default=argparse.SUPPRESS,
        help="don't push images to docker hub",
    )
    parser.add_argument("--os", default=["ubuntu", "alpine"], help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-ubuntu",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build ubuntu image",
    )
    parser.add_argument(
        "--no-alpine",
        action=DelOS,
        nargs=0,
        default=argparse.SUPPRESS,
        help="don't build alpine image",
    )

    return parser.parse_args()


def retry_popen(cmd: str) -> int:
    max_retries = 5
    for retry in range(max_retries):
        # From time to time docker build may failed. Curl issues, or even push
        # It will sleep progressively 5, 15, 30 and 50 seconds between retries
        progressive_sleep = 5 * sum(i + 1 for i in range(retry))
        if progressive_sleep:
            logging.warning(
                "The following command failed, sleep %s before retry: %s",
                progressive_sleep,
                cmd,
            )
            time.sleep(progressive_sleep)
        with subprocess.Popen(
            cmd,
            shell=True,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        ) as process:
            for line in process.stdout:  # type: ignore
                print(line, end="")
            retcode = process.wait()
            if retcode == 0:
                return 0
    return retcode


def auto_release_type(version: ClickHouseVersion, release_type: str) -> str:
    if release_type != "auto":
        return release_type

    git_versions = get_tagged_versions()
    reference_version = git_versions[0]
    for i in reversed(range(len(git_versions))):
        if git_versions[i] <= version:
            if i == len(git_versions) - 1:
                return "latest"
            reference_version = git_versions[i + 1]
            break

    if version.major < reference_version.major:
        return "major"
    if version.minor < reference_version.minor:
        return "minor"
    if version.patch < reference_version.patch:
        return "patch"

    raise ValueError(
        "Release type 'tweak' is not supported for "
        f"{version.string} < {reference_version.string}"
    )


def gen_tags(version: ClickHouseVersion, release_type: str) -> List[str]:
    """
    22.2.2.2 + latest:
    - latest
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    22.2.2.2 + major:
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    22.2.2.2 + minor:
    - 22.2
    - 22.2.2
    - 22.2.2.2
    22.2.2.2 + patch:
    - 22.2.2
    - 22.2.2.2
    22.2.2.2 + head:
    - head
    """
    parts = version.string.split(".")
    tags = []
    if release_type == "latest":
        tags.append(release_type)
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif release_type == "major":
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif release_type == "minor":
        for i in range(1, len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif release_type == "patch":
        for i in range(2, len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif release_type == "head":
        tags.append(release_type)
    else:
        raise ValueError(f"{release_type} is not valid release part")
    return tags


def buildx_args(bucket_prefix: str, arch: str) -> List[str]:
    args = [f"--platform=linux/{arch}", f"--label=build-url={GITHUB_RUN_URL}"]
    if bucket_prefix:
        url = p.join(bucket_prefix, BUCKETS[arch])  # to prevent a double //
        args.append(f"--build-arg=REPOSITORY='{url}'")
        args.append(f"--build-arg=deb_location_url='{url}'")
    return args


def build_and_push_image(
    image: DockerImage,
    push: bool,
    bucket_prefix: str,
    os: str,
    tag: str,
    version: ClickHouseVersion,
) -> List[Tuple[str, str]]:
    result = []
    if os != "ubuntu":
        tag += f"-{os}"
    init_args = ["docker", "buildx", "build", "--build-arg BUILDKIT_INLINE_CACHE=1"]
    if push:
        init_args.append("--push")
        init_args.append("--output=type=image,push-by-digest=true")
        init_args.append(f"--tag={image.repo}")
    else:
        init_args.append("--output=type=docker")

    # `docker buildx build --load` does not support multiple images currently
    # images must be built separately and merged together with `docker manifest`
    digests = []
    for arch in BUCKETS:
        arch_tag = f"{tag}-{arch}"
        metadata_path = p.join(TEMP_PATH, arch_tag)
        dockerfile = p.join(image.full_path, f"Dockerfile.{os}")
        cmd_args = list(init_args)
        cmd_args.extend(buildx_args(bucket_prefix, arch))
        if not push:
            cmd_args.append(f"--tag={image.repo}:{arch_tag}")
        cmd_args.extend(
            [
                f"--metadata-file={metadata_path}",
                f"--build-arg=VERSION='{version.string}'",
                "--progress=plain",
                f"--file={dockerfile}",
                image.full_path,
            ]
        )
        cmd = " ".join(cmd_args)
        logging.info("Building image %s:%s for arch %s: %s", image.repo, tag, arch, cmd)
        if retry_popen(cmd) != 0:
            result.append((f"{image.repo}:{tag}-{arch}", "FAIL"))
            return result
        result.append((f"{image.repo}:{tag}-{arch}", "OK"))
        with open(metadata_path, "rb") as m:
            metadata = json.load(m)
            digests.append(metadata["containerimage.digest"])
    if push:
        cmd = (
            "docker buildx imagetools create "
            f"--tag {image.repo}:{tag} {' '.join(digests)}"
        )
        logging.info("Pushing merged %s:%s image: %s", image.repo, tag, cmd)
        if retry_popen(cmd) != 0:
            result.append((f"{image.repo}:{tag}", "FAIL"))
            return result
    else:
        logging.info(
            "Merging is available only on push, separate %s images are created",
            f"{image.repo}:{tag}-$arch",
        )

    return result


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()
    makedirs(TEMP_PATH, exist_ok=True)

    args = parse_args()
    image = DockerImage(args.image_path, args.image_repo, False)
    args.release_type = auto_release_type(args.version, args.release_type)
    tags = gen_tags(args.version, args.release_type)
    NAME = f"Docker image {image.repo} building check (actions)"
    pr_info = None
    if CI:
        pr_info = PRInfo()
        release_or_pr = get_release_or_pr(pr_info, {"package_type": ""}, args.version)
        args.bucket_prefix = (
            f"https://s3.amazonaws.com/{S3_BUILDS_BUCKET}/"
            f"{release_or_pr}/{pr_info.sha}"
        )

    if args.push:
        subprocess.check_output(  # pylint: disable=unexpected-keyword-arg
            "docker login --username 'robotclickhouse' --password-stdin",
            input=get_parameter_from_ssm("dockerhub_robot_password"),
            encoding="utf-8",
            shell=True,
        )
        NAME = f"Docker image {image.repo} build and push (actions)"

    logging.info("Following tags will be created: %s", ", ".join(tags))
    status = "success"
    test_results = []  # type: List[Tuple[str, str]]
    for os in args.os:
        for tag in tags:
            test_results.extend(
                build_and_push_image(
                    image, args.push, args.bucket_prefix, os, tag, args.version
                )
            )
            if test_results[-1][1] != "OK":
                status = "failure"

    pr_info = pr_info or PRInfo()
    s3_helper = S3Helper("https://s3.amazonaws.com")

    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")
    print(f'::set-output name=url_output::"{url}"')

    if not args.reports:
        return

    description = f"Processed tags: {', '.join(tags)}"

    if len(description) >= 140:
        description = description[:136] + "..."

    gh = Github(get_best_robot_token())
    post_commit_status(gh, pr_info.sha, NAME, description, status, url)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        url,
        NAME,
    )
    ch_helper = ClickHouseHelper()
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)


if __name__ == "__main__":
    main()
