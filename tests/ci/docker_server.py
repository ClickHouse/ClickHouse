#!/usr/bin/env python

# here
import argparse
import json
import logging
import sys
import time
from pathlib import Path
from os import path as p, makedirs
from typing import List

from github import Github

from build_check import get_release_or_pr
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import format_description, get_commit, post_commit_status
from docker_images_helper import DockerImageData, docker_login
from env_helper import (
    CI,
    GITHUB_RUN_URL,
    REPORT_PATH,
    TEMP_PATH,
    S3_BUILDS_BUCKET,
    S3_DOWNLOAD,
)
from get_robot_token import get_best_robot_token
from git_helper import Git
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from build_download_helper import read_build_urls
from upload_result_helper import upload_results
from version_helper import (
    ClickHouseVersion,
    get_tagged_versions,
    get_version_from_repo,
    version_arg,
)

git = Git(ignore_no_tags=True)

ARCH = ("amd64", "arm64")


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
    parser.add_argument(
        "--allow-build-reuse",
        action="store_true",
        help="allows binaries built on different branch if source digest matches current repo state",
    )

    return parser.parse_args()


def retry_popen(cmd: str, log_file: Path) -> int:
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
        with TeePopen(
            cmd,
            log_file=log_file,
        ) as process:
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


def buildx_args(urls: dict[str, str], arch: str) -> List[str]:
    args = [
        f"--platform=linux/{arch}",
        f"--label=build-url={GITHUB_RUN_URL}",
        f"--label=com.clickhouse.build.githash={git.sha}",
    ]
    if urls:
        url = urls[arch]
        args.append(f"--build-arg=REPOSITORY='{url}'")
        args.append(f"--build-arg=deb_location_url='{url}'")
    return args


def build_and_push_image(
    image: DockerImageData,
    push: bool,
    repo_urls: dict[str, str],
    os: str,
    tag: str,
    version: ClickHouseVersion,
) -> TestResults:
    result = []  # type: TestResults
    if os != "ubuntu":
        tag += f"-{os}"
    init_args = ["docker", "buildx", "build"]
    if push:
        init_args.append("--push")
        init_args.append("--output=type=image,push-by-digest=true")
        init_args.append(f"--tag={image.repo}")
    else:
        init_args.append("--output=type=docker")

    # `docker buildx build --load` does not support multiple images currently
    # images must be built separately and merged together with `docker manifest`
    digests = []
    multiplatform_sw = Stopwatch()
    for arch in ARCH:
        single_sw = Stopwatch()
        arch_tag = f"{tag}-{arch}"
        metadata_path = p.join(TEMP_PATH, arch_tag)
        dockerfile = p.join(image.path, f"Dockerfile.{os}")
        cmd_args = list(init_args)
        cmd_args.extend(buildx_args(repo_urls, arch))
        if not push:
            cmd_args.append(f"--tag={image.repo}:{arch_tag}")
        cmd_args.extend(
            [
                f"--metadata-file={metadata_path}",
                f"--build-arg=VERSION='{version.string}'",
                "--progress=plain",
                f"--file={dockerfile}",
                image.path.as_posix(),
            ]
        )
        cmd = " ".join(cmd_args)
        logging.info("Building image %s:%s for arch %s: %s", image.repo, tag, arch, cmd)
        log_file = Path(TEMP_PATH) / f"{image.repo.replace('/', '__')}:{tag}-{arch}.log"
        if retry_popen(cmd, log_file) != 0:
            result.append(
                TestResult(
                    f"{image.repo}:{tag}-{arch}",
                    "FAIL",
                    single_sw.duration_seconds,
                    [log_file],
                )
            )
            return result
        result.append(
            TestResult(
                f"{image.repo}:{tag}-{arch}",
                "OK",
                single_sw.duration_seconds,
                [log_file],
            )
        )
        with open(metadata_path, "rb") as m:
            metadata = json.load(m)
            digests.append(metadata["containerimage.digest"])
    if push:
        cmd = (
            "docker buildx imagetools create "
            f"--tag {image.repo}:{tag} {' '.join(digests)}"
        )
        logging.info("Pushing merged %s:%s image: %s", image.repo, tag, cmd)
        if retry_popen(cmd, Path("/dev/null")) != 0:
            result.append(
                TestResult(
                    f"{image.repo}:{tag}", "FAIL", multiplatform_sw.duration_seconds
                )
            )
            return result
        result.append(
            TestResult(f"{image.repo}:{tag}", "OK", multiplatform_sw.duration_seconds)
        )
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
    image = DockerImageData(args.image_path, args.image_repo, False)
    args.release_type = auto_release_type(args.version, args.release_type)
    tags = gen_tags(args.version, args.release_type)
    NAME = f"Docker image {image.repo} building check"
    pr_info = None
    repo_urls = dict()
    pr_info = PRInfo()
    release_or_pr, _ = get_release_or_pr(pr_info, args.version)
    for arch, build_name in zip(ARCH, ("package_release", "package_aarch64")):
        if CI:
            if args.allow_build_reuse:
                # read s3 urls from pre-downloaded build reports
                urls = read_build_urls(build_name, Path(REPORT_PATH))
                url = urls[0].split(build_name)[0][:-1]
                repo_urls[arch] = f"{url}/{build_name}"
            else:
                # generate url address for build in current ci run
                repo_urls[
                    arch
                ] = f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{release_or_pr}/{pr_info.sha}/{build_name}"
        elif args.bucket_prefix:
            repo_urls[arch] = f"{args.bucket_prefix}/{build_name}"

    if args.push:
        docker_login()
        NAME = f"Docker image {image.repo} build and push"

    logging.info("Following tags will be created: %s", ", ".join(tags))
    status = "success"
    test_results = []  # type: TestResults
    for os in args.os:
        for tag in tags:
            test_results.extend(
                build_and_push_image(image, args.push, repo_urls, os, tag, args.version)
            )
            if test_results[-1].status != "OK":
                status = "failure"

    pr_info = pr_info or PRInfo()
    s3_helper = S3Helper()

    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME)

    print(f"::notice ::Report url: {url}")

    if not args.reports:
        return

    description = f"Processed tags: {', '.join(tags)}"

    description = format_description(description)

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    post_commit_status(
        commit, status, url, description, NAME, pr_info, dump_to_file=True
    )

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
    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
