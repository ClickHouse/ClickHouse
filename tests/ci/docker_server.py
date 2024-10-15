#!/usr/bin/env python

# here
import argparse
import json
import logging
import sys
import time
from os import makedirs
from os import path as p
from pathlib import Path
from typing import Dict, List

from build_download_helper import read_build_urls
from docker_images_helper import DockerImageData, docker_login
from env_helper import (
    GITHUB_RUN_URL,
    REPORT_PATH,
    S3_BUILDS_BUCKET,
    S3_DOWNLOAD,
    TEMP_PATH,
)
from git_helper import Git
from pr_info import PRInfo
from report import FAILURE, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen
from version_helper import ClickHouseVersion, get_version_from_repo, version_arg

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
        "--check-name",
        required=False,
        default="",
    )
    parser.add_argument(
        "--version",
        type=version_arg,
        default=get_version_from_repo(git=git).string,
        help="a version to build, automaticaly got from version_helper, accepts either "
        "tag ('refs/tags/' is removed automatically) or a normal 22.2.2.2 format",
    )
    parser.add_argument(
        "--sha",
        type=str,
        default="",
        help="sha of the commit to use packages from",
    )
    parser.add_argument(
        "--tag-type",
        type=str,
        choices=("head", "release", "release-latest"),
        default="head",
        help="defines required tags for resulting docker image. "
        "head - for master image (tag: head) "
        "release - for release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX) "
        "release-latest - for latest release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX, latest) ",
    )
    parser.add_argument(
        "--image-path",
        type=str,
        default="",
        help="a path to docker context directory",
    )
    parser.add_argument(
        "--image-repo",
        type=str,
        default="",
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
    parser.add_argument("--push", action="store_true", help=argparse.SUPPRESS)
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
    max_retries = 2
    sleep_seconds = 10
    retcode = -1
    for _retry in range(max_retries):
        with TeePopen(
            cmd,
            log_file=log_file,
        ) as process:
            retcode = process.wait()
            if retcode == 0:
                return 0
            # From time to time docker build may failed. Curl issues, or even push
            logging.error(
                "The following command failed, sleep %s before retry: %s",
                sleep_seconds,
                cmd,
            )
            time.sleep(sleep_seconds)
    return retcode


def gen_tags(version: ClickHouseVersion, tag_type: str) -> List[str]:
    """
    @tag_type release-latest, @version 22.2.2.2:
    - latest
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    @tag_type release, @version 22.2.2.2:
    - 22
    - 22.2
    - 22.2.2
    - 22.2.2.2
    @tag_type head:
    - head
    """
    parts = version.string.split(".")
    tags = []
    if tag_type == "release-latest":
        tags.append("latest")
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    elif tag_type == "head":
        tags.append(tag_type)
    elif tag_type == "release":
        for i in range(len(parts)):
            tags.append(".".join(parts[: i + 1]))
    else:
        assert False, f"Invalid release type [{tag_type}]"
    return tags


def buildx_args(
    urls: Dict[str, str], arch: str, direct_urls: List[str], version: str
) -> List[str]:
    args = [
        f"--platform=linux/{arch}",
        f"--label=build-url={GITHUB_RUN_URL}",
        f"--label=com.clickhouse.build.githash={git.sha}",
        f"--label=com.clickhouse.build.version={version}",
    ]
    if direct_urls:
        args.append(f"--build-arg=DIRECT_DOWNLOAD_URLS='{' '.join(direct_urls)}'")
    elif urls:
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
    direct_urls: Dict[str, List[str]],
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
        urls = []
        if direct_urls:
            if os == "ubuntu" and "clickhouse-server" in image.repo:
                urls = [url for url in direct_urls[arch] if ".deb" in url]
            else:
                urls = [url for url in direct_urls[arch] if ".tgz" in url]
        cmd_args.extend(
            buildx_args(repo_urls, arch, direct_urls=urls, version=version.describe)
        )
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

    pr_info = PRInfo()

    if args.check_name:
        assert not args.image_path and not args.image_repo
        if "server image" in args.check_name:
            image_path = "docker/server"
            image_repo = "clickhouse/clickhouse-server"
        elif "keeper image" in args.check_name:
            image_path = "docker/keeper"
            image_repo = "clickhouse/clickhouse-keeper"
        else:
            assert False, "Invalid --check-name"
    else:
        assert args.image_path and args.image_repo
        image_path = args.image_path
        image_repo = args.image_repo

    push = args.push
    del args.image_path
    del args.image_repo
    del args.push

    if pr_info.is_master:
        push = True

    image = DockerImageData(image_path, image_repo, False)
    tags = gen_tags(args.version, args.tag_type)
    repo_urls = {}
    direct_urls: Dict[str, List[str]] = {}

    for arch, build_name in zip(ARCH, ("package_release", "package_aarch64")):
        if args.allow_build_reuse:
            # read s3 urls from pre-downloaded build reports
            if "clickhouse-server" in image_repo:
                PACKAGES = [
                    "clickhouse-client",
                    "clickhouse-server",
                    "clickhouse-common-static",
                ]
            elif "clickhouse-keeper" in image_repo:
                PACKAGES = ["clickhouse-keeper"]
            else:
                assert False, "BUG"
            urls = read_build_urls(build_name, Path(REPORT_PATH))
            assert (
                urls
            ), f"URLS has not been read from build report, report path[{REPORT_PATH}], build [{build_name}]"
            direct_urls[arch] = [
                url
                for url in urls
                if any(package in url for package in PACKAGES) and "-dbg" not in url
            ]
        elif args.bucket_prefix:
            assert not args.allow_build_reuse
            repo_urls[arch] = f"{args.bucket_prefix}/{build_name}"
            print(f"Bucket prefix is set: Fetching packages from [{repo_urls}]")
        elif args.sha:
            version = args.version
            repo_urls[arch] = (
                f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/"
                f"{version.major}.{version.minor}/{args.sha}/{build_name}"
            )
            print(f"Fetching packages from [{repo_urls}]")
        else:
            assert (
                False
            ), "--sha, --bucket_prefix or --allow-build-reuse (to fetch packages from build report) must be provided"

    if push:
        docker_login()

    logging.info("Following tags will be created: %s", ", ".join(tags))
    status = SUCCESS
    test_results = []  # type: TestResults
    for os in args.os:
        for tag in tags:
            test_results.extend(
                build_and_push_image(
                    image, push, repo_urls, os, tag, args.version, direct_urls
                )
            )
            if test_results[-1].status != "OK":
                status = FAILURE

    description = f"Processed tags: {', '.join(tags)}"
    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[],
    ).dump()

    if status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
