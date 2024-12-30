#!/usr/bin/env python3

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Tuple

import docker_images_helper
from ci_config import CI
from env_helper import REPO_COPY, S3_BUILDS_BUCKET, TEMP_PATH
from git_helper import Git
from pr_info import PRInfo
from report import FAILURE, SUCCESS, JobReport, StatusType
from stopwatch import Stopwatch
from tee_popen import TeePopen
from version_helper import (
    ClickHouseVersion,
    get_version_from_repo,
    update_version_local,
)

IMAGE_NAME = "clickhouse/binary-builder"
BUILD_LOG_NAME = "build_log.log"


def _can_export_binaries(build_config: CI.BuildConfig) -> bool:
    if build_config.package_type != "deb":
        return False
    if build_config.sanitizer != "":
        return True
    if build_config.debug_build:
        return True
    return False


def get_packager_cmd(
    build_config: CI.BuildConfig,
    packager_path: Path,
    output_path: Path,
    build_version: str,
    image_version: str,
    official: bool,
) -> str:
    package_type = build_config.package_type
    comp = build_config.compiler
    cmake_flags = "-DENABLE_CLICKHOUSE_SELF_EXTRACTING=1"
    cmd = (
        f"cd {packager_path} && CMAKE_FLAGS='{cmake_flags}' ./packager "
        f"--output-dir={output_path} --package-type={package_type} --compiler={comp}"
    )

    if build_config.debug_build:
        cmd += " --debug-build"
    if build_config.sanitizer:
        cmd += f" --sanitizer={build_config.sanitizer}"
    if build_config.coverage:
        cmd += " --coverage"
    if build_config.tidy:
        cmd += " --clang-tidy"

    cmd += " --cache=sccache"
    cmd += " --s3-rw-access"
    cmd += f" --s3-bucket={S3_BUILDS_BUCKET}"

    if build_config.additional_pkgs:
        cmd += " --additional-pkgs"

    cmd += f" --docker-image-version={image_version}"
    cmd += " --with-profiler"
    cmd += f" --version={build_version}"

    if _can_export_binaries(build_config):
        cmd += " --with-binaries=tests"

    if official:
        cmd += " --official"

    return cmd


def build_clickhouse(
    packager_cmd: str, logs_path: Path, build_output_path: Path
) -> Tuple[Path, StatusType]:
    build_log_path = logs_path / BUILD_LOG_NAME
    success = False
    with TeePopen(packager_cmd, build_log_path) as process:
        retcode = process.wait()
        if build_output_path.exists():
            results_exists = any(build_output_path.iterdir())
        else:
            results_exists = False

        if retcode == 0:
            if results_exists:
                success = True
                logging.info("Built successfully")
            else:
                logging.info(
                    "Success exit code, but no build artifacts => build failed"
                )
        else:
            logging.info("Build failed")
    return build_log_path, SUCCESS if success else FAILURE


def is_release_pr(pr_info: PRInfo) -> bool:
    return (
        CI.Labels.RELEASE in pr_info.labels or CI.Labels.RELEASE_LTS in pr_info.labels
    )


def get_release_or_pr(pr_info: PRInfo, version: ClickHouseVersion) -> Tuple[str, str]:
    "Return prefixes for S3 artifacts paths"
    # FIXME performance
    # performance builds are havily relies on a fixed path for artifacts, that's why
    # we need to preserve 0 for anything but PR number
    # It should be fixed in performance-comparison image eventually
    # For performance tests we always set PRs prefix
    performance_pr = "PRs/0"
    if is_release_pr(pr_info):
        # for release pull requests we use branch names prefixes, not pr numbers
        return pr_info.head_ref, performance_pr
    if pr_info.number == 0:
        # for pushes to master - major version
        return f"{version.major}.{version.minor}", performance_pr
    # PR number for anything else
    pr_number = f"PRs/{pr_info.number}"
    return pr_number, pr_number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Clickhouse builder script")
    parser.add_argument(
        "build_name",
        help="build name",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    stopwatch = Stopwatch()
    build_name = args.build_name

    build_config = CI.JOB_CONFIGS[build_name].build_config
    assert build_config

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    pr_info = PRInfo()

    logging.info("Repo copy path %s", repo_path)

    version = get_version_from_repo(git=Git(True))
    logging.info("Got version from repo %s", version.string)

    official_flag = pr_info.number == 0

    version_type = "testing"
    if is_release_pr(pr_info):
        version_type = "stable"
        official_flag = True

    update_version_local(version, version_type)

    logging.info("Updated local files with version")

    logging.info("Build short name %s", build_name)

    build_output_path = temp_path / build_name
    build_output_path.mkdir(parents=True, exist_ok=True)

    docker_image = docker_images_helper.pull_image(
        docker_images_helper.get_docker_image(IMAGE_NAME)
    )

    packager_cmd = get_packager_cmd(
        build_config,
        repo_path / "docker" / "packager",
        build_output_path,
        version.string,
        docker_image.version,
        official_flag,
    )

    logging.info("Going to run packager with %s", packager_cmd)

    logs_path = temp_path / "build_log"
    logs_path.mkdir(parents=True, exist_ok=True)

    start = time.time()
    log_path, build_status = build_clickhouse(
        packager_cmd, logs_path, build_output_path
    )
    elapsed = int(time.time() - start)
    subprocess.check_call(
        f"sudo chown -R ubuntu:ubuntu {build_output_path}", shell=True
    )
    logging.info("Build finished as %s, log path %s", build_status, log_path)
    if build_status != SUCCESS:
        # We check if docker works, because if it's down, it's infrastructure
        try:
            subprocess.check_call("docker info", shell=True)
            logging.warning("Collecting 'dmesg -T' content")
            with TeePopen("sudo dmesg -T", build_output_path / "dmesg.log") as process:
                process.wait()
        except subprocess.CalledProcessError:
            logging.error(
                "The dockerd looks down, won't upload anything and generate report"
            )
            sys.exit(1)

    JobReport(
        description=version.describe,
        test_results=[],
        status=build_status,
        start_time=stopwatch.start_time_str,
        duration=elapsed,
        additional_files=[log_path],
        build_dir_for_upload=build_output_path,
        version=version.describe,
    ).dump()

    # Fail the build job if it didn't succeed
    if build_status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
