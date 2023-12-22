#!/usr/bin/env python3

from distutils.version import StrictVersion
from pathlib import Path
from typing import List, Tuple
import argparse
import logging
import subprocess
import sys

from github import Github

from build_download_helper import download_builds_filter
from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import RerunHelper, get_commit, post_commit_status
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import TEMP_PATH, REPORT_PATH
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from upload_result_helper import upload_results

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
DOWNLOAD_RETRIES_COUNT = 5


def process_os_check(log_path: Path) -> TestResult:
    name = log_path.name
    with open(log_path, "r") as log:
        line = log.read().split("\n")[0].strip()
        if line != "OK":
            return TestResult(name, "FAIL")
        return TestResult(name, "OK")


def process_glibc_check(log_path: Path, max_glibc_version: str) -> TestResults:
    test_results = []  # type: TestResults
    with open(log_path, "r") as log:
        for line in log:
            if line.strip():
                columns = line.strip().split(" ")
                symbol_with_glibc = columns[-2]  # sysconf@GLIBC_2.2.5
                _, version = symbol_with_glibc.split("@GLIBC_")
                if version == "PRIVATE":
                    test_results.append(TestResult(symbol_with_glibc, "FAIL"))
                elif StrictVersion(version) > max_glibc_version:
                    test_results.append(TestResult(symbol_with_glibc, "FAIL"))
    if not test_results:
        test_results.append(TestResult("glibc check", "OK"))
    return test_results


def process_result(
    result_directory: Path,
    server_log_directory: Path,
    check_glibc: bool,
    check_distributions: bool,
    max_glibc_version: str,
) -> Tuple[str, str, TestResults, List[Path]]:
    glibc_log_path = result_directory / "glibc.log"
    test_results = process_glibc_check(glibc_log_path, max_glibc_version)

    status = "success"
    description = "Compatibility check passed"

    if check_glibc:
        if len(test_results) > 1 or test_results[0].status != "OK":
            status = "failure"
            description = "glibc check failed"

    if status == "success" and check_distributions:
        for operating_system in ("ubuntu:12.04", "centos:5"):
            test_result = process_os_check(result_directory / operating_system)
            if test_result.status != "OK":
                status = "failure"
                description = f"Old {operating_system} failed"
                test_results += [test_result]
                break
            test_results += [test_result]

    result_logs = [
        p
        for p in [
            server_log_directory / name
            for name in ("clickhouse-server.log", "stderr.log", "clientstderr.log")
        ]
        + [glibc_log_path]
        if p.exists()
    ]

    return status, description, test_results, result_logs


def get_run_commands_glibc(build_path: Path, result_directory: Path) -> List[str]:
    return [
        f"readelf -s --wide {build_path}/usr/bin/clickhouse | "
        f"grep '@GLIBC_' > {result_directory}/glibc.log",
        f"readelf -s --wide {build_path}/usr/bin/clickhouse-odbc-bridge | "
        f"grep '@GLIBC_' >> {result_directory}/glibc.log",
        f"readelf -s --wide {build_path}/usr/bin/clickhouse-library-bridge | "
        f"grep '@GLIBC_' >> {result_directory}/glibc.log",
    ]


def get_run_commands_distributions(
    build_path: Path,
    result_directory: Path,
    server_log_directory: Path,
    image_centos: DockerImage,
    image_ubuntu: DockerImage,
) -> List[str]:
    return [
        f"docker run --network=host --volume={build_path}/usr/bin/clickhouse:/clickhouse "
        f"--volume={build_path}/etc/clickhouse-server:/config "
        f"--volume={server_log_directory}:/var/log/clickhouse-server {image_ubuntu} > "
        f"{result_directory}/ubuntu:12.04",
        f"docker run --network=host --volume={build_path}/usr/bin/clickhouse:/clickhouse "
        f"--volume={build_path}/etc/clickhouse-server:/config "
        f"--volume={server_log_directory}:/var/log/clickhouse-server {image_centos} > "
        f"{result_directory}/centos:5",
    ]


def parse_args():
    parser = argparse.ArgumentParser("Check compatibility with old distributions")
    parser.add_argument("--check-name", required=True)
    parser.add_argument("--check-glibc", action="store_true")
    parser.add_argument(
        "--check-distributions", action="store_true"
    )  # currently hardcoded to x86, don't enable for ARM
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, args.check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    def url_filter(url):
        return url.endswith(".deb") and (
            "clickhouse-common-static_" in url or "clickhouse-server_" in url
        )

    download_builds_filter(args.check_name, reports_path, packages_path, url_filter)

    for package in packages_path.iterdir():
        if package.suffix == ".deb":
            subprocess.check_call(
                f"dpkg -x {package} {packages_path} && rm {package}", shell=True
            )

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_commands = []

    if args.check_glibc:
        check_glibc_commands = get_run_commands_glibc(packages_path, result_path)
        run_commands.extend(check_glibc_commands)

    if args.check_distributions:
        centos_image = pull_image(get_docker_image(IMAGE_CENTOS))
        ubuntu_image = pull_image(get_docker_image(IMAGE_UBUNTU))
        check_distributions_commands = get_run_commands_distributions(
            packages_path,
            result_path,
            server_log_path,
            centos_image,
            ubuntu_image,
        )
        run_commands.extend(check_distributions_commands)

    state = "success"
    for run_command in run_commands:
        try:
            logging.info("Running command %s", run_command)
            subprocess.check_call(run_command, shell=True)
        except subprocess.CalledProcessError as ex:
            logging.info("Exception calling command %s", ex)
            state = "failure"

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    # See https://sourceware.org/glibc/wiki/Glibc%20Timeline
    max_glibc_version = ""
    if "amd64" in args.check_name:
        max_glibc_version = "2.4"
    elif "aarch64" in args.check_name:
        max_glibc_version = "2.18"  # because of build with newer sysroot?
    else:
        raise Exception("Can't determine max glibc version")

    s3_helper = S3Helper()
    state, description, test_results, additional_logs = process_result(
        result_path,
        server_log_path,
        args.check_glibc,
        args.check_distributions,
        max_glibc_version,
    )

    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        additional_logs,
        args.check_name,
    )
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(
        commit,
        state,
        report_url,
        description,
        args.check_name,
        pr_info,
        dump_to_file=True,
    )

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        args.check_name,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
