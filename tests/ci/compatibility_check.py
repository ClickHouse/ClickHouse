#!/usr/bin/env python3

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from pip._vendor.packaging.version import Version

from build_download_helper import download_builds_filter
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from report import FAILURE, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
DOWNLOAD_RETRIES_COUNT = 5


def process_os_check(log_path: Path) -> TestResult:
    name = log_path.name
    with open(log_path, "r", encoding="utf-8") as log:
        line = log.read().split("\n")[0].strip()
        if line != "OK":
            return TestResult(name, "FAIL")
        return TestResult(name, "OK")


def process_glibc_check(log_path: Path, max_glibc_version: str) -> TestResults:
    test_results = []  # type: TestResults
    with open(log_path, "r", encoding="utf-8") as log:
        for line in log:
            if line.strip():
                columns = line.strip().split(" ")
                symbol_with_glibc = columns[-2]  # sysconf@GLIBC_2.2.5
                _, version = symbol_with_glibc.split("@GLIBC_")
                if version == "PRIVATE":
                    test_results.append(TestResult(symbol_with_glibc, "FAIL"))
                elif Version(version) > Version(max_glibc_version):
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

    status = SUCCESS
    description = "Compatibility check passed"

    if check_glibc:
        if len(test_results) > 1 or test_results[0].status != "OK":
            status = FAILURE
            description = "glibc check failed"

    if status == SUCCESS and check_distributions:
        for operating_system in ("ubuntu:12.04", "centos:5"):
            test_result = process_os_check(result_directory / operating_system)
            if test_result.status != "OK":
                status = FAILURE
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
    parser.add_argument("--check-name", required=False)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()
    check_name = args.check_name or os.getenv("CHECK_NAME")
    assert check_name
    check_glibc = True
    # currently hardcoded to x86, don't enable for ARM
    check_distributions = (
        "aarch64" not in check_name.lower() and "arm64" not in check_name.lower()
    )

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    def url_filter(url):
        return url.endswith(".deb") and (
            "clickhouse-common-static_" in url or "clickhouse-server_" in url
        )

    download_builds_filter(check_name, reports_path, packages_path, url_filter)

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

    if check_glibc:
        check_glibc_commands = get_run_commands_glibc(packages_path, result_path)
        run_commands.extend(check_glibc_commands)

    if check_distributions:
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

    state = SUCCESS
    for run_command in run_commands:
        try:
            logging.info("Running command %s", run_command)
            subprocess.check_call(run_command, shell=True)
        except subprocess.CalledProcessError as ex:
            logging.info("Exception calling command %s", ex)
            state = FAILURE

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    # See https://sourceware.org/glibc/wiki/Glibc%20Timeline
    max_glibc_version = ""
    if "amd64" in check_name or "release" in check_name:
        max_glibc_version = "2.4"
    elif "aarch64" in check_name:
        max_glibc_version = "2.18"  # because of build with newer sysroot?
    else:
        raise RuntimeError("Can't determine max glibc version")

    state, description, test_results, additional_logs = process_result(
        result_path,
        server_log_path,
        check_glibc,
        check_distributions,
        max_glibc_version,
    )

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_logs,
    ).dump()

    if state == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
