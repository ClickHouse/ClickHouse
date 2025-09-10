import argparse
import os
import subprocess
from pathlib import Path
from typing import List, Tuple

from pip._vendor.packaging.version import Version

from ci.jobs.scripts.docker_image import DockerImage
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Utils, Shell

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
DOWNLOAD_RETRIES_COUNT = 5


def process_os_check(log_path: Path) -> Result:
    name = log_path.name
    with open(log_path, "r", encoding="utf-8") as log:
        line = log.read().split("\n")[0].strip()
        if line != "OK":
            return Result(name=name, status="FAIL", files=[log_path])
        return Result(name=name, status="OK")


def process_glibc_check(log_path: Path, max_glibc_version: str) -> List[Result]:
    test_results = []
    with open(log_path, "r", encoding="utf-8") as log:
        for line in log:
            if line.strip():
                columns = line.strip().split(" ")
                symbol_with_glibc = columns[-2]  # sysconf@GLIBC_2.2.5
                _, version = symbol_with_glibc.split("@GLIBC_")
                if version == "PRIVATE":
                    test_results.append(Result(symbol_with_glibc, "FAIL"))
                elif Version(version) > Version(max_glibc_version):
                    test_results.append(Result(symbol_with_glibc, "FAIL"))
    if not test_results:
        test_results.append(Result("glibc check", "OK"))
    return test_results


def process_result(
    result_directory: Path,
    server_log_directory: Path,
    check_glibc: bool,
    check_distributions: bool,
    max_glibc_version: str,
) -> Tuple[str, str, List[Result], List[Path]]:
    glibc_log_path = result_directory / "glibc.log"
    test_results = process_glibc_check(glibc_log_path, max_glibc_version)

    status = Result.Status.SUCCESS
    description = "Compatibility check passed"

    if check_glibc:
        if len(test_results) > 1 or test_results[0].status != "OK":
            status = Result.Status.FAILED
            description = "glibc check failed"

    if status == Result.Status.SUCCESS and check_distributions:
        for operating_system in ("ubuntu:12.04", "centos:5"):
            test_result = process_os_check(result_directory / operating_system)
            if test_result.status != "OK":
                status = Result.Status.FAILED
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
        # FIXME: odbc bridge is not present in the deb package
        # f"readelf -s --wide {build_path}/usr/bin/clickhouse-odbc-bridge | "
        f"grep '@GLIBC_' >> {result_directory}/glibc.log",
        # FIXME: library bridge is not present in the deb package
        # f"readelf -s --wide {build_path}/usr/bin/clickhouse-library-bridge | "
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
    stopwatch = Utils.Stopwatch()

    args = parse_args()
    check_name = args.check_name or os.getenv("CHECK_NAME")
    assert check_name
    check_glibc = True
    # currently hardcoded to x86, don't enable for AARCH64
    check_distributions = (
        "aarch64" not in check_name.lower() and "arm" not in check_name.lower()
    )

    repo_dir = Utils.cwd()
    temp_path = Path(repo_dir) / "ci" / "tmp"

    installed = 0
    for package in temp_path.iterdir():
        if package.suffix == ".deb" and any(
            package.name.startswith(prefix)
            for prefix in ("clickhouse-server_", "clickhouse-common-static_")
        ):
            Shell.check(f"dpkg -x {package} {temp_path}", verbose=True)
            installed += 1

    if installed < 2:
        assert False, f"No deb packages in {temp_path}"

    run_commands = []

    if check_glibc:
        check_glibc_commands = get_run_commands_glibc(temp_path, temp_path)
        run_commands.extend(check_glibc_commands)

    if check_distributions:
        centos_image = DockerImage.get_docker_image(IMAGE_CENTOS).pull_image()
        ubuntu_image = DockerImage.get_docker_image(IMAGE_UBUNTU).pull_image()
        check_distributions_commands = get_run_commands_distributions(
            temp_path,
            temp_path,
            temp_path,
            centos_image,
            ubuntu_image,
        )
        run_commands.extend(check_distributions_commands)

    for run_command in run_commands:
        try:
            print(f"Running command {run_command}")
            subprocess.check_call(run_command, shell=True)
        except subprocess.CalledProcessError as ex:
            print(f"Exception calling command: {ex}")
            Result.create_from(
                status=Result.Status.ERROR, info=f"Exception calling command: {ex}"
            ).complete_job()

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    # See https://sourceware.org/glibc/wiki/Glibc%20Timeline
    max_glibc_version = ""
    if "amd" in check_name or "(release)" in check_name:
        max_glibc_version = "2.4"
    elif "aarch" in check_name or "arm" in check_name:
        max_glibc_version = "2.18"  # because of build with newer sysroot?
    else:
        raise RuntimeError("Can't determine max glibc version")

    state, description, test_results, additional_logs = process_result(
        temp_path,
        temp_path,
        check_glibc,
        check_distributions,
        max_glibc_version,
    )

    Result(
        name=Info().job_name,
        info=description,
        results=test_results,
        status=state,
        start_time=stopwatch.start_time,
        duration=stopwatch.duration,
        files=additional_logs,
    ).complete_job()


if __name__ == "__main__":
    main()
