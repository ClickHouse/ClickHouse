import argparse
import subprocess
from pathlib import Path
from typing import List, Tuple

from pip._vendor.packaging.version import Version

from ci.jobs.scripts.docker_image import DockerImage
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
DOWNLOAD_RETRIES_COUNT = 5

temp_path = Path(f"{Utils.cwd()}/ci/tmp")


def process_os_check(log_path: Path) -> Result:
    name = log_path.name
    with open(log_path, "r", encoding="utf-8") as log:
        line = log.read().split("\n")[0].strip()
        if line != "OK":
            return Result(name=name, status=Result.StatusExtended.FAIL)
        return Result(name=name, status=Result.StatusExtended.OK)


def process_glibc_check(log_path: Path, max_glibc_version: str) -> List[Result]:
    test_results = []
    with open(log_path, "r", encoding="utf-8") as log:
        for line in log:
            if line.strip():
                columns = line.strip().split(" ")
                symbol_with_glibc = columns[-2]  # sysconf@GLIBC_2.2.5
                _, version = symbol_with_glibc.split("@GLIBC_")
                if version == "PRIVATE":
                    test_results.append(
                        Result(symbol_with_glibc, Result.StatusExtended.FAIL)
                    )
                elif Version(version) > Version(max_glibc_version):
                    test_results.append(
                        Result(symbol_with_glibc, Result.StatusExtended.FAIL)
                    )
    if not test_results:
        test_results.append(Result("glibc check", Result.StatusExtended.OK))
    return test_results


def process_result(
    check_glibc: bool,
    check_distributions: bool,
    max_glibc_version: str,
) -> Tuple[str, str, List[Result], List[Path]]:
    glibc_log_path = temp_path / "glibc.log"
    test_results = process_glibc_check(glibc_log_path, max_glibc_version)

    status = Result.Status.SUCCESS
    description = "Compatibility check passed"

    if check_glibc:
        if len(test_results) > 1 or test_results[0].status != "OK":
            status = Result.Status.FAILED
            description = "glibc check failed"

    if status == Result.Status.SUCCESS and check_distributions:
        for operating_system in ("ubuntu:12.04", "centos:5"):
            test_result = process_os_check(temp_path / operating_system)
            if test_result.status != "OK":
                status = Result.Status.FAILED
                description = f"Old {operating_system} failed"
                test_results += [test_result]
                break
            test_results += [test_result]

    result_logs = [
        p
        for p in [
            temp_path / name
            for name in ("clickhouse-server.log", "stderr.log", "clientstderr.log")
        ]
        + [glibc_log_path]
        if p.exists()
    ]

    return status, description, test_results, result_logs


def get_run_commands_glibc() -> List[str]:
    return [
        f"readelf -s --wide {temp_path}/clickhouse | grep '@GLIBC_' > {temp_path}/glibc.log",
        # FIXME: odbc bridge is not present in the deb package
        # f"readelf -s --wide {temp_path}/clickhouse-odbc-bridge | grep '@GLIBC_' >> {temp_path}/glibc.log",
        # FIXME: library bridge is not present in the deb package
        # f"readelf -s --wide {temp_path}/clickhouse-library-bridge | grep '@GLIBC_' >> {temp_path}/glibc.log",
    ]


def get_run_commands_distributions(
    image_centos: DockerImage,
    image_ubuntu: DockerImage,
) -> List[str]:
    return [
        f"docker run --network=host --volume={temp_path}/ci/tmp/clickhouse:/clickhouse "
        f"--volume={temp_path}/etc/clickhouse-server:/config "
        f"--volume={temp_path}:/var/log/clickhouse-server {image_ubuntu} > "
        f"{temp_path}/ubuntu:12.04",
        f"docker run --network=host --volume={temp_path}/clickhouse:/clickhouse "
        f"--volume={temp_path}/etc/clickhouse-server:/config "
        f"--volume={temp_path}:/var/log/clickhouse-server {image_centos} > "
        f"{temp_path}/centos:5",
    ]


def parse_args():
    parser = argparse.ArgumentParser("Check compatibility with old distributions")
    parser.add_argument("--check-name", required=False)
    return parser.parse_args()


def main():
    stopwatch = Utils.Stopwatch()

    check_name = Info().job_name
    assert check_name
    check_glibc = True
    # currently hardcoded to x86, don't enable for AARCH64
    check_distributions = (
        "aarch64" not in check_name.lower() and "arm" not in check_name.lower()
    )
    Shell.check(f"chmod +x {temp_path}/clickhouse", verbose=True, strict=True)

    run_commands = []

    if check_glibc:
        check_glibc_commands = get_run_commands_glibc()
        run_commands.extend(check_glibc_commands)

    if check_distributions:
        centos_image = DockerImage.get_docker_image(IMAGE_CENTOS).pull_image()
        ubuntu_image = DockerImage.get_docker_image(IMAGE_UBUNTU).pull_image()
        check_distributions_commands = get_run_commands_distributions(
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
    if "amd" in check_name:
        max_glibc_version = "2.4"
    elif "arm" in check_name:
        max_glibc_version = "2.18"  # because of build with newer sysroot?
    else:
        raise RuntimeError("Can't determine max glibc version")

    state, description, test_results, additional_logs = process_result(
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
