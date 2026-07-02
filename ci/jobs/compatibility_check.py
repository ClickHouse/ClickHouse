import argparse
from pathlib import Path

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
DOWNLOAD_RETRIES_COUNT = 5

temp_path = Path(f"{Utils.cwd()}/ci/tmp")


def parse_args():
    parser = argparse.ArgumentParser("Check compatibility with old distributions")
    parser.add_argument("--check-name", required=False)
    return parser.parse_args()


def main():
    stopwatch = Utils.Stopwatch()

    check_name = Info().job_name
    assert check_name
    # currently hardcoded to x86, don't enable for AARCH64
    check_distributions = (
        "aarch64" not in check_name.lower() and "arm" not in check_name.lower()
    )

    for package in temp_path.iterdir():
        if package.suffix == ".deb":
            Shell.check(
                f"dpkg -x {package} {temp_path} && rm {package}",
                verbose=True,
                strict=True,
            )
    Shell.check(
        f"mv {temp_path}/usr/bin/clickhouse {temp_path}/clickhouse",
        verbose=True,
        strict=True,
    )
    # Shell.check(f"chmod +x {temp_path}/clickhouse", verbose=True, strict=True)

    test_results = []

    # A fully static (musl) binary must not have a PT_INTERP segment or any
    # DT_NEEDED entries. Either would mean the binary depends on a dynamic
    # linker / shared libraries at runtime.
    test_results.append(
        Result.from_commands_run(
            name="not dynamically linked",
            command=[
                f"test \"$(readelf -l {temp_path}/clickhouse | grep -c INTERP)\" = 0",
                f"test \"$(readelf -d {temp_path}/clickhouse 2>/dev/null | grep -c '(NEEDED)')\" = 0",
            ],
        )
    )

    if check_distributions:
        test_results.append(
            Result.from_commands_run(
                name="ubuntu12",
                command=[
                    f"docker run --volume={temp_path}/clickhouse:/clickhouse ubuntu:12.04 /clickhouse local --query 'select 1'",
                ],
                with_info=True,
            )
        )
        test_results.append(
            Result.from_commands_run(
                name="centos5",
                command=[
                    f"docker run --volume={temp_path}/clickhouse:/clickhouse centos:5 /clickhouse local --query 'select 1'"
                ],
                with_info=True,
            )
        )

    Result.create_from(results=test_results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
