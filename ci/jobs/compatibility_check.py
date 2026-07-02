import argparse
from pathlib import Path

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_path = Path(f"{Utils.cwd()}/ci/tmp")


def parse_args():
    parser = argparse.ArgumentParser("Check compatibility with old distributions")
    parser.add_argument("--check-name", required=False)
    return parser.parse_args()


def main():
    stopwatch = Utils.Stopwatch()

    check_name = Info().job_name
    assert check_name

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

    Result.create_from(results=test_results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
