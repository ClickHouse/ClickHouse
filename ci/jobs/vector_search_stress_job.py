import sys
import traceback

from ci.jobs.clickbench import install_introspection
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"

# The binary under test: the release build of PR #131027
CH_BINARY_URL_ARM = "https://clickhouse-builds.s3.amazonaws.com/PRs/131027/e7d7a78a5a9df6ac19262116ba9805a3f3f54c43/pr/build_arm_release/clickhouse"
CH_BINARY_URL_AMD = "https://clickhouse-builds.s3.amazonaws.com/PRs/131027/e7d7a78a5a9df6ac19262116ba9805a3f3f54c43/pr/build_amd_release/clickhouse"


def clickhouse_binary_url():
    if Utils.is_arm():
        return CH_BINARY_URL_ARM
    if Utils.is_amd():
        return CH_BINARY_URL_AMD
    assert False, "Unknown processor architecture"


def main():
    results = []  # array of job's sub results - see CI report
    stop_watch = Utils.Stopwatch()
    info = Info()

    # bash wrappers examples
    _ = Shell.check("echo 'Run any shell command'", verbose=True)
    assert Shell.get_output("echo 'Run any shell command'") == "Run any shell command"

    try:
        if not info.is_local_run:
            ch_url = clickhouse_binary_url()
            print(f"ClickHouse binary : {ch_url}")
            results.append(r := Result.from_commands_run(
                name="Download ClickHouse",
                command=[
                    f"wget -nv -P {temp_dir} {ch_url}",
                    f"chmod +x {temp_dir}/clickhouse",
                    f"{temp_dir}/clickhouse --version",
                ],
            ))
            if not r.is_ok():
                raise RuntimeError(f"[{r.name}] failed: {r.info}")

        with ClickHouseService(
            results=results,
            config_hooks=[
                ClickHouseService.install_base,
                install_introspection,
            ],
        ):
            results.append(
                r := Result.from_commands_run(
                    name="Load the data",
                    command=[lambda: S3.copy_file_from_s3(
                        s3_path="clickhouse-datasets/hits/partitions/hits_v1.tar",
                        local_path=temp_dir,
                    )],
                    with_info=True,
                )
            )
            if not r.is_ok():
                raise RuntimeError(f"[{r.name}] failed: {r.info}")

            print("Tests")
            test_results = []
            test_results.append(
                Result.from_commands_run(
                    name="select 1",
                    command=f"{temp_dir}/clickhouse-client --query 'select 1'",
                )
            )
            test_results.append(Result(name="test 2", status=Result.Status.OK))
            results.append(Result.create_from(name="Tests", results=test_results))
    except Exception as e:
        print(traceback.format_exc(), file=sys.stdout)
        results.append(
            Result(name="Job error", status=Result.Status.FAIL, info=str(e))
        )

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=[],
        info="write result info here",
    ).complete_job()


if __name__ == "__main__":
    main()
