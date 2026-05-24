import sys
import traceback

from ci.jobs.clickbench import install_clickbench_config
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


def main():
    results = []  # array of job's sub results - see CI report
    stop_watch = Utils.Stopwatch()
    info = Info()

    # bash wrappers examples
    _ = Shell.check("echo 'Run any shell command'", verbose=True)
    assert Shell.get_output("echo 'Run any shell command'") == "Run any shell command"

    # Get link to the latest CH binary
    if Utils.is_arm():
        latest_ch_master_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
    elif Utils.is_amd():
        latest_ch_master_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
    else:
        assert False, f"Unknown processor architecture"

    try:
        if not info.is_local_run:
            results.append(r := Result.from_commands_run(
                name="Download ClickHouse",
                command=[
                    f"wget -nv -P {temp_dir} {latest_ch_master_url}",
                    f"chmod +x {temp_dir}/clickhouse",
                    f"{temp_dir}/clickhouse --version",
                ],
            ))
            r.raise_if_failed()

        results.append(r := Result.from_commands_run(name="Install ClickHouse", command=[install_clickbench_config]))
        r.raise_if_failed()

        with ClickHouseService(results=results) as service:
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
            r.raise_if_failed()

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
