from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseProc()
    info = Info()

    _ = Shell.check("echo 'Run any shell command'", verbose=True)
    assert Shell.get_output("echo 'Run any shell command'") == "Run any shell command"

    if Utils.is_arm():
        latest_ch_master_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
    elif Utils.is_amd():
        latest_ch_master_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
    else:
        assert False, f"Unknown processor architecture"

    if res and not info.is_local_run:
        print("Download ClickHouse")
        commands = [
            f"wget -nv -P {temp_dir} {latest_ch_master_url}",
            f"chmod +x {temp_dir}/clickhouse",
            f"{temp_dir}/clickhouse --version",
        ]
        results.append(
            Result.from_commands_run(name="Download ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

    if res:
        print("Install ClickHouse")

        def install():
            # implement required ch configuration
            return (
                ch.install_clickbench_config()
            )  # reuses config used for clickbench job, it's more or less default ch configuration

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=[install])
        )
        res = results[-1].is_ok()

    if res:
        print("Start ClickHouse")

        def start():
            return ch.start_light()

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=[
                    start,
                ],
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Download the data")

        def do():
            res = S3.copy_file_from_s3(
                s3_path="clickhouse-datasets/hits/partitions/hits_v1.tar",
                local_path=temp_dir,
            )
            return res

        results.append(
            Result.from_commands_run(
                name="Load the data",
                command=[
                    do,
                ],
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Tests")
        commands = [
            f"{temp_dir}/clickhouse-client --query 'select 1'",
        ]
        results.append(Result.from_commands_run(name="Tests", command=commands))
        res = results[-1].is_ok()

    Result.create_from(
        results=results,  # job status success or failure will be generated in accordance with subtask results in this array
        # status=Result.Status.FAILED, # or set status here
        stopwatch=stop_watch,
        files=[],  # files you need to store after the job completes
        info="write result info here",  # will be shown in the report
    ).complete_job()


if __name__ == "__main__":
    main()
