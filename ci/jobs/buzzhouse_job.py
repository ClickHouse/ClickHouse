from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseProc()

    if res:
        print("Install ClickHouse")

        def install():
            res = ch.install_fuzzer_config()
            if Info().is_local_run:
                return res
            return res and ch.create_log_export_config()

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=install)
        )
        res = results[-1].is_ok()

    if res:
        print("Start ClickHouse")

        def start():
            res = ch.start_light()
            if Info().is_local_run:
                return res
            return res and ch.start_log_exports(check_start_time=stop_watch.start_time)

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=start,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Buzzing")
        commnads = [
            "echo Hello World",
            "clickhouse-client --query 'select 1'",
        ]

        results.append(Result.from_commands_run(name="Buzzing", command=commnads))

    # stop log replication
    Shell.check(
        f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
        verbose=True,
    )

    Result.create_from(results=results, stopwatch=stop_watch, files=[]).complete_job()


if __name__ == "__main__":
    main()
