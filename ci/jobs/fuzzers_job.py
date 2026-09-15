from praktika.result import Result
from praktika.utils import Shell, Utils

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.info import Info

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseProc()
    info = Info()

    if res:
        print("Install ClickHouse")

        def install():
            return ch.fuzzer_config_tweaks()

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=install)
        )
        res = results[-1].is_ok()

    if res:
        print("Start ClickHouse")

        def start():
            return ch.start()
            # TODO: attach gdb
            # and ch.attach_gdb()

        if ch.create_log_export_config():
            ch.start_log_exports(check_start_time=stop_watch.start_time)

    if res:
        print("AST Fuzzer")

        commands = ["./ci/jobs/scripts/fuzzer/runner.sh"]
        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=commands,
            )
        )
        res = results[-1].is_ok()

    # stop log replication
    Shell.check(
        f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
        verbose=True,
    )

    Result.create_from(
        results=results, stopwatch=stop_watch, files=[ch.prepare_logs(info=info)]
    ).complete_job()


if __name__ == "__main__":
    main()
