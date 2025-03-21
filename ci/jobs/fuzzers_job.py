from praktika.result import Result
from praktika.utils import Shell, Utils

from ci.jobs.scripts.clickhouse_proc import ClickHouseLight

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseLight()

    if res:
        print("Install ClickHouse")

        def install():
            return ch.install() and ch.fuzzer_config_tweaks()

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

        log_export_config = f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --config-logs-export-cluster {ch.config_path}/config.d/system_logs_export.yaml"
        setup_logs_replication = f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication"

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=[start, log_export_config, setup_logs_replication],
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res:
        print("AST Fuzzer")

        commands = ["./ci/jobs/scripts/fuzzer/runner.sh"]
        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=commands,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    # stop log replication
    Shell.check(
        f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
        verbose=True,
    )

    Result.create_from(results=results, stopwatch=stop_watch, files=[]).complete_job()


if __name__ == "__main__":
    main()
