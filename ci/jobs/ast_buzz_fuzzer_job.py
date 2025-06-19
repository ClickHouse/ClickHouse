import subprocess
from pathlib import Path

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"
    CHECK_ERRORS = "check_errors"
    COLLECT_LOGS = "collect_logs"


def main():
    info = Info()

    fuzzer_name = (
        "BuzzHouse" if info.job_name.lower().startswith("buzzhouse") else "AST Fuzzer"
    )

    temp_path = f"{Utils.cwd()}/ci/tmp"
    ch_path = temp_path
    assert Path(f"{ch_path}/clickhouse").exists(), f"Binary not found in {temp_path}"

    CH = ClickHouseProc()

    Utils.add_to_PATH(ch_path)

    res = True
    results = []
    stages = [s for s in JobStages]

    if res and JobStages.INSTALL_CLICKHOUSE in stages:

        def configure_log_export():
            if not info.is_local_run:
                print("prepare log export config")
                return CH.create_log_export_config()
            else:
                print("skip log export config for local run")

        commands = [
            f"chmod +x {ch_path}/clickhouse",
            f"rm -rf /etc/clickhouse-client/* /etc/clickhouse-server/*",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {ch_path}/ch",
            f"ln -sf /usr/bin/clickhouse-odbc-bridge {ch_path}/clickhouse-odbc-bridge",
            f"cp programs/server/config.xml programs/server/users.xml /etc/clickhouse-server/",
            f"./tests/config/install.sh /etc/clickhouse-server /etc/clickhouse-client --fast-test",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml /etc/clickhouse-server/users.d",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/allow-nullable-key.xml /etc/clickhouse-server/config.d",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/max_server_memory_usage_to_ram_ratio.xml /etc/clickhouse-server/config.d",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/core.xml /etc/clickhouse-server/config.d",
            f"{ch_path}/clickhouse-server --version",
            f"./ci/jobs/scripts/ast_buzz_fuzzer/generate-test-j2.py --path ./tests/queries/0_stateless",
            configure_log_export,
        ]

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

    if res and JobStages.START in stages:
        step_name = "Start ClickHouse Server"
        print(step_name)

        def start():
            res = CH.start() and CH.wait_ready()
            if res:
                if "asan" not in info.job_name:
                    print("Attaching gdb")
                    res = res and CH.attach_gdb()
                else:
                    print("Skipping gdb attachment for asan build")
            if res and not Info().is_local_run:
                if not CH.start_log_exports(stop_watch.start_time):
                    info.add_workflow_report_message(
                        "WARNING: Failed to start log export"
                    )
                    print("Failed to start log export")
            return res

        results.append(
            Result.from_commands_run(
                name=step_name,
                command=start,
            )
        )
        res = results[-1].is_ok()

    test_result = []
    if res and JobStages.TEST in stages:

        def run_fuzzer():
            if fuzzer_name == "AST Fuzzer":
                # Get all SQL files and join their paths with spaces
                sql_files = [
                    str(p)
                    for p in sorted(Path("./tests/queries/0_stateless").glob("*.sql"))
                    if p.is_file()
                ]
                # TODO: how do i fit all files?
                queries_files = " ".join(sql_files[:1000])
                fuzzer_args = f"--query-fuzzer-runs=1000 --create-query-fuzzer-runs=50 --queries-file {queries_files}"  # {new_tests_opt}
            elif fuzzer_name == "BuzzHouse":
                Path("fuzzer_out.sql").touch()
                Path("fuzz.json").touch()
                fuzzer_output_sql_file = Path("fuzzer_out.sql").resolve()
                buzzhouse_config_file = Path("fuzz.json").resolve()
                with buzzhouse_config_file.open("w") as f:
                    f.write(
                        json.dumps(
                            {
                                "db_file_path": "/var/lib/clickhouse/user_files",
                                "log_path": str(fuzzer_output_sql_file),
                                "seed": 0,
                                "read_log": False,
                                "use_dump_table_oracle": False,
                                "time_to_run": 180,
                            }
                        )
                    )
                fuzzer_args = f"--buzz-house-config={buzzhouse_config_file}"
            else:
                print(
                    f"Fuzzer {fuzzer_name} unknown, provide either AST Fuzzer or BuzzHouse"
                )
                return False

            exit_code = Shell.run(
                command=f"timeout --verbose --signal TERM --kill-after=5m --preserve-status 2m clickhouse-client --max_memory_usage_in_client=1000000000 --receive_timeout=10 --receive_data_timeout_ms=10000 --stacktrace {fuzzer_args}",
                verbose=True,
                log_file=f"{temp_path}/fuzzer.log",
                executable="/usr/bin/bash",
                no_stdout=True,
            )
            print(f"Fuzzer exit code: {exit_code}")
            return True if exit_code in (0, 143) else False

        results.append(
            Result.from_commands_run(
                name="Fuzzing",
                command=run_fuzzer,
            )
        )
        test_result = results[-1]
        res = results[-1].is_ok()

    CH.terminate()
    if not info.is_local_run:
        CH.stop_log_exports()

    if test_result and JobStages.CHECK_ERRORS in stages:
        print("Check fatal errors")
        sw_ = Utils.Stopwatch()
        results.append(
            Result.create_from(
                name="Check errors",
                results=CH.check_fatal_messeges_in_logs(),
                status=Result.Status.SUCCESS,
                stopwatch=sw_,
            )
        )
        test_result.extend_sub_results(results[-1].results)
        results[-1].results = []
        if not results[-1].is_ok():
            results[-1].set_info("Found errors added into Tests results")

    if JobStages.COLLECT_LOGS in stages:
        print("Collect logs")

        def collect_logs():
            CH.prepare_logs(all=test_result and not test_result.is_ok())

        results.append(
            Result.from_commands_run(
                name="Collect logs",
                command=collect_logs,
            )
        )
        results[-1].results = CH.extra_tests_results

    run_result = Result.create_from(results=results)
    run_result.complete_job()
    # paths = {
    #     "report.html": workspace_path / "report.html",
    #     "core.zst": workspace_path / "core.zst",
    #     "dmesg.log": workspace_path / "dmesg.log",
    #     "fatal.log": workspace_path / "fatal.log",
    #     "stderr.log": workspace_path / "stderr.log",
    # }
    #
    # compressed_server_log_path = workspace_path / "server.log.zst"
    # if compressed_server_log_path.exists():
    #     paths["server.log.zst"] = compressed_server_log_path
    # else:
    #     # The script can fail before the invocation of `zstd`, but we are still interested in its log:
    #     not_compressed_server_log_path = workspace_path / "server.log"
    #     if not_compressed_server_log_path.exists():
    #         paths["server.log"] = not_compressed_server_log_path
    #
    # # Same idea but with the fuzzer log
    # compressed_fuzzer_log_path = workspace_path / "fuzzer.log.zst"
    # if compressed_fuzzer_log_path.exists():
    #     paths["fuzzer.log.zst"] = compressed_fuzzer_log_path
    # else:
    #     not_compressed_fuzzer_log_path = workspace_path / "fuzzer.log"
    #     if not_compressed_fuzzer_log_path.exists():
    #         paths["fuzzer.log"] = not_compressed_fuzzer_log_path
    #
    # # Same idea but with the fuzzer output SQL
    # compressed_fuzzer_output_sql_path = workspace_path / "fuzzer_out.sql.zst"
    # if compressed_fuzzer_output_sql_path.exists():
    #     paths["fuzzer_out.sql.zst"] = compressed_fuzzer_output_sql_path
    # else:
    #     not_compressed_fuzzer_output_sql_path = workspace_path / "fuzzer_out.sql"
    #     if not_compressed_fuzzer_output_sql_path.exists():
    #         paths["fuzzer_out.sql"] = not_compressed_fuzzer_output_sql_path
    #
    # # Try to get status message saved by the fuzzer
    # try:
    #     with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
    #         status = status_f.readline().rstrip("\n")
    #
    #     with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
    #         description = desc_f.readline().rstrip("\n")
    # except:
    #     status = FAILURE
    #     description = "Task failed: $?=" + str(retcode)
    #
    # test_result = TestResult(description, OK)
    # if "fail" in status:
    #     test_result.status = FAIL
    #
    # JobReport(
    #     description=description,
    #     test_results=[test_result],
    #     status=status,
    #     start_time=stopwatch.start_time_str,
    #     duration=stopwatch.duration_seconds,
    #     # test generates its own report.html
    #     additional_files=[v for _, v in paths.items() if Path(v).is_file()],
    # ).dump()
    #
    # logging.info("Result: '%s', '%s'", status, description)
    # if status != SUCCESS:
    #     sys.exit(1)


if __name__ == "__main__":
    main()
