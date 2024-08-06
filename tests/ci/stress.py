#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This script is used in docker images for stress tests and upgrade tests"""
import argparse
import logging
import random
import time
from multiprocessing import cpu_count
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen, call, check_output
from typing import List


def get_options(i: int, upgrade_check: bool) -> str:
    options = []
    client_options = []
    if i > 0:
        options.append("--order=random")

    if i % 3 == 2 and not upgrade_check:
        options.append(f'''--db-engine="Replicated('/test/db/test_{i}', 's1', 'r1')"''')
        client_options.append("enable_deflate_qpl_codec=1")
        client_options.append("enable_zstd_qat_codec=1")

    # If database name is not specified, new database is created for each functional test.
    # Run some threads with one database for all tests.
    if i % 2 == 1:
        options.append(f" --database=test_{i}")

    if i % 3 == 1:
        client_options.append("join_use_nulls=1")

    if i % 2 == 1:
        join_alg_num = i // 2
        if join_alg_num % 5 == 0:
            client_options.append("join_algorithm='parallel_hash'")
        if join_alg_num % 5 == 1:
            client_options.append("join_algorithm='partial_merge'")
        if join_alg_num % 5 == 2:
            client_options.append("join_algorithm='full_sorting_merge'")
        if join_alg_num % 5 == 3 and not upgrade_check:
            # Some crashes are not fixed in 23.2 yet, so ignore the setting in Upgrade check
            client_options.append("join_algorithm='grace_hash'")
        if join_alg_num % 5 == 4:
            client_options.append("join_algorithm='auto'")
            client_options.append("max_rows_in_join=1000")

    if i > 0 and random.random() < 1 / 3:
        client_options.append("use_query_cache=1")
        client_options.append("query_cache_nondeterministic_function_handling='ignore'")
        client_options.append("query_cache_system_table_handling='ignore'")

    if i % 5 == 1:
        client_options.append("memory_tracker_fault_probability=0.001")

    if i % 5 == 1:
        client_options.append(
            "merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.05"
        )

    if i % 2 == 1 and not upgrade_check:
        client_options.append("group_by_use_nulls=1")

    # 12 % 3 == 0, so it's Atomic database
    if i == 12 and not upgrade_check:
        client_options.append("implicit_transaction=1")
        client_options.append("throw_on_unsupported_query_inside_transaction=0")

    if random.random() < 0.1:
        client_options.append("optimize_trivial_approximate_count_query=1")

    if random.random() < 0.3:
        client_options.append(f"http_make_head_request={random.randint(0, 1)}")

    # TODO: After release 24.3 use ignore_drop_queries_probability for both
    #       stress test and upgrade check
    if not upgrade_check:
        client_options.append("ignore_drop_queries_probability=0.5")

    if random.random() < 0.2:
        client_options.append("allow_experimental_parallel_reading_from_replicas=1")
        client_options.append("max_parallel_replicas=3")
        client_options.append("cluster_for_parallel_replicas='parallel_replicas'")
        client_options.append("parallel_replicas_for_non_replicated_merge_tree=1")

    if client_options:
        options.append(" --client-option " + " ".join(client_options))

    return " ".join(options)


def run_func_test(
    cmd: str,
    output_prefix: Path,
    num_processes: int,
    skip_tests_option: str,
    global_time_limit: int,
    upgrade_check: bool,
) -> List[Popen]:
    upgrade_check_option = "--upgrade-check" if upgrade_check else ""
    global_time_limit_option = (
        f"--global_time_limit={global_time_limit}" if global_time_limit else ""
    )

    output_paths = [
        output_prefix / f"stress_test_run_{i}.txt" for i in range(num_processes)
    ]
    pipes = []
    for i, path in enumerate(output_paths):
        with open(path, "w", encoding="utf-8") as op:
            full_command = (
                f"{cmd} {get_options(i, upgrade_check)} {global_time_limit_option} "
                f"{skip_tests_option} {upgrade_check_option}"
            )
            logging.info("Run func tests '%s'", full_command)
            # pylint:disable-next=consider-using-with
            pipes.append(Popen(full_command, shell=True, stdout=op, stderr=op))
            time.sleep(0.5)
    return pipes


def compress_stress_logs(output_path: Path, files_prefix: str) -> None:
    cmd = (
        f"cd {output_path} && tar --zstd --create --file=stress_run_logs.tar.zst "
        f"{files_prefix}* && rm {files_prefix}*"
    )
    check_output(cmd, shell=True)


def call_with_retry(query: str, timeout: int = 30, retry_count: int = 5) -> None:
    logging.info("Running command: %s", str(query))
    for i in range(retry_count):
        code = call(query, shell=True, stderr=STDOUT, timeout=timeout)
        if code != 0:
            logging.info("Command returend %s, retrying", str(code))
            time.sleep(i)
        else:
            break


def make_query_command(query: str) -> str:
    return (
        f'clickhouse client -q "{query}" --max_untracked_memory=1Gi '
        "--memory_profiler_step=1Gi --max_memory_usage_for_user=0 --max_memory_usage_in_client=1000000000"
    )


def prepare_for_hung_check(drop_databases: bool) -> bool:
    # FIXME this function should not exist, but...

    # We attach gdb to clickhouse-server before running tests
    # to print stacktraces of all crashes even if clickhouse cannot print it for some reason.
    # However, it obstructs checking for hung queries.
    logging.info("Will terminate gdb (if any)")
    call_with_retry("kill -TERM $(pidof gdb)")
    call_with_retry(
        "timeout 50s tail --pid=$(pidof gdb) -f /dev/null || kill -9 $(pidof gdb) ||:",
        timeout=60,
    )
    # Sometimes there is a message `Child process was stopped by signal 19` in logs after stopping gdb
    call_with_retry(
        "kill -CONT $(cat /var/run/clickhouse-server/clickhouse-server.pid) && clickhouse client -q 'SELECT 1 FORMAT Null'"
    )

    # ThreadFuzzer significantly slows down server and causes false-positive hung check failures
    call_with_retry(make_query_command("SYSTEM STOP THREAD FUZZER"))
    # Some tests execute SYSTEM STOP MERGES or similar queries.
    # It may cause some ALTERs to hang.
    # Possibly we should fix tests and forbid to use such queries without specifying table.
    call_with_retry(make_query_command("SYSTEM START MERGES"))
    call_with_retry(make_query_command("SYSTEM START DISTRIBUTED SENDS"))
    call_with_retry(make_query_command("SYSTEM START TTL MERGES"))
    call_with_retry(make_query_command("SYSTEM START MOVES"))
    call_with_retry(make_query_command("SYSTEM START FETCHES"))
    call_with_retry(make_query_command("SYSTEM START REPLICATED SENDS"))
    call_with_retry(make_query_command("SYSTEM START REPLICATION QUEUES"))
    call_with_retry(make_query_command("SYSTEM DROP MARK CACHE"))

    # Issue #21004, live views are experimental, so let's just suppress it
    call_with_retry(make_query_command("KILL QUERY WHERE upper(query) LIKE 'WATCH %'"))

    # Kill other queries which known to be slow
    # It's query from 01232_preparing_sets_race_condition_long,
    # it may take up to 1000 seconds in slow builds
    call_with_retry(
        make_query_command("KILL QUERY WHERE query LIKE 'insert into tableB select %'")
    )
    # Long query from 00084_external_agregation
    call_with_retry(
        make_query_command(
            "KILL QUERY WHERE query LIKE 'SELECT URL, uniq(SearchPhrase) AS u FROM "
            "test.hits GROUP BY URL ORDER BY u %'"
        )
    )
    # Long query from 02136_kill_scalar_queries
    call_with_retry(
        make_query_command(
            "KILL QUERY WHERE query LIKE "
            "'SELECT (SELECT number FROM system.numbers WHERE number = 1000000000000)%'"
        )
    )

    if drop_databases:
        for i in range(5):
            try:
                # Here we try to drop all databases in async mode.
                # If some queries really hung, than drop will hung too.
                # Otherwise we will get rid of queries which wait for background pool.
                # It can take a long time on slow builds (more than 900 seconds).
                #
                # Also specify max_untracked_memory to allow 1GiB of memory to overcommit.
                databases = (
                    check_output(
                        make_query_command("SHOW DATABASES"), shell=True, timeout=30
                    )
                    .decode("utf-8")
                    .strip()
                    .split()
                )
                for db in databases:
                    if db == "system":
                        continue
                    command = make_query_command(f"DETACH DATABASE {db}")
                    # we don't wait for drop
                    # pylint:disable-next=consider-using-with
                    Popen(command, shell=True)
                break
            except Exception as ex:
                logging.error(
                    "Failed to SHOW or DROP databasese, will retry %s", str(ex)
                )
                time.sleep(i)
        else:
            raise RuntimeError(
                "Cannot drop databases after stress tests. Probably server consumed "
                "too much memory and cannot execute simple queries"
            )

    # Wait for last queries to finish if any, not longer than 300 seconds
    call(
        make_query_command(
            """
    SELECT sleepEachRow((
        SELECT maxOrDefault(300 - elapsed) + 1
        FROM system.processes
        WHERE query NOT LIKE '%FROM system.processes%' AND elapsed < 300
    ) / 300)
    FROM numbers(300)
    FORMAT Null
    SETTINGS function_sleep_max_microseconds_per_block = 0
    """
        ),
        shell=True,
        stderr=STDOUT,
        timeout=330,
    )

    # Even if all clickhouse-test processes are finished, there are probably some sh scripts,
    # which still run some new queries. Let's ignore them.
    try:
        query = 'clickhouse client -q "SELECT count() FROM system.processes where elapsed > 300" '
        output = (
            check_output(query, shell=True, stderr=STDOUT, timeout=30)
            .decode("utf-8")
            .strip()
        )
        if int(output) == 0:
            return False
    except:
        pass
    return True


def is_ubsan_build() -> bool:
    try:
        query = (
            'clickhouse client -q "SELECT value FROM system.build_options '
            "WHERE name = 'CXX_FLAGS'\" "
        )
        output = (
            check_output(query, shell=True, stderr=STDOUT, timeout=30)
            .decode("utf-8")
            .strip()
        )
        return "-fsanitize=undefined" in output
    except Exception as e:
        logging.info("Failed to get build flags: %s", str(e))
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ClickHouse script for running stresstest"
    )
    parser.add_argument("--test-cmd", default="/usr/bin/clickhouse-test")
    parser.add_argument("--skip-func-tests", default="")
    parser.add_argument(
        "--server-log-folder", default="/var/log/clickhouse-server", type=Path
    )
    parser.add_argument("--output-folder", type=Path)
    parser.add_argument("--global-time-limit", type=int, default=1800)
    parser.add_argument("--num-parallel", type=int, default=cpu_count())
    parser.add_argument("--upgrade-check", action="store_true")
    parser.add_argument("--hung-check", action="store_true", default=False)
    # make sense only for hung check
    parser.add_argument("--drop-databases", action="store_true", default=False)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()

    if args.drop_databases and not args.hung_check:
        raise argparse.ArgumentTypeError(
            "--drop-databases only used in hung check (--hung-check)"
        )

    # FIXME Hung check with ubsan is temporarily disabled due to
    # https://github.com/ClickHouse/ClickHouse/issues/45372
    suppress_hung_check = is_ubsan_build()

    func_pipes = []
    func_pipes = run_func_test(
        args.test_cmd,
        args.output_folder,
        args.num_parallel,
        args.skip_func_tests,
        args.global_time_limit,
        args.upgrade_check,
    )

    logging.info("Will wait functests to finish")
    while True:
        retcodes = []
        for p in func_pipes:
            if p.poll() is not None:
                retcodes.append(p.returncode)
        if len(retcodes) == len(func_pipes):
            break
        logging.info("Finished %s from %s processes", len(retcodes), len(func_pipes))
        time.sleep(5)

    logging.info("All processes finished")

    logging.info("Compressing stress logs")
    compress_stress_logs(args.output_folder, "stress_test_run_")
    logging.info("Logs compressed")

    if args.hung_check:
        try:
            have_long_running_queries = prepare_for_hung_check(args.drop_databases)
        except Exception as ex:
            have_long_running_queries = True
            logging.error("Failed to prepare for hung check: %s", str(ex))
        logging.info("Checking if some queries hung")
        cmd = " ".join(
            [
                args.test_cmd,
                # Do not track memory allocations up to 1Gi,
                # this will allow to ignore server memory limit (max_server_memory_usage) for this query.
                #
                # NOTE: memory_profiler_step should be also adjusted, because:
                #
                #     untracked_memory_limit = min(settings.max_untracked_memory, settings.memory_profiler_step)
                #
                # NOTE: that if there will be queries with GROUP BY, this trick
                # will not work due to CurrentMemoryTracker::check() from
                # Aggregator code.
                # But right now it should work, since neither hung check, nor 00001_select_1 has GROUP BY.
                "--client-option",
                "max_untracked_memory=1Gi",
                "max_memory_usage_for_user=0",
                "memory_profiler_step=1Gi",
                # Use system database to avoid CREATE/DROP DATABASE queries
                "--database=system",
                "--hung-check",
                "--report-logs-stats",
                "00001_select_1",
            ]
        )
        hung_check_log = args.output_folder / "hung_check.log"  # type: Path
        with Popen(["/usr/bin/tee", hung_check_log], stdin=PIPE) as tee:
            res = call(cmd, shell=True, stdout=tee.stdin, stderr=STDOUT, timeout=600)
            if tee.stdin is not None:
                tee.stdin.close()
        if res != 0 and have_long_running_queries and not suppress_hung_check:
            logging.info("Hung check failed with exit code %d", res)
        else:
            hung_check_status = "No queries hung\tOK\t\\N\t\n"
            with open(
                args.output_folder / "test_results.tsv", "w+", encoding="utf-8"
            ) as results:
                results.write(hung_check_status)
                hung_check_log.unlink()

    logging.info("Stress test finished")


if __name__ == "__main__":
    main()
