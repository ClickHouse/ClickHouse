#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from multiprocessing import cpu_count
from subprocess import Popen, call, check_output, STDOUT, PIPE
import os
import argparse
import logging
import time
import random


def get_options(i, upgrade_check):
    options = []
    client_options = []
    if i > 0:
        options.append("--order=random")

    if i % 3 == 2 and not upgrade_check:
        options.append(
            '''--db-engine="Replicated('/test/db/test_{}', 's1', 'r1')"'''.format(i)
        )
        client_options.append("allow_experimental_database_replicated=1")

    # If database name is not specified, new database is created for each functional test.
    # Run some threads with one database for all tests.
    if i % 2 == 1:
        options.append(" --database=test_{}".format(i))

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

    if i % 5 == 1:
        client_options.append("memory_tracker_fault_probability=0.001")

    if i % 2 == 1 and not upgrade_check:
        client_options.append("group_by_use_nulls=1")

    # 12 % 3 == 0, so it's Atomic database
    if i == 12 and not upgrade_check:
        client_options.append("implicit_transaction=1")
        client_options.append("throw_on_unsupported_query_inside_transaction=0")

    if client_options:
        options.append(" --client-option " + " ".join(client_options))

    return " ".join(options)


def run_func_test(
    cmd,
    output_prefix,
    num_processes,
    skip_tests_option,
    global_time_limit,
    upgrade_check,
):
    upgrade_check_option = "--upgrade-check" if upgrade_check else ""
    global_time_limit_option = ""
    if global_time_limit:
        global_time_limit_option = "--global_time_limit={}".format(global_time_limit)

    output_paths = [
        os.path.join(output_prefix, "stress_test_run_{}.txt".format(i))
        for i in range(num_processes)
    ]
    pipes = []
    for i, path in enumerate(output_paths):
        f = open(path, "w")
        full_command = "{} {} {} {} {}".format(
            cmd,
            get_options(i, upgrade_check),
            global_time_limit_option,
            skip_tests_option,
            upgrade_check_option,
        )
        logging.info("Run func tests '%s'", full_command)
        p = Popen(full_command, shell=True, stdout=f, stderr=f)
        pipes.append(p)
        time.sleep(0.5)
    return pipes


def compress_stress_logs(output_path, files_prefix):
    cmd = f"cd {output_path} && tar --zstd --create --file=stress_run_logs.tar.zst {files_prefix}* && rm {files_prefix}*"
    check_output(cmd, shell=True)


def call_with_retry(query, timeout=30, retry_count=5):
    for i in range(retry_count):
        code = call(query, shell=True, stderr=STDOUT, timeout=timeout)
        if code != 0:
            time.sleep(i)
        else:
            break


def make_query_command(query):
    return f"""clickhouse client -q "{query}" --max_untracked_memory=1Gi --memory_profiler_step=1Gi --max_memory_usage_for_user=0"""


def prepare_for_hung_check(drop_databases):
    # FIXME this function should not exist, but...

    # We attach gdb to clickhouse-server before running tests
    # to print stacktraces of all crashes even if clickhouse cannot print it for some reason.
    # However, it obstruct checking for hung queries.
    logging.info("Will terminate gdb (if any)")
    call_with_retry("kill -TERM $(pidof gdb)")

    # ThreadFuzzer significantly slows down server and causes false-positive hung check failures
    call_with_retry("clickhouse client -q 'SYSTEM STOP THREAD FUZZER'")

    call_with_retry(make_query_command("SELECT 1 FORMAT Null"))

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
    # It's query from 01232_preparing_sets_race_condition_long, it may take up to 1000 seconds in slow builds
    call_with_retry(
        make_query_command("KILL QUERY WHERE query LIKE 'insert into tableB select %'")
    )
    # Long query from 00084_external_agregation
    call_with_retry(
        make_query_command(
            "KILL QUERY WHERE query LIKE 'SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u %'"
        )
    )
    # Long query from 02136_kill_scalar_queries
    call_with_retry(
        make_query_command(
            "KILL QUERY WHERE query LIKE 'SELECT (SELECT number FROM system.numbers WHERE number = 1000000000000)%'"
        )
    )

    if drop_databases:
        for i in range(5):
            try:
                # Here we try to drop all databases in async mode. If some queries really hung, than drop will hung too.
                # Otherwise we will get rid of queries which wait for background pool. It can take a long time on slow builds (more than 900 seconds).
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
                    Popen(command, shell=True)
                break
            except Exception as ex:
                logging.error(
                    "Failed to SHOW or DROP databasese, will retry %s", str(ex)
                )
                time.sleep(i)
        else:
            raise Exception(
                "Cannot drop databases after stress tests. Probably server consumed too much memory and cannot execute simple queries"
            )

    # Wait for last queries to finish if any, not longer than 300 seconds
    call(
        make_query_command(
            """
    select sleepEachRow((
        select maxOrDefault(300 - elapsed) + 1
        from system.processes
        where query not like '%from system.processes%' and elapsed < 300
    ) / 300)
    from numbers(300)
    format Null
    """
        ),
        shell=True,
        stderr=STDOUT,
        timeout=330,
    )

    # Even if all clickhouse-test processes are finished, there are probably some sh scripts,
    # which still run some new queries. Let's ignore them.
    try:
        query = """clickhouse client -q "SELECT count() FROM system.processes where elapsed > 300" """
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


def is_ubsan_build():
    try:
        query = """clickhouse client -q "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'" """
        output = (
            check_output(query, shell=True, stderr=STDOUT, timeout=30)
            .decode("utf-8")
            .strip()
        )
        return "-fsanitize=undefined" in output
    except Exception as e:
        logging.info("Failed to get build flags: %s", str(e))
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(
        description="ClickHouse script for running stresstest"
    )
    parser.add_argument("--test-cmd", default="/usr/bin/clickhouse-test")
    parser.add_argument("--skip-func-tests", default="")
    parser.add_argument("--server-log-folder", default="/var/log/clickhouse-server")
    parser.add_argument("--output-folder")
    parser.add_argument("--global-time-limit", type=int, default=1800)
    parser.add_argument("--num-parallel", type=int, default=cpu_count())
    parser.add_argument("--upgrade-check", action="store_true")
    parser.add_argument("--hung-check", action="store_true", default=False)
    # make sense only for hung check
    parser.add_argument("--drop-databases", action="store_true", default=False)

    args = parser.parse_args()
    if args.drop_databases and not args.hung_check:
        raise Exception("--drop-databases only used in hung check (--hung-check)")

    # FIXME Hung check with ubsan is temporarily disabled due to https://github.com/ClickHouse/ClickHouse/issues/45372
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
            logging.error("Failed to prepare for hung check %s", str(ex))
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
        hung_check_log = os.path.join(args.output_folder, "hung_check.log")
        tee = Popen(["/usr/bin/tee", hung_check_log], stdin=PIPE)
        res = call(cmd, shell=True, stdout=tee.stdin, stderr=STDOUT)
        if tee.stdin is not None:
            tee.stdin.close()
        if res != 0 and have_long_running_queries and not suppress_hung_check:
            logging.info("Hung check failed with exit code %d", res)
        else:
            hung_check_status = "No queries hung\tOK\t\\N\t\n"
            with open(
                os.path.join(args.output_folder, "test_results.tsv"), "w+"
            ) as results:
                results.write(hung_check_status)
            os.remove(hung_check_log)

    logging.info("Stress test finished")
