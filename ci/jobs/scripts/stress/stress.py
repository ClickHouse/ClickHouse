#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This script is used in docker images for stress tests and upgrade tests"""
import argparse
import logging
import os
import random
import shlex
import shutil
import signal
import subprocess
import time
import threading
from multiprocessing import cpu_count
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen, call, check_output
from typing import List, Optional

# Failpoint that delays every background mutation by a bounded random amount.
MUTATION_DELAY_FAILPOINT = "mutate_task_random_sleep_in_prepare"

# GNU `tar` exit statuses: 0 - success, 1 - some files differ (a file changed
# or shrank while it was being read), 2 and above - a fatal error.
TAR_EXIT_DIFFERS = 1


class ServerDied(Exception):
    pass


def escape_tsv_info(text: str) -> str:
    # Escape CR alongside the other separators rather than dropping it.
    # Bare CR is emitted by tools like `apt-get`/`dpkg` to overwrite
    # progress frames in place, and the hung-check path embeds dpkg
    # output verbatim when `clickhouse-test --capture-client-stacktrace`
    # installs `lldb` on the fly. Left raw in the TSV, those CRs are
    # turned back into LF by universal-newlines mode at read time and
    # fragment the row. Encoding them as `\r` keeps the diagnostic
    # detail intact for the unescape pass in `read_test_results`.
    return (
        text.replace("\0", "\\0")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


class RandomDisruptor:
    """Background thread that randomly kills queries, client processes and mutations, and
    briefly stops background operations such as merges, during stress tests.

    This helps test that queries and mutations are cancelled correctly, handles
    scenarios where the client unexpectedly disconnects (issue #39803), and exercises
    operations that run while one of the background activities in `_SYSTEM_STATEMENT_PAIRS`
    is unavailable.
    """

    # Subprocess caps for one loop iteration: a SELECT to pick a victim, then the kill.
    _SELECT_TIMEOUT = 5
    _KILL_QUERY_TIMEOUT = 5
    _KILL_MUTATION_TIMEOUT = 15
    # A `SYSTEM STOP/START ...` without a table walks every database and table, so it needs a
    # longer cap than the kills above. Deliberately less generous than the same statements get
    # in `prepare_for_hung_check` (30s and five retries): here a slow one is worth giving up
    # on, since the next iteration comes around anyway and that teardown is the backstop.
    _SYSTEM_STATEMENT_TIMEOUT = 15
    _SYSTEM_STATEMENT_PAUSE_MIN = 1.0
    _SYSTEM_STATEMENT_PAUSE_MAX = 10.0
    # Retries for the restart only: a failed stop is harmless (the pause just does not
    # happen), but a start that never lands leaves the operation stopped until
    # `prepare_for_hung_check` runs at the very end of the run. Skipped once shutdown is
    # requested, since that teardown is about to retry the same statement anyway.
    _SYSTEM_STATEMENT_RESTART_ATTEMPTS = 3
    _SYSTEM_STATEMENT_RESTART_BACKOFF = 2.0
    # (stop, start) pairs the pause branch picks from uniformly. Only add a pair whose start
    # fully undoes its stop and that `prepare_for_hung_check` also restarts, since that
    # teardown is the last-resort net for a start this thread never managed to run.
    _SYSTEM_STATEMENT_PAIRS = (
        ("STOP MERGES", "START MERGES"),
        ("STOP TTL MERGES", "START TTL MERGES"),
        ("STOP MOVES", "START MOVES"),
        ("STOP VIEWS", "START VIEWS"),
        ("PAUSE VIEWS", "START VIEWS"),
    )
    # Longest an iteration can run, plus margin, so stop() outlasts one of them. The pause
    # branch is the stop, the wait and the start back to back; on shutdown the wait collapses,
    # but bound it for the case where stop() arrives just before the pause begins.
    _JOIN_TIMEOUT = (
        max(
            _SELECT_TIMEOUT + _KILL_MUTATION_TIMEOUT,
            _SYSTEM_STATEMENT_TIMEOUT
            + _SYSTEM_STATEMENT_PAUSE_MAX
            + _SYSTEM_STATEMENT_RESTART_ATTEMPTS * _SYSTEM_STATEMENT_TIMEOUT
            + (_SYSTEM_STATEMENT_RESTART_ATTEMPTS - 1) * _SYSTEM_STATEMENT_RESTART_BACKOFF,
        )
        + 5
    )

    def __init__(self, interval: float = 3.0):
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._interval = interval

    def _kill_random_query(self) -> None:
        """Select a random query from system.processes and kill it."""
        try:
            # Get a random query_id, excluding our own queries and system queries
            result = check_output(
                "clickhouse client --receive_timeout=5 -q \""
                "SELECT query_id FROM system.processes "
                "WHERE query NOT LIKE '%system.processes%' "
                "AND query NOT LIKE '%KILL QUERY%' "
                "AND elapsed > 0.1 "
                "ORDER BY rand() LIMIT 1\" 2>/dev/null",
                shell=True,
                timeout=self._SELECT_TIMEOUT,
            )
            # Strip only the row delimiter: a query_id may legitimately start or end with
            # a space, and TSV escapes the separators, so a raw newline is always the
            # delimiter rather than part of the value.
            query_id = result.decode("utf-8").removesuffix("\n")
            if query_id:
                # Shutdown may have been requested while the SELECT above was running.
                if self._stop_event.is_set():
                    return
                logging.info("Killing random query: %s", query_id)
                # A query_id is arbitrary text (tests pass --query_id), so pass it as a query
                # parameter instead of interpolating it: parameters are read with
                # deserializeTextEscaped, the exact inverse of the TSV escaping above.
                returncode = call(
                    [
                        "clickhouse",
                        "client",
                        "--receive_timeout=5",
                        "--param_query_id",
                        query_id,
                        "-q",
                        "KILL QUERY WHERE query_id = {query_id:String} ASYNC",
                    ],
                    stderr=subprocess.DEVNULL,
                    timeout=self._KILL_QUERY_TIMEOUT,
                )
                # Both expected outcomes exit 0: a matched kill prints a kill_status row,
                # a query that already finished prints nothing. Non-zero means the command
                # itself is broken, which would silently disable the disruptor.
                if returncode:
                    logging.warning(
                        "KILL QUERY exited %s for query_id %s", returncode, query_id
                    )
        except subprocess.TimeoutExpired as e:
            # Expected while the server is loaded, and far too frequent to report louder.
            logging.debug("Random query kill timed out: %s", e)
        except Exception as e:
            # Anything else means the disruptor itself is misbehaving.
            logging.warning("Random query kill failed: %s: %s", type(e).__name__, e)

    def _kill_random_client(self) -> None:
        """Kill a random clickhouse-client process."""
        try:
            # Get list of clickhouse-test child processes (clickhouse client)
            result = check_output(
                "pgrep -f 'clickhouse-client|clickhouse client' 2>/dev/null || true",
                shell=True,
                timeout=5,
            )
            pids = [p.strip() for p in result.decode("utf-8").strip().split("\n") if p.strip()]
            if pids:
                # Pick a random pid and kill it
                pid = random.choice(pids)
                logging.info("Killing random client process: %s", pid)
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except (ProcessLookupError, ValueError):
                    pass  # Process already gone
        except subprocess.TimeoutExpired as e:
            logging.debug("Random client kill timed out: %s", e)
        except Exception as e:
            logging.warning("Random client kill failed: %s: %s", type(e).__name__, e)

    def _kill_random_mutation(self) -> None:
        """Select a random unfinished mutation and kill it."""
        try:
            # Skip mutations already killed: KILL MUTATION is not instantaneous, a mutation
            # stays visible with is_killed=1 and is_done=0 while it finalizes.
            result = check_output(
                "clickhouse client --receive_timeout=5 -q \""
                "SELECT mutation_id, database, table "
                "FROM system.mutations "
                "WHERE NOT is_done AND NOT is_killed "
                "ORDER BY rand() LIMIT 1\" 2>/dev/null",
                shell=True,
                timeout=self._SELECT_TIMEOUT,
            )
            # Strip only the row delimiter, so a name that starts or ends with a space
            # survives; TSV escapes the separators, so a raw newline is the delimiter.
            line = result.decode("utf-8").removesuffix("\n")
            if line:
                mutation_id, db, table = line.split("\t")
                # Shutdown may have been requested while the SELECT above was running.
                if self._stop_event.is_set():
                    return
                logging.info("Killing random mutation: %s on %s.%s", mutation_id, db, table)
                # Names are arbitrary text, so pass them as query parameters instead of
                # interpolating: parameters are read with deserializeTextEscaped, the exact
                # inverse of the TSV escaping above.
                # KILL MUTATION is ASYNC by default (ASTKillQueryQuery::sync = false), so it
                # returns a kill_status row without waiting for the mutation to finalize. The
                # subprocess cap stays above --receive_timeout so the client's own timeout is
                # the one that governs.
                returncode = call(
                    [
                        "clickhouse",
                        "client",
                        "--receive_timeout=10",
                        "--param_database",
                        db,
                        "--param_table",
                        table,
                        "--param_mutation_id",
                        mutation_id,
                        "-q",
                        "KILL MUTATION WHERE database = {database:String} "
                        "AND table = {table:String} AND mutation_id = {mutation_id:String}",
                    ],
                    stderr=subprocess.DEVNULL,
                    timeout=self._KILL_MUTATION_TIMEOUT,
                )
                # A mutation that finished or a table dropped meanwhile still exits 0, so a
                # non-zero code means the command itself is broken.
                if returncode:
                    logging.warning(
                        "KILL MUTATION exited %s for %s on %s.%s",
                        returncode,
                        mutation_id,
                        db,
                        table,
                    )
        except subprocess.TimeoutExpired as e:
            logging.debug("Random mutation kill timed out: %s", e)
        except Exception as e:
            logging.warning("Random mutation kill failed: %s: %s", type(e).__name__, e)

    def _run_system_statement(self, statement: str) -> bool:
        """Run `SYSTEM <statement>`, reporting whether it succeeded."""
        query = f"SYSTEM {statement}"
        try:
            # Keep the subprocess cap above --receive_timeout so the client's own timeout is
            # the one that governs, as in the mutation kill above.
            returncode = call(
                ["clickhouse", "client", "--receive_timeout=10", "-q", query],
                stderr=subprocess.DEVNULL,
                timeout=self._SYSTEM_STATEMENT_TIMEOUT,
            )
            if returncode:
                logging.warning("%s exited %s", query, returncode)
            return returncode == 0
        except subprocess.TimeoutExpired as e:
            logging.debug("%s timed out: %s", query, e)
            return False
        except Exception as e:
            logging.warning("%s failed: %s: %s", query, type(e).__name__, e)
            return False

    def _pause_random_operation(self) -> None:
        """Stop a background operation server-wide for a short interval, then start it again."""
        stop_statement, start_statement = random.choice(self._SYSTEM_STATEMENT_PAIRS)
        pause = random.uniform(self._SYSTEM_STATEMENT_PAUSE_MIN, self._SYSTEM_STATEMENT_PAUSE_MAX)
        try:
            if self._run_system_statement(stop_statement):
                logging.info("SYSTEM %s, resuming in %.1fs", stop_statement, pause)
                # Interruptible, so a shutdown request cuts the pause short instead of
                # holding stop() for the full interval.
                self._stop_event.wait(pause)
        finally:
            # Always run the start: on shutdown, and also when the stop above reported a
            # failure, since a client-side timeout does not mean the server skipped the
            # statement. Retried, so one failed attempt does not leave the operation
            # stopped until `prepare_for_hung_check`'s own retry at the very end of the run.
            for attempt in range(1, self._SYSTEM_STATEMENT_RESTART_ATTEMPTS + 1):
                if self._run_system_statement(start_statement):
                    if attempt > 1:
                        logging.info("SYSTEM %s succeeded on attempt %d", start_statement, attempt)
                    break
                attempts_left = self._SYSTEM_STATEMENT_RESTART_ATTEMPTS - attempt
                if not attempts_left or self._stop_event.is_set():
                    logging.error(
                        "Failed to run SYSTEM %s after %d attempt%s%s",
                        start_statement,
                        attempt,
                        "" if attempt == 1 else "s",
                        "" if attempts_left == 0 else " (shutting down, deferring to prepare_for_hung_check)",
                    )
                    break
                # Interruptible, so shutdown does not wait out the full backoff.
                self._stop_event.wait(self._SYSTEM_STATEMENT_RESTART_BACKOFF)

    def _run(self) -> None:
        """Main loop that runs in the background thread."""
        logging.info("Random disruptor started (interval: %.1fs)", self._interval)
        # Picked from uniformly, one per iteration. The pause is the only one whose effect
        # outlives the iteration that started it: it holds a background operation off
        # server-wide for up to `_SYSTEM_STATEMENT_PAUSE_MAX` seconds.
        disruptions = (
            self._kill_random_query,
            self._kill_random_client,
            self._kill_random_mutation,
            self._pause_random_operation,
        )
        while not self._stop_event.is_set():
            random.choice(disruptions)()
            self._stop_event.wait(self._interval)
        logging.info("Random disruptor stopped")

    def start(self) -> None:
        """Start the background disruptor thread."""
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the background disruptor thread."""
        if self._thread is None:
            return
        self._stop_event.set()
        # Outlast one full in-flight iteration: the stop flag is only checked between the
        # SELECT and the kill, so a request arriving just after that check still has to
        # wait out the kill client call. The caller goes on to the hung check and
        # DROP DATABASE, which must not race a disruptor that is still running.
        self._thread.join(timeout=self._JOIN_TIMEOUT)
        if self._thread.is_alive():
            # Keep the handle so a later start() cannot spawn a second disruptor.
            logging.error("Random disruptor did not stop in time")
            return
        self._thread = None


def get_options(i: int, upgrade_check: bool, encrypted_storage: bool) -> str:
    options = []
    client_options = []

    if upgrade_check:
        # Disable settings randomization for upgrade checks to prevent test failures caused by missing settings in old version
        options.append("--no-random-settings")
        options.append("--no-random-merge-tree-settings")

    # The stress test profile constrains enable_analyzer to >= 1 (stress_tests.lib) so neither the
    # AST fuzzer nor a test spends the run on the old interpreter. Send the setting explicitly so the
    # randomized compatibility below cannot revert it: compatibility only rewrites settings that are
    # not `changed`, and a constraint cannot catch that revert because there is no explicit change to
    # check. The profile pins the same value server-side for the queries this does not cover.
    client_options.append("enable_analyzer=1")

    if i > 0:
        options.append("--order=random")

    if i % 3 == 2 and not upgrade_check:
        client_options.extend(
            [
                # For Replicated database
                "distributed_ddl_output_mode=none",
                "database_replicated_always_detach_permanently=1",
            ]
        )
        options.extend(
            [
                "--replicated-database",
                "--database",
                f"test_{i}",
            ]
        )

    # If database name is not specified, new database is created for each functional test.
    # Run some threads with one database for all tests.
    if i % 2 == 1:
        options.append(f" --database=test_{i}")

    if i % 3 == 1:
        client_options.append("join_use_nulls=1")

    if i % 2 == 1:
        # `join_algorithm` accepts a comma-separated priority list: for each query the
        # first applicable algorithm is used. Pick a random subset in random order, so
        # every algorithm meets every worker mode (plain, join_use_nulls, replicated
        # database) and the multi-algorithm fallback paths are covered too.
        join_algorithms = [
            "hash",
            "parallel_hash",
            "partial_merge",
            "full_sorting_merge",
            "grace_hash",
            "auto",
        ]
        if not upgrade_check:
            # The Upgrade check runs the pre-upgrade load against the previous
            # release server, which rejects join_algorithm values it does not
            # know yet and would fail every query, so skip the values that are
            # newer than the previous release.
            join_algorithms += ["parallel_full_sorting_merge", "ie_join"]
        selected = random.sample(
            join_algorithms, k=random.randint(1, len(join_algorithms))
        )
        if selected == ["ie_join"]:
            # ie_join applies only to inequality joins; alone it would fail every plain
            # equality join with NOT_IMPLEMENTED.
            selected.append("hash")
        client_options.append("join_algorithm='{}'".format(",".join(selected)))
        if selected[0] == "auto":
            # The low limit makes auto switch from hash to partial_merge. It is safe
            # only when auto is actually selected: the planner takes the first
            # buildable algorithm from the list, and max_rows_in_join applies to
            # every join implementation, so with e.g. 'hash,auto' the hash join
            # would run under the cap and fail with SET_SIZE_LIMIT_EXCEEDED.
            client_options.append("max_rows_in_join=1000")

    # Rarely enable the query cache; independently, half the time also pin the
    # `*_overflow_mode` settings to 'throw'.
    if i > 0 and random.random() < 1 / 15:
        client_options.append("use_query_cache=1")
        client_options.append("query_cache_nondeterministic_function_handling='ignore'")
        client_options.append("query_cache_system_table_handling='ignore'")
        if random.random() < 1 / 2:
            client_options.append("read_overflow_mode='throw'")
            client_options.append("read_overflow_mode_leaf='throw'")
            client_options.append("group_by_overflow_mode='throw'")
            client_options.append("sort_overflow_mode='throw'")
            client_options.append("result_overflow_mode='throw'")
            client_options.append("timeout_overflow_mode='throw'")
            client_options.append("set_overflow_mode='throw'")
            client_options.append("join_overflow_mode='throw'")
            client_options.append("transfer_overflow_mode='throw'")
            client_options.append("distinct_overflow_mode='throw'")

    if i % 5 == 1:
        client_options.append("memory_tracker_fault_probability=0.001")

    if i % 5 == 1:
        client_options.append(
            "merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.05"
        )

    if i % 2 == 1 and not upgrade_check:
        client_options.append("group_by_use_nulls=1")

    # TODO: Enable implicit_transaction back after the issue with `assertHasValidVersionMetadata` will be fixed:
    # https://play.clickhouse.com/play?user=play&run=1#U0VMRUNUIGNoZWNrX3N0YXJ0X3RpbWUsIGNoZWNrX25hbWUsIHRlc3RfbmFtZSwgcmVwb3J0X3VybApGUk9NIGNoZWNrcwpXSEVSRSAxCiAgICBBTkQgY2hlY2tfc3RhcnRfdGltZSA+PSBub3coKSAtIElOVEVSVkFMIDEwIERBWQogICAgQU5EIChoZWFkX3JlZiA9ICdtYXN0ZXInIEFORCBzdGFydHNXaXRoKGhlYWRfcmVwbywgJ0NsaWNrSG91c2UvJykpCiAgICBBTkQgdGVzdF9zdGF0dXMgIT0gJ1NLSVBQRUQnCiAgICBBTkQgKHRlc3Rfc3RhdHVzIExJS0UgJ0YlJyBPUiB0ZXN0X3N0YXR1cyBMSUtFICdFJScpCiAgICBBTkQgY2hlY2tfc3RhdHVzICE9ICdzdWNjZXNzJwogICAgQU5EIGNoZWNrX25hbWUgTk9UIExJS0UgJ2xpYkZ1enplciUnCiAgICBBTkQgY2hlY2tfbmFtZSAhPSAnQ2xpY2tIb3VzZSBLZWVwZXIgSmVwc2VuJwogICAgQU5EIHRlc3RfbmFtZSBMSUtFICclYXNzZXJ0SGFzVmFsaWRWZXJzaW9uTWV0YWRhdGElJwpPUkRFUiBCWSBjaGVja19zdGFydF90aW1lIERFU0M=

    if random.random() < 0.1:
        client_options.append("optimize_trivial_approximate_count_query=1")

    if random.random() < 0.3:
        client_options.append(f"http_make_head_request={random.randint(0, 1)}")

    # TODO: After release 24.3 use ignore_drop_queries_probability for both
    #       stress test and upgrade check
    if not upgrade_check:
        client_options.append("ignore_drop_queries_probability=0.2")

    if random.random() < 0.2:
        client_options.append("enable_parallel_replicas=1")
        client_options.append("max_parallel_replicas=3")
        client_options.append("cluster_for_parallel_replicas='parallel_replicas'")
        client_options.append("parallel_replicas_for_non_replicated_merge_tree=1")

    if random.random() < 0.2:
        client_options.append(
            f"query_plan_join_swap_table={random.choice(['auto', 'false', 'true'])}"
        )
        client_options.append(
            f"query_plan_optimize_join_order_limit={random.randint(0, 64)}"
        )

    if random.random() < 0.2 and not upgrade_check:
        client_options.append(
            f"compatibility='{random.randint(20, 26)}.{random.randint(1, 12)}'"
        )

    if random.random() < 0.3:
        options.append("--replace-log-memory-with-mergetree")

    if random.random() < 0.2:
        client_options.append("async_insert=1")

    if random.random() < 0.05:
        client_options.append("enable_join_runtime_filters=1")

    # dpsize' - implements DPsize algorithm currently only for Inner joins. So it may not work in some tests.
    # That is why we use it with fallback to 'greedy'.
    join_order_algorithm_combinations = ["greedy", "dpsize,greedy", "greedy,dpsize"]
    client_options.append(
        f"query_plan_optimize_join_order_algorithm={random.choice(join_order_algorithm_combinations)}"
    )

    # Pin max_parser_backtracks on the client command line. Its pre-24.3 default is 0, so the
    # randomized compatibility='NN.N' above reverts it to 0 in the client, which then sends 0 to
    # the server and trips the <min>1</min> limit-recursion constraint on every query. A
    # command-line value survives applyCompatibilitySetting, unlike a users.d profile value.
    client_options.append("max_parser_backtracks=1000000")

    if client_options:
        options.append(" --client-option " + " ".join(client_options))

    return " ".join(options)


def install_thread_pool_fault_injection() -> None:
    """Install `cannot_allocate_thread_injection.xml` and reload config so
    `cannot_allocate_thread_fault_injection_probability` becomes active.
    Fail-close on persistent reload failure or inactive setting after reload."""
    src = "/repo/tests/config/config.d/cannot_allocate_thread_injection.xml"
    dst = "/etc/clickhouse-server/config.d/cannot_allocate_thread_injection.xml"

    if not os.path.exists(src):
        raise RuntimeError(f"Thread-pool fault-injection source config not found at {src}")

    logging.info("Installing thread-pool fault-injection config: %s -> %s", src, dst)
    subprocess.run(["ln", "-sf", src, dst], check=True)
    if not call_with_retry(make_query_command("SYSTEM RELOAD CONFIG"), timeout=30, retry_count=5):
        # Fail-close before the verify query: a stale non-zero probability left
        # over from an earlier reload would otherwise mask the reload failure.
        raise RuntimeError(
            "SYSTEM RELOAD CONFIG failed after all retries; "
            "cannot activate thread-pool fault injection"
        )

    # The reload succeeded, but still verify the injector probability is
    # actually non-zero. The verify query gets the same retry treatment as the
    # reload itself: right after `SYSTEM RELOAD CONFIG` a debug server under
    # ThreadFuzzer can be slow enough to exceed the client's 15 s
    # `receive_timeout`, and a single timeout here must not kill the whole
    # stress job.
    verify_query = make_query_command(
        "SELECT value FROM system.server_settings "
        "WHERE name = 'cannot_allocate_thread_fault_injection_probability'"
    )
    retry_count = 5
    value = ""
    for i in range(retry_count):
        try:
            value = check_output(verify_query, shell=True, timeout=30, text=True).strip()
            break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            if i + 1 == retry_count:
                raise
            logging.info("Verify query failed (%s), retrying", str(e))
            time.sleep(i)
    if not value or float(value) <= 0:
        raise RuntimeError(
            f"cannot_allocate_thread_fault_injection_probability is {value!r} after reload"
        )
    logging.info("Thread-pool fault injection active: probability=%s", value)


def enable_mutation_delay_failpoint() -> None:
    """Enable `mutate_task_random_sleep_in_prepare`, so tests that `ALTER` without waiting
    routinely read parts the mutation has not rewritten yet. Reads over such parts resolve
    columns with the part's own (older) type, a state that mutations normally close too
    quickly to test (see #113925).
    Fail-close on persistent failure or if the failpoint is still off afterwards."""
    call_with_retry(
        make_query_command(f"SYSTEM ENABLE FAILPOINT {MUTATION_DELAY_FAILPOINT}")
    )

    # Fail-close: `call_with_retry` is silent when all its retries fail, so verify that the
    # failpoint really became active instead of silently losing the coverage.
    verify_query = make_query_command(
        f"SELECT enabled FROM system.fail_points WHERE name = '{MUTATION_DELAY_FAILPOINT}'"
    )
    enabled = check_output(verify_query, shell=True, timeout=30, text=True).strip()
    if enabled != "1":
        raise RuntimeError(
            f"Failpoint {MUTATION_DELAY_FAILPOINT} is not enabled after "
            f"SYSTEM ENABLE FAILPOINT: system.fail_points.enabled is {enabled!r}"
        )
    logging.info("Mutation-delay failpoint active: %s", MUTATION_DELAY_FAILPOINT)


def disable_mutation_delay_failpoint() -> None:
    """Disable `mutate_task_random_sleep_in_prepare` before the hung check, so the
    mutations still pending drain at full speed. Fail-close, mirroring
    `enable_mutation_delay_failpoint`: verify through `system.fail_points` that the
    failpoint is really off instead of trusting best-effort `call_with_retry`. Binaries
    that do not register the failpoint at all (e.g. the old binary in upgrade check,
    where `SYSTEM DISABLE FAILPOINT` would throw on the unknown name) are skipped."""
    probe_query = make_query_command(
        f"SELECT enabled FROM system.fail_points WHERE name = '{MUTATION_DELAY_FAILPOINT}'"
    )
    registered = check_output(probe_query, shell=True, timeout=30, text=True).strip()
    if not registered:
        logging.info(
            "Failpoint %s is not registered by this binary, nothing to disable",
            MUTATION_DELAY_FAILPOINT,
        )
        return
    call_with_retry(
        make_query_command(f"SYSTEM DISABLE FAILPOINT {MUTATION_DELAY_FAILPOINT}")
    )
    enabled = check_output(probe_query, shell=True, timeout=30, text=True).strip()
    if enabled != "0":
        raise RuntimeError(
            f"Failpoint {MUTATION_DELAY_FAILPOINT} is still enabled after "
            f"SYSTEM DISABLE FAILPOINT: system.fail_points.enabled is {enabled!r}; "
            "the hung check would run with mutations still delayed"
        )
    logging.info("Mutation-delay failpoint disabled: %s", MUTATION_DELAY_FAILPOINT)


def run_func_test(
    cmd: str,
    output_prefix: Path,
    num_processes: int,
    skip_tests_option: str,
    global_time_limit: int,
    upgrade_check: bool,
    encrypted_storage: bool,
    disruptor: Optional["RandomDisruptor"] = None,
) -> List[Popen]:
    upgrade_check_option = "--upgrade-check" if upgrade_check else ""
    encrypted_storage_option = "--encrypted-storage" if encrypted_storage else ""
    global_time_limit_option = (
        f"--global_time_limit={global_time_limit}" if global_time_limit else ""
    )
    # --stress-tests loops until global_time_limit; cap the smoke check so
    # clickhouse-test exits on its own within the execute_bash timeout (180s).
    smoke_time_limit = min(global_time_limit, 120) if global_time_limit else 120
    smoke_time_limit_option = f"--global_time_limit={smoke_time_limit}"

    output_paths = [
        output_prefix / f"stress_test_run_{i}.txt" for i in range(num_processes)
    ]
    pipes = []
    commands = []
    logging.info("Smoke check")
    for i, path in enumerate(output_paths):
        # Validate that simple tests work across all randomizations.
        # IF THIS FAILS, THE STRESS TESTS ARE BROKEN
        options = get_options(i, upgrade_check, encrypted_storage)
        base_command = (
            f"{cmd} --stress-tests {options} "
            f"{skip_tests_option} {upgrade_check_option} {encrypted_storage_option} "
        )
        full_command = f"{base_command} {global_time_limit_option} "
        commands.append(full_command)
        # Smoke check: disable AST fuzzer (fuzzed queries produce expected
        # errors in stderr) and cap global_time_limit so clickhouse-test
        # exits on its own within the execute_bash timeout.
        smoke_command = base_command.replace(
            "--client-option ", "--client-option ast_fuzzer_runs=0 ", 1
        ) + f" {smoke_time_limit_option} "
        check_command = (
            smoke_command
            + "--server-logs-level fatal --jobs 1 00001_select_1 00234_disjunctive_equality_chains_optimization"
        )
        logging.info(check_command)
        try:
            execute_bash(check_command, timeout=180)
        except subprocess.CalledProcessError as e:
            logging.info("Smoke check stdout:\n%s", e.stdout)
            logging.info("Smoke check stderr:\n%s", e.stderr)

            # Thread-pool fault injection is off during smoke check, so the
            # tolerated transients are ZK fault injection + per-worker
            # `memory_tracker_fault_probability` only.
            ignored_errors = [
                "Query memory tracker: fault injected",
                "KEEPER_EXCEPTION",
                "DATABASE_REPLICATION_FAILED",
                "QUERY_WAS_CANCELLED",
                "UNKNOWN_STATUS_OF_INSERT",
            ]
            if any(err in e.stdout or err in e.stderr for err in ignored_errors):
                logging.warning(
                    f"Detected known transient error, ignoring: {ignored_errors}"
                )
                continue
            raise RuntimeError(
                f"Smoke check failed (exit code {e.returncode}):\n"
                f"Command: {e.cmd}\n"
                f"stdout:\n{e.stdout}\n"
                f"stderr:\n{e.stderr}"
            ) from e

    # Smoke check passed: activate thread-pool fault injection for the real
    # stress test. Upgrade-check never had it (old binary may not support
    # the setting), so keep that behavior.
    if not upgrade_check:
        install_thread_pool_fault_injection()

        # Delay every background mutation by a bounded random amount.
        # Not in upgrade check: the old binary may not know the failpoint.
        enable_mutation_delay_failpoint()

    # Start the disruptor after smoke check completes, before actual stress test
    if disruptor is not None:
        disruptor.start()

    logging.info("Run stress tests")
    for i, path in enumerate(output_paths):
        with open(path, "w", encoding="utf-8") as op:
            command = commands[i]
            logging.info("Run func tests '%s'", command)
            # pylint:disable-next=consider-using-with
            pipes.append(Popen(command, shell=True, stdout=op, stderr=op))
            time.sleep(0.5)

    logging.info("Will wait functests to finish")
    while True:
        retcodes = []
        for p in pipes:
            if p.poll() is not None:
                retcodes.append(p.returncode)
        if len(retcodes) == len(pipes):
            break
        logging.info("Finished %s from %s processes", len(retcodes), len(pipes))
        time.sleep(5)

    return pipes


def compress_stress_logs(output_path: Path, files_prefix: str) -> None:
    """Archive the per-process `clickhouse-test` logs into a single file.

    A log can still be growing while it is archived: when the global time
    limit is reached, `clickhouse-test` force-kills its workers and exits,
    but a worker - or a `clickhouse client` the worker spawned - can outlive
    it and keep appending through the inherited stdout descriptor. `tar`
    notices the size change and exits with `TAR_EXIT_DIFFERS`, which used to
    fail the whole stress test job right before the hung check, even though
    the archive itself is written and only the tail of one log is missing.
    Only a fatal `tar` status is treated as a failure here.
    """
    archive = "stress_run_logs.tar.zst"
    result = subprocess.run(
        f"cd {output_path} && tar --zstd --create --file={archive} {files_prefix}*",
        shell=True,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == TAR_EXIT_DIFFERS:
        logging.warning(
            "Some logs changed while %s was being created: %s",
            archive,
            result.stderr.strip(),
        )
    elif result.returncode != 0:
        raise RuntimeError(
            f"Failed to create {archive}, tar exit code {result.returncode}:\n"
            f"{result.stdout}\n{result.stderr}"
        )

    # Not chained after `tar` with `&&`: the logs have to be removed on the
    # `TAR_EXIT_DIFFERS` path as well, otherwise they are uploaded twice.
    for path in output_path.glob(f"{files_prefix}*"):
        path.unlink()


def call_with_retry(
    query: str, timeout: int | float = 30, retry_count: int = 5
) -> bool:
    """Return whether the command eventually succeeded, so that callers which
    must not proceed after a persistent failure can fail close instead of
    silently continuing."""
    logging.info("Running command: %s", str(query))
    for i in range(retry_count):
        try:
            code = call(query, shell=True, stderr=STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired:
            logging.info("Command timed out after %s seconds, retrying", str(timeout))
            time.sleep(i)
            continue
        if code != 0:
            logging.info("Command returned %s, retrying", str(code))
            time.sleep(i)
        else:
            return True
    return False


def execute_bash(full_command, timeout=120):
    try:
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
        logging.info(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        # Display output before raising the exception as requested
        logging.info("Test failed. Captured Output:")
        logging.info(e.stdout)
        logging.info(e.stderr)
        raise
    except subprocess.TimeoutExpired as e:
        logging.info(f"Test timed out. Partial output:\n{e.stdout}")
        raise


def make_query_command(query: str) -> str:
    return (
        f'clickhouse client -q "{query}" --receive_timeout=15 --max_untracked_memory=1Gi '
        "--memory_profiler_step=1Gi --max_memory_usage_for_user=0 --max_memory_usage_in_client=1000000000 "
        "--enable-progress-table-toggle=0 "
        "--ast_fuzzer_runs=0",
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
    # Ensure that process exists
    if (
        call(
            "kill -0 $(cat /var/run/clickhouse-server/clickhouse-server.pid)",
            shell=True,
        )
        != 0
    ):
        raise ServerDied("clickhouse-server process does not exist")
    # Sometimes there is a message `Child process was stopped by signal 19` in logs after stopping gdb
    call_with_retry(
        "kill -CONT $(cat /var/run/clickhouse-server/clickhouse-server.pid) && clickhouse client --receive_timeout=5 -q 'SELECT 1 FORMAT Null'"
    )

    # ThreadFuzzer significantly slows down server and causes false-positive hung check failures
    call_with_retry(make_query_command("SYSTEM STOP THREAD FUZZER"))
    # Stop delaying mutations, so the ones still pending drain at full speed. Fail-close:
    # the hung check must not run with the delay still armed, and a binary that does not
    # register the failpoint (e.g. the old binary in upgrade check) is skipped.
    disable_mutation_delay_failpoint()
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
    call_with_retry(make_query_command("SYSTEM START VIEWS"))
    call_with_retry(make_query_command("SYSTEM DROP MARK CACHE"))

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
                    "Failed to SHOW or DROP databases, will retry %s", str(ex)
                )
                time.sleep(i)
        else:
            raise RuntimeError(
                "Cannot drop databases after stress tests. Probably server consumed "
                "too much memory and cannot execute simple queries"
            )

    # Wait for last queries to finish if any, not longer than 300 seconds
    cutoff_time = time.time() + 300
    while time.time() < cutoff_time:
        queries = int(
            check_output(
                make_query_command(
                    "SELECT count() FROM system.processes WHERE query NOT LIKE '%FROM system.processes%'"
                ),
                shell=True,
                stderr=STDOUT,
                timeout=30,
            )
            .decode("utf-8")
            .strip()
        )
        if queries == 0:
            break
        time.sleep(1)

    # Even if all clickhouse-test processes are finished, there are probably some sh scripts,
    # which still run some new queries. Let's ignore them.
    try:
        output = (
            check_output(
                make_query_command(
                    "SELECT count() FROM system.processes where elapsed > 300"
                ),
                shell=True,
                stderr=STDOUT,
                timeout=30,
            )
            .decode("utf-8")
            .strip()
        )
        if int(output) == 0:
            return False
    except Exception as ex:
        logging.error("Failed to check for long running queries: %s", str(ex))
    return True


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
    parser.add_argument("--num-parallel", type=int, default=min(8, cpu_count()))
    parser.add_argument("--upgrade-check", action="store_true")
    parser.add_argument("--hung-check", action="store_true", default=False)
    # make sense only for hung check
    parser.add_argument("--drop-databases", action="store_true", default=False)
    parser.add_argument(
        "--encrypted-storage", type=lambda x: bool(int(x)), default=False
    )
    parser.add_argument(
        "--no-random-disruptor",
        # Kept as an alias: the flag shipped under the old name, back when the thread only
        # killed queries.
        "--no-random-query-killer",
        dest="no_random_disruptor",
        action="store_true",
        default=False,
        help="Disable the random disruptor (query/client/mutation kills, merge pauses) "
        "during stress test",
    )
    return parser.parse_args()


def collect_stacktrace_dumps(output_folder: Path) -> None:
    # stdout keeps only a trimmed preview of the server stacktrace dumps;
    # the full dumps are written to the working directory.
    for stacktrace_log in ("sql_stacktraces.log", "c_stacktraces.log"):
        path = Path.cwd() / stacktrace_log
        if path.exists():
            # Not rename: source and destination are different mounts.
            shutil.move(path, output_folder / stacktrace_log)


def run_stress_test(args: argparse.Namespace) -> None:
    call_with_retry(make_query_command("SELECT 1"), timeout=0.5, retry_count=20)

    # Create the random disruptor unless disabled or in upgrade check mode
    # (upgrade check mode should not have random kills as it may interfere with
    # the upgrade process itself)
    # Note: the disruptor is started inside run_func_test after the smoke check completes
    disruptor = None
    if not args.no_random_disruptor and not args.upgrade_check:
        disruptor = RandomDisruptor(interval=3.0)

    try:
        run_func_test(
            args.test_cmd,
            args.output_folder,
            args.num_parallel,
            args.skip_func_tests,
            args.global_time_limit,
            args.upgrade_check,
            args.encrypted_storage,
            disruptor,
        )
    finally:
        # Stop the disruptor when tests are done
        if disruptor is not None:
            disruptor.stop()

    logging.info("All processes finished")

    logging.info("Compressing stress logs")
    compress_stress_logs(args.output_folder, "stress_test_run_")
    logging.info("Logs compressed")

    if args.hung_check:
        server_died = False
        try:
            have_long_running_queries = prepare_for_hung_check(args.drop_databases)
        except ServerDied:
            server_died = True
            status_message = "Server died\tFAIL\t\\N\t\n"
            with open(
                args.output_folder / "test_results.tsv", "w+", encoding="utf-8"
            ) as results:
                results.write(status_message)
        except Exception as ex:
            have_long_running_queries = True
            logging.error("Failed to prepare for hung check: %s", str(ex))

        if not server_died:
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
                    "--client-option",
                    "max_untracked_memory=1Gi",
                    "max_memory_usage_for_user=0",
                    "memory_profiler_step=1Gi",
                    "ast_fuzzer_runs=0",
                    # Use system database to avoid CREATE/DROP DATABASE queries
                    "--database=system",
                    "--hung-check",
                    "--capture-client-stacktrace",
                    "--report-logs-stats",
                    "00001_select_1",
                ]
            )
            hung_check_log = args.output_folder / "hung_check.log"  # type: Path
            with Popen(["/usr/bin/tee", hung_check_log], stdin=PIPE) as tee:
                try:
                    # Own session, so that on timeout the whole process
                    # tree can be killed at once; otherwise survivors keep
                    # appending to the dumps while they are collected.
                    with Popen(
                        cmd,
                        shell=True,
                        stdout=tee.stdin,
                        stderr=STDOUT,
                        start_new_session=True,
                    ) as hung_check:
                        try:
                            res = hung_check.wait(timeout=600)
                        except subprocess.TimeoutExpired:
                            os.killpg(hung_check.pid, signal.SIGKILL)
                            # The test runner starts each test in its own
                            # session, out of reach of the killpg above,
                            # but records the pgid in a file for exactly
                            # this situation. The test command may carry
                            # options (e.g. in the upgrade check), while
                            # cleanup needs only the executable.
                            test_runner = shlex.split(args.test_cmd)[0]
                            call([test_runner, "--cleanup"], timeout=60)
                            raise
                finally:
                    if tee.stdin is not None:
                        tee.stdin.close()
                    try:
                        # EOF on the pipe means every process that
                        # inherited it as stdout/stderr has exited: the
                        # barrier that keeps the collection of the dumps
                        # from racing a live writer.
                        tee.wait(timeout=60)
                    except subprocess.TimeoutExpired:
                        # A writer survived both kills, e.g. a process
                        # in uninterruptible sleep dies only once its
                        # kernel wait completes. Give up on the barrier:
                        # a dump with a torn tail beats losing it to the
                        # job timeout.
                        logging.warning(
                            "Some hung check process survived the kill"
                        )
                        tee.kill()
            if res != 0 and have_long_running_queries:
                logging.info("Hung check failed with exit code %d", res)

                # Embed a tail of the captured hung-check output in
                # test_results.tsv so the processlist and thread stacktraces
                # are visible in CIDB. The full log is also kept as a CI
                # artifact (see process_results in stress_job.py), giving
                # investigators access to the complete diagnostic output.
                #
                # Read only the last 32 KiB rather than the whole file: on
                # deadlock failures `hung_check.log` can be very large (a
                # full processlist plus a `gdb` backtrace for every server
                # process), and the stress-test machine is already under
                # memory pressure. The diagnostic content we need
                # (`Found hung queries`, the processlist with stacktraces,
                # the `gdb` backtraces) is printed at the end of the log,
                # so the tail is exactly the relevant region.
                info_field = ""
                try:
                    tail_bytes_size = 32 * 1024
                    with open(hung_check_log, "rb") as f:
                        f.seek(0, os.SEEK_END)
                        size = f.tell()
                        offset = max(0, size - tail_bytes_size)
                        f.seek(offset)
                        tail_bytes = f.read()
                    log_text = tail_bytes.decode("utf-8", errors="replace")
                    if offset > 0:
                        # Drop the (likely partial) first line so the tail
                        # always starts on a line boundary.
                        nl = log_text.find("\n")
                        if nl >= 0:
                            log_text = log_text[nl + 1 :]
                        log_text = (
                            "(truncated; see hung_check.log artifact for"
                            " the full output; showing last 32 KiB)\n...\n"
                            + log_text
                        )
                    # Escape so NUL, tab, and newline survive the TSV encoding,
                    # matching the decoder in read_test_results().
                    info_field = escape_tsv_info(log_text)
                except OSError as ex:
                    logging.warning(
                        "Failed to read hung_check.log to embed in"
                        " test_results.tsv: %s",
                        ex,
                    )

                hung_check_status = (
                    "Hung check failed, possible deadlock found\tFAIL\t\\N\t"
                    f"{info_field}\n"
                )
                with open(
                    args.output_folder / "test_results.tsv", "w+", encoding="utf-8"
                ) as results:
                    results.write(hung_check_status)
                # Keep hung_check.log on disk so the CI artifact upload picks
                # it up. Without it, deadlock investigations have no evidence
                # to work with — see ClickHouse/ClickHouse#100941.
            else:
                logging.info("No queries hung")

    logging.info("Stress test finished")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()

    if args.drop_databases and not args.hung_check:
        raise argparse.ArgumentTypeError(
            "--drop-databases only used in hung check (--hung-check)"
        )

    try:
        run_stress_test(args)
    finally:
        # Any exit path can leave dumps behind: the upgrade check runs
        # without the hung check, and the test run can raise before the
        # hung check is reached.
        collect_stacktrace_dumps(args.output_folder)


if __name__ == "__main__":
    main()
