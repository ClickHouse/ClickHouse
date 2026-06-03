#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This script is used in docker images for stress tests and upgrade tests"""
import argparse
import logging
import os
import random
import signal
import subprocess
import time
import threading
from multiprocessing import cpu_count
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen, call, check_output
from typing import List, Optional


CLICKHOUSE_PID_FILE = "/var/run/clickhouse-server/clickhouse-server.pid"


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


class ChaosThread:
    """Base class for background chaos threads.

    Subclasses implement _loop_body with their specific chaos logic.
    The base class handles the threading, interval jitter, exception
    resilience, and clean shutdown.
    """

    NAME = "ChaosThread"

    def __init__(self, min_interval: float, max_interval: float):
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._min_interval = min_interval
        self._max_interval = max_interval

    def _loop_body(self) -> None:
        raise NotImplementedError

    def _run(self) -> None:
        logging.info(
            "%s started (interval: %.0f-%.0fs)",
            self.NAME,
            self._min_interval,
            self._max_interval,
        )
        while not self._stop_event.is_set():
            delay = random.uniform(self._min_interval, self._max_interval)
            if self._stop_event.wait(delay):
                break
            if self._stop_event.is_set():
                break
            try:
                self._loop_body()
            except Exception as e:
                logging.error("%s: cycle failed: %s", self.NAME, e)
        logging.info("%s stopped", self.NAME)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=10)
        self._thread = None


class RandomRestarter(ChaosThread):
    """Base for chaos threads that periodically restart a service.

    Subclasses implement _stop_hard, _stop_graceful, and _start_and_wait.
    The base class handles the hard/graceful coin flip and ensures stop()
    waits for any in-progress restart to finish.
    """

    NAME = "RandomRestarter"

    def __init__(self, min_interval: float = 120.0, max_interval: float = 300.0):
        super().__init__(min_interval, max_interval)
        # Held while a restart cycle is in progress so stop() can wait for the
        # service to be back up before returning.
        self._restart_lock = threading.Lock()
        self._recovery_failures = 0

    def _stop_hard(self) -> None:
        raise NotImplementedError

    def _stop_graceful(self) -> None:
        raise NotImplementedError

    def _start_and_wait(self) -> None:
        raise NotImplementedError

    def _loop_body(self) -> None:
        with self._restart_lock:
            if random.random() < 0.5:
                self._stop_hard()
            else:
                self._stop_graceful()
            self._start_and_wait()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=10)
        with self._restart_lock:
            pass
        self._thread = None

    @staticmethod
    def _wait_port_free(port: int, timeout: float = 60.0) -> None:
        """Wait until *port* is no longer in LISTEN state."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ret = subprocess.run(
                f"ss -tlnp | grep -q ':{port}\\b'",
                shell=True,
                capture_output=True,
            )
            if ret.returncode != 0:
                return
            time.sleep(1)
        logging.warning("port %d still in use after %.0fs", port, timeout)


class RandomServerRestarter(RandomRestarter):
    """Periodically stops and restarts clickhouse-server.

    Today the server only goes down at the start/end of the stress run, so
    recovery and shutdown code paths under heavy load are never exercised.
    This thread randomly alternates between hard crashes (SIGKILL) and
    graceful stops once every few minutes so that WAL replay, mark-cache
    rebuilds, merge recovery, flush paths, and distributed send draining
    all get coverage during stress.

    Each SIGKILL is recorded in a marker file so that stress_job.py can
    distinguish intentional kills from real OOM events.
    """

    NAME = "RandomServerRestarter"
    INTENTIONAL_KILLS_FILE = "/test_output/stress_intentional_server_kills.log"

    def _read_pid(self) -> Optional[int]:
        try:
            with open(CLICKHOUSE_PID_FILE, "r") as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _wait_server_up(self, timeout: float = 120.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                subprocess.run(
                    "clickhouse client -q 'SELECT 1'",
                    shell=True,
                    capture_output=True,
                    timeout=5,
                    check=True,
                )
                return True
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                time.sleep(1)
        return False

    def _stop_hard(self) -> None:
        pid = self._read_pid()
        if pid is None:
            logging.warning("%s: no PID file, server may already be down", self.NAME)
            return
        logging.info("%s: sending SIGKILL to server (pid %d)", self.NAME, pid)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            logging.info("%s: server already dead", self.NAME)
            return
        for _ in range(60):
            try:
                os.kill(pid, 0)
                time.sleep(0.5)
            except OSError:
                break
        with open(self.INTENTIONAL_KILLS_FILE, "a") as f:
            f.write(f"{pid}\n")

    def _stop_graceful(self) -> None:
        logging.info("%s: graceful stop via clickhouse stop", self.NAME)
        ret = subprocess.run(
            "clickhouse stop --do-not-kill --max-tries 60",
            shell=True,
            capture_output=True,
            timeout=90,
        )
        if ret.returncode != 0:
            logging.warning(
                "%s: graceful stop failed (rc=%d), falling back to SIGKILL",
                self.NAME,
                ret.returncode,
            )
            self._stop_hard()

    def _start_and_wait(self) -> None:
        self._wait_port_free(9000)
        self._wait_port_free(9009)
        self._wait_port_free(9181)

        logging.info("%s: starting server", self.NAME)
        subprocess.run(
            "clickhouse start --user root "
            ">>/var/log/clickhouse-server/stdout.log "
            "2>>/var/log/clickhouse-server/stderr.log",
            shell=True,
            timeout=30,
        )

        if self._wait_server_up():
            logging.info("%s: server back up", self.NAME)
        else:
            logging.error("%s: server did not come back within timeout", self.NAME)
            self._recovery_failures += 1


class RandomMinIORestarter(RandomRestarter):
    """Periodically kills and restarts MinIO.

    S3 storage policies route through MinIO in the stress test environment.
    Killing MinIO mid-operation exercises object storage retry logic,
    reconnection handling, and partial-upload recovery in ClickHouse.

    Note: Keeper restarts are not needed separately because the stress test
    uses embedded Keeper (same process as clickhouse-server), which is already
    covered by RandomServerRestarter.
    """

    NAME = "RandomMinIORestarter"
    MINIO_PORT = 11111

    def __init__(self, minio_data_dir: str, min_interval: float = 120.0, max_interval: float = 300.0):
        super().__init__(min_interval, max_interval)
        self._minio_data_dir = minio_data_dir

    def _wait_minio_up(self, timeout: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                subprocess.run(
                    "/mc ls clickminio/test",
                    shell=True,
                    capture_output=True,
                    timeout=5,
                    check=True,
                )
                return True
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                time.sleep(1)
        return False

    def _stop_hard(self) -> None:
        logging.info("%s: hard-killing MinIO (SIGKILL)", self.NAME)
        subprocess.run(
            "pkill -9 -f 'minio server'",
            shell=True,
            capture_output=True,
        )

    def _stop_graceful(self) -> None:
        logging.info("%s: graceful stop MinIO (SIGTERM)", self.NAME)
        subprocess.run(
            "pkill -f 'minio server'",
            shell=True,
            capture_output=True,
        )
        for _ in range(30):
            ret = subprocess.run(
                "pgrep -f 'minio server'",
                shell=True,
                capture_output=True,
            )
            if ret.returncode != 0:
                break
            time.sleep(1)

    def _start_and_wait(self) -> None:
        self._wait_port_free(self.MINIO_PORT)

        logging.info("%s: starting MinIO", self.NAME)
        subprocess.Popen(
            f"nohup /minio server --address :{self.MINIO_PORT} {self._minio_data_dir} "
            ">/dev/null 2>&1 &",
            shell=True,
        )

        if self._wait_minio_up():
            logging.info("%s: MinIO back up", self.NAME)
        else:
            logging.error("%s: MinIO did not come back within timeout", self.NAME)
            self._recovery_failures += 1


class RandomAzuriteRestarter(RandomRestarter):
    """Periodically kills and restarts Azurite (Azure Blob Storage emulator).

    Azure storage policies route through Azurite in the stress test environment.
    Killing Azurite mid-operation exercises Azure retry logic, reconnection
    handling, and partial-upload recovery in ClickHouse.
    """

    NAME = "RandomAzuriteRestarter"
    AZURITE_PORT = 10000

    def _wait_azurite_up(self, timeout: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                subprocess.run(
                    f"curl -s -o /dev/null -w '%{{http_code}}' http://127.0.0.1:{self.AZURITE_PORT}/ "
                    "| grep -qE '400|200'",
                    shell=True,
                    capture_output=True,
                    timeout=5,
                    check=True,
                )
                return True
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                time.sleep(1)
        return False

    def _stop_hard(self) -> None:
        logging.info("%s: hard-killing Azurite (SIGKILL)", self.NAME)
        subprocess.run(
            "pkill -9 -f 'azurite-rs'",
            shell=True,
            capture_output=True,
        )

    def _stop_graceful(self) -> None:
        logging.info("%s: graceful stop Azurite (SIGTERM)", self.NAME)
        subprocess.run(
            "pkill -f 'azurite-rs'",
            shell=True,
            capture_output=True,
        )
        for _ in range(30):
            ret = subprocess.run(
                "pgrep -f 'azurite-rs'",
                shell=True,
                capture_output=True,
            )
            if ret.returncode != 0:
                break
            time.sleep(1)

    def _start_and_wait(self) -> None:
        self._wait_port_free(self.AZURITE_PORT)

        logging.info("%s: starting Azurite", self.NAME)
        subprocess.Popen(
            "(ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)) && "
            f"nohup azurite-rs --host 0.0.0.0 --blob-port {self.AZURITE_PORT} --silent --in-memory "
            ">/dev/null 2>&1 &",
            shell=True,
        )

        if self._wait_azurite_up():
            logging.info("%s: Azurite back up", self.NAME)
        else:
            logging.error("%s: Azurite did not come back within timeout", self.NAME)
            self._recovery_failures += 1


class RandomRedpandaRestarter(RandomRestarter):
    """Periodically kills and restarts Redpanda (Kafka-compatible broker).

    Kafka engines and table functions route through Redpanda in the stress test
    environment. Killing Redpanda mid-operation exercises consumer group
    recovery, offset management, producer retry logic, and reconnection
    handling in ClickHouse.
    """

    NAME = "RandomRedpandaRestarter"
    KAFKA_PORT = 9092
    SCHEMA_REGISTRY_PORT = 8081
    RPC_PORT = 19092

    def _wait_redpanda_up(self, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                subprocess.run(
                    f"rpk topic list --brokers 127.0.0.1:{self.KAFKA_PORT} >/dev/null 2>&1 "
                    f"&& curl -sf http://127.0.0.1:{self.SCHEMA_REGISTRY_PORT}/subjects >/dev/null 2>&1",
                    shell=True,
                    capture_output=True,
                    timeout=5,
                    check=True,
                )
                return True
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                time.sleep(1)
        return False

    def _stop_hard(self) -> None:
        logging.info("%s: hard-killing Redpanda (SIGKILL)", self.NAME)
        subprocess.run("pkill -9 -f 'rpk redpanda start'", shell=True, capture_output=True)
        subprocess.run("pkill -9 -f '/opt/redpanda/'", shell=True, capture_output=True)

    def _stop_graceful(self) -> None:
        logging.info("%s: graceful stop Redpanda (SIGTERM)", self.NAME)
        subprocess.run("pkill -f 'rpk redpanda start'", shell=True, capture_output=True)
        subprocess.run("pkill -f '/opt/redpanda/'", shell=True, capture_output=True)
        for _ in range(30):
            ret = subprocess.run(
                "pgrep -f '/opt/redpanda/'",
                shell=True,
                capture_output=True,
            )
            if ret.returncode != 0:
                break
            time.sleep(1)

    def _start_and_wait(self) -> None:
        self._wait_port_free(self.KAFKA_PORT)
        self._wait_port_free(self.SCHEMA_REGISTRY_PORT)
        self._wait_port_free(self.RPC_PORT)

        logging.info("%s: starting Redpanda", self.NAME)
        subprocess.Popen(
            "nohup rpk redpanda start "
            "--mode dev-container --smp 1 --memory 256M --reserve-memory 0M --overprovisioned "
            f"--kafka-addr 127.0.0.1:{self.KAFKA_PORT} --advertise-kafka-addr 127.0.0.1:{self.KAFKA_PORT} "
            f"--rpc-addr 127.0.0.1:{self.RPC_PORT} --advertise-rpc-addr 127.0.0.1:{self.RPC_PORT} "
            f"--schema-registry-addr 127.0.0.1:{self.SCHEMA_REGISTRY_PORT} "
            "--set redpanda.auto_create_topics_enabled=false "
            "--set redpanda.log_segment_size=16777216 "
            ">/dev/null 2>&1 &",
            shell=True,
        )

        if self._wait_redpanda_up():
            logging.info("%s: Redpanda back up", self.NAME)
        else:
            logging.error("%s: Redpanda did not come back within timeout", self.NAME)
            self._recovery_failures += 1


class RandomQueryKiller(ChaosThread):
    """Randomly kills queries and client processes during stress tests.

    This helps test that queries are cancelled correctly and handles scenarios
    where the client unexpectedly disconnects (issue #39803).
    """

    NAME = "RandomQueryKiller"

    def __init__(self, interval: float = 3.0):
        super().__init__(min_interval=interval, max_interval=interval)

    def _kill_random_query(self) -> None:
        try:
            result = check_output(
                "clickhouse client -q \""
                "SELECT query_id FROM system.processes "
                "WHERE query NOT LIKE '%system.processes%' "
                "AND query NOT LIKE '%KILL QUERY%' "
                "AND elapsed > 0.1 "
                "ORDER BY rand() LIMIT 1\" 2>/dev/null",
                shell=True,
                timeout=5,
            )
            query_id = result.decode("utf-8").strip()
            if query_id:
                logging.info("Killing random query: %s", query_id)
                call(
                    f"clickhouse client -q \"KILL QUERY WHERE query_id = '{query_id}' ASYNC\" 2>/dev/null",
                    shell=True,
                    timeout=5,
                )
        except Exception as e:
            logging.debug("Random query killer got exception (expected): %s", e)

    def _kill_random_client(self) -> None:
        try:
            result = check_output(
                "pgrep -f 'clickhouse-client|clickhouse client' 2>/dev/null || true",
                shell=True,
                timeout=5,
            )
            pids = [p.strip() for p in result.decode("utf-8").strip().split("\n") if p.strip()]
            if pids:
                pid = random.choice(pids)
                logging.info("Killing random client process: %s", pid)
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except (ProcessLookupError, ValueError):
                    pass
        except Exception as e:
            logging.debug("Random client killer got exception (expected): %s", e)

    def _kill_random_mutation(self) -> None:
        try:
            result = check_output(
                "clickhouse client -q \""
                "SELECT mutation_id, database, table "
                "FROM system.mutations "
                "WHERE NOT is_done AND NOT is_killed "
                "ORDER BY rand() LIMIT 1\" 2>/dev/null",
                shell=True,
                timeout=5,
            )
            line = result.decode("utf-8").strip()
            if line:
                mutation_id, db, table = line.split("\t")
                logging.info("Killing random mutation: %s on %s.%s", mutation_id, db, table)
                call(
                    f"clickhouse client -q \"KILL MUTATION WHERE database = '{db}' "
                    f"AND table = '{table}' AND mutation_id = '{mutation_id}'\" 2>/dev/null",
                    shell=True,
                    timeout=5,
                )
        except Exception as e:
            logging.debug("Random mutation killer got exception (expected): %s", e)

    def _loop_body(self) -> None:
        r = random.random()
        if r < 0.6:
            self._kill_random_query()
        elif r < 0.8:
            self._kill_random_client()
        else:
            self._kill_random_mutation()


def get_options(i: int, upgrade_check: bool, encrypted_storage: bool) -> str:
    options = []
    client_options = []

    if upgrade_check:
        # Disable settings randomization for upgrade checks to prevent test failures caused by missing settings in old version
        options.append("--no-random-settings")
        options.append("--no-random-merge-tree-settings")

    # allow constraint
    client_options.append(f"enable_analyzer=1")

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
    encrypted_storage: bool,
    chaos_threads: Optional[List["ChaosThread"]] = None,
) -> List[Popen]:
    upgrade_check_option = "--upgrade-check" if upgrade_check else ""
    encrypted_storage_option = "--encrypted-storage" if encrypted_storage else ""
    global_time_limit_option = (
        f"--global_time_limit={global_time_limit}" if global_time_limit else ""
    )

    output_paths = [
        output_prefix / f"stress_test_run_{i}.txt" for i in range(num_processes)
    ]
    pipes = []
    commands = []
    logging.info("Smoke check")
    for i, path in enumerate(output_paths):
        # Validate that simple tests work across all randomizations.
        # IF THIS FAILS, THE STRESS TESTS ARE BROKEN
        full_command = (
            f"{cmd} --stress-tests {get_options(i, upgrade_check, encrypted_storage)} {global_time_limit_option} "
            f"{skip_tests_option} {upgrade_check_option} {encrypted_storage_option} "
        )
        commands.append(full_command)
        # Disable server-side AST fuzzer for the smoke check: fuzzed queries
        # produce expected errors in stderr, which would fail these tests.
        smoke_command = full_command.replace(
            "--client-option ", "--client-option ast_fuzzer_runs=0 ", 1
        )
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

            # Ignore fault injects and transient errors, but most of the time tests should complete successfully
            ignored_errors = [
                "CANNOT_SCHEDULE_TASK",
                "Fault injection",
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

    # Start chaos threads after smoke check completes, before actual stress test
    for ct in chaos_threads or []:
        ct.start()

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
    cmd = (
        f"cd {output_path} && tar --zstd --create --file=stress_run_logs.tar.zst "
        f"{files_prefix}* && rm {files_prefix}*"
    )
    check_output(cmd, shell=True)


def call_with_retry(
    query: str, timeout: int | float = 30, retry_count: int = 5
) -> None:
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
            break


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
        f'clickhouse client -q "{query}" --max_untracked_memory=1Gi '
        "--memory_profiler_step=1Gi --max_memory_usage_for_user=0 --max_memory_usage_in_client=1000000000 "
        "--enable-progress-table-toggle=0"
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
            f"kill -0 $(cat {CLICKHOUSE_PID_FILE})",
            shell=True,
        )
        != 0
    ):
        raise ServerDied("clickhouse-server process does not exist")
    # Sometimes there is a message `Child process was stopped by signal 19` in logs after stopping gdb
    call_with_retry(
        f"kill -CONT $(cat {CLICKHOUSE_PID_FILE}) && clickhouse client -q 'SELECT 1 FORMAT Null'"
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

    # Issue #21004, window views are experimental, so let's just suppress it
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
        "--no-random-query-killer",
        action="store_true",
        default=False,
        help="Disable random query/client killer during stress test",
    )
    parser.add_argument(
        "--no-random-server-restart",
        action="store_true",
        default=False,
        help="Disable random kill -9 / restart of clickhouse-server during stress test",
    )
    parser.add_argument(
        "--no-random-minio-restart",
        action="store_true",
        default=False,
        help="Disable random kill -9 / restart of MinIO during stress test",
    )
    parser.add_argument(
        "--no-random-azurite-restart",
        action="store_true",
        default=False,
        help="Disable random kill -9 / restart of Azurite during stress test",
    )
    parser.add_argument(
        "--no-random-redpanda-restart",
        action="store_true",
        default=False,
        help="Disable random kill -9 / restart of Redpanda during stress test",
    )
    parser.add_argument(
        "--minio-data-dir",
        default="",
        help="MinIO data directory (required for random MinIO restarts)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()

    if args.drop_databases and not args.hung_check:
        raise argparse.ArgumentTypeError(
            "--drop-databases only used in hung check (--hung-check)"
        )

    call_with_retry(make_query_command("SELECT 1"), timeout=0.5, retry_count=20)

    # Build chaos threads list — started inside run_func_test after smoke check.
    # Order matters: stop runs in reverse so services come down before the
    # query killer, and the server stops last (hung check needs it alive).
    chaos_threads: List[ChaosThread] = []
    if not args.no_random_query_killer and not args.upgrade_check:
        chaos_threads.append(RandomQueryKiller(interval=3.0))
    # Azurite runs --in-memory; a restart wipes all blobs. Any table or
    # object that references Azure storage (MergeTree default policy, explicit
    # Azure disks, table functions) loses its data — testing data loss, not
    # retry/reconnect resilience. Disabled until Azurite runs with persistent
    # storage.
    if not args.no_random_azurite_restart and not args.upgrade_check:
        logging.info("Skipping RandomAzuriteRestarter: Azurite --in-memory restart wipes all blobs")
    if not args.no_random_minio_restart and not args.upgrade_check and args.minio_data_dir:
        chaos_threads.append(RandomMinIORestarter(args.minio_data_dir, min_interval=120.0, max_interval=300.0))
    if not args.no_random_redpanda_restart and not args.upgrade_check:
        chaos_threads.append(RandomRedpandaRestarter(min_interval=120.0, max_interval=300.0))
    if not args.no_random_server_restart and not args.upgrade_check:
        chaos_threads.append(RandomServerRestarter(min_interval=120.0, max_interval=300.0))

    try:
        func_pipes = run_func_test(
            args.test_cmd,
            args.output_folder,
            args.num_parallel,
            args.skip_func_tests,
            args.global_time_limit,
            args.upgrade_check,
            args.encrypted_storage,
            chaos_threads,
        )
    finally:
        for ct in reversed(chaos_threads):
            ct.stop()

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
                    #
                    # NOTE: that if there will be queries with GROUP BY, this trick
                    # will not work due to CurrentMemoryTracker::check() from
                    # Aggregator code.
                    # But right now it should work, since neither hung check, nor 00001_select_1 has GROUP BY.
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
                res = call(
                    cmd, shell=True, stdout=tee.stdin, stderr=STDOUT, timeout=600
                )
                if tee.stdin is not None:
                    tee.stdin.close()
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

    failed_chaos = [
        ct
        for ct in chaos_threads
        if isinstance(ct, RandomRestarter) and ct._recovery_failures > 0
    ]
    if failed_chaos:
        with open(
            args.output_folder / "test_results.tsv", "a+", encoding="utf-8"
        ) as results:
            for ct in failed_chaos:
                results.write(
                    f"{ct.NAME} recovery failed {ct._recovery_failures} time(s)"
                    "\tFAIL\t\\N\t\n"
                )
        logging.error(
            "Chaos thread recovery failures: %s",
            ", ".join(f"{ct.NAME}={ct._recovery_failures}" for ct in failed_chaos),
        )

    logging.info("Stress test finished")


if __name__ == "__main__":
    main()
