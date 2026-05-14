#!/usr/bin/env python3

import re
import time

import pytest

from helpers.cluster import ClickHouseCluster

NUM_TABLES = 12
DATABASE = "many_tables"
FAILPOINT = "database_catalog_shutdown_sleep_per_table"
MAX_EXPECTED_SHUTDOWN_SEC = 10

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/concurrency.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_tables():
    node.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    for i in range(NUM_TABLES):
        node.query(
            f"CREATE TABLE {DATABASE}.t{i} (id UInt64, v String) ENGINE = MergeTree ORDER BY id"
        )
        node.query(f"INSERT INTO {DATABASE}.t{i} VALUES ({i}, 'row{i}')")


def _drop_database():
    node.query(f"DROP DATABASE IF EXISTS {DATABASE} SYNC")


def test_parallel_shutdown_logs_and_data_intact(started_cluster):
    """
    Server shutdown must (a) traverse the parallel-shutdown code path for our
    user database, (b) beat the sequential baseline when each table shutdown
    is artificially slowed down, and (c) leave the data behind so that we can
    read it back after restart.
    """
    _create_tables()
    restarted = False
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

        # Pin a marker we can scan logs against to avoid false matches from any
        # earlier startup activity. Truncate the file to start clean.
        node.exec_in_container(["bash", "-c", ": > /var/log/clickhouse-server/clickhouse-server.log"])

        # Soft shutdown — exercises the same Context::shutdown path that
        # production SIGTERM hits, including DatabaseWithOwnTablesBase::shutdown.
        shutdown_started = time.monotonic()
        node.stop_clickhouse(kill=False, stop_wait_sec=60)
        shutdown_elapsed = time.monotonic() - shutdown_started

        assert shutdown_elapsed < MAX_EXPECTED_SHUTDOWN_SEC, (
            "Graceful shutdown took too long with per-table delay injection: "
            f"{shutdown_elapsed:.2f}s >= {MAX_EXPECTED_SHUTDOWN_SEC}s. "
            "That looks closer to sequential shutdown than the expected parallel path."
        )

        # zgrep uses BRE — escape +. Database name is unquoted unless it needs
        # backquoting; `many_tables` is a plain identifier so no backticks appear.
        shutdown_log = node.grep_in_log(
            f"flushAndShutdown for [0-9][0-9]* tables in {DATABASE} took",
            only_latest=True,
        )
        assert shutdown_log, (
            "Did not see the expected per-database shutdown log line — "
            "the parallel shutdown path may not have run."
        )

        # Format: "flushAndShutdown for N tables in many_tables took M ms"
        match = re.search(r"flushAndShutdown for (\d+) tables", shutdown_log)
        assert match, f"Could not parse table count from log line: {shutdown_log!r}"
        assert int(match.group(1)) == NUM_TABLES

        # Both phases should have run.
        prepare_log = node.grep_in_log(
            f"flushAndPrepareForShutdown for [0-9][0-9]* tables in {DATABASE} took",
            only_latest=True,
        )
        assert prepare_log, "Did not see the flushAndPrepareForShutdown phase log line."

        node.start_clickhouse()
        restarted = True

        # Data should still be there after restart.
        for i in range(NUM_TABLES):
            assert (
                node.query(f"SELECT v FROM {DATABASE}.t{i} WHERE id = {i}").strip()
                == f"row{i}"
            )
    finally:
        if node.get_process_pid("clickhouse") is not None:
            node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
            if restarted:
                _drop_database()
