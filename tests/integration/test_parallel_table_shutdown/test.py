#!/usr/bin/env python3

import re

import pytest

from helpers.cluster import ClickHouseCluster

NUM_TABLES = 12
DATABASE = "many_tables"
FAILPOINT = "database_catalog_shutdown_sleep_per_table"
# concurrency.xml sets database_catalog_shutdown_table_concurrency = 4, so with a 1s
# per-table sleep the 12 tables drain in ~3 waves (~3s). Sequential would be ~12s.
# We assert against the server-reported duration (not wall clock) so sanitizer overhead
# on the test/orchestration side doesn't make this flaky.
MAX_EXPECTED_SHUTDOWN_MS = 8000

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
        node.stop_clickhouse(kill=False, stop_wait_sec=60)

        # zgrep uses BRE — escape +. Database name is unquoted unless it needs
        # backquoting; `many_tables` is a plain identifier so no backticks appear.
        # Format: "Shut down N tables in many_tables in M ms"
        shutdown_log = node.grep_in_log(
            f"Shut down [0-9][0-9]* tables in {DATABASE} in [0-9][0-9]* ms",
            only_latest=True,
        )
        assert shutdown_log, (
            "Did not see the expected per-database shutdown log line — "
            "the parallel shutdown path may not have run."
        )

        match = re.search(rf"Shut down (\d+) tables in {DATABASE} in (\d+) ms", shutdown_log)
        assert match, f"Could not parse the shutdown log line: {shutdown_log!r}"
        assert int(match.group(1)) == NUM_TABLES

        # Server-reported drain duration. With the per-table sleep this proves the tables
        # were shut down in parallel: sequential would be ~NUM_TABLES seconds.
        shutdown_ms = int(match.group(2))
        assert shutdown_ms < MAX_EXPECTED_SHUTDOWN_MS, (
            f"Shutdown drain took {shutdown_ms} ms >= {MAX_EXPECTED_SHUTDOWN_MS} ms with per-table "
            "delay injection — that looks closer to sequential than the expected parallel path."
        )

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
