#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

# Buffer thresholds set far away so nothing flushes on its own before shutdown.
BUFFER_THRESHOLDS = "1, 3600, 3600, 1000000000, 1000000000, 1000000000000, 1000000000000"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_buffer_chain_flushed_on_graceful_shutdown(started_cluster):
    """
    Regression test for issue #116803: a same-database Buffer -> Buffer chain silently
    losing acked rows on a graceful (SIGTERM) shutdown.

    Tables are shut down in table-name order, so the destination buffer `a_target` is
    prepared (and flushed, while empty) before the source buffer `z_source` flushes its
    rows into `a_target`'s in-memory buffer. `StorageBuffer` has no shutdown() override
    and no destructor flush, so without a second prepare pass those rows are destroyed
    silently when the storages are dropped. A correct shutdown flushes the whole chain
    down to the final MergeTree destination.
    """
    node.query("CREATE DATABASE d")
    node.query("CREATE TABLE d.mt (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(f"CREATE TABLE d.a_target (x UInt64) ENGINE = Buffer(d, mt, {BUFFER_THRESHOLDS})")
    node.query(f"CREATE TABLE d.z_source (x UInt64) ENGINE = Buffer(d, a_target, {BUFFER_THRESHOLDS})")

    try:
        node.query("INSERT INTO d.z_source VALUES (0), (1), (2)")

        # Rows are acked and visible through the chain, but still buffered (not yet in mt).
        assert node.query("SELECT count() FROM d.z_source").strip() == "3"
        assert node.query("SELECT count() FROM d.mt").strip() == "0"

        # Graceful shutdown (SIGTERM) — the same path production hits — then restart.
        node.stop_clickhouse(kill=False, stop_wait_sec=60)
        node.start_clickhouse()

        # The three acked rows must have reached the final destination.
        assert node.query("SELECT count() FROM d.mt").strip() == "3"
    finally:
        if node.get_process_pid("clickhouse") is not None:
            node.query("DROP DATABASE IF EXISTS d SYNC")
