"""A graceful shutdown must hand a `Buffer` table's rows over to its destination.

Databases shut down one at a time in name order, so a destination in an earlier-sorting database is
already gone when the `Buffer` prepares for shutdown, and a chain of `Buffer` tables moves rows at
most one link per pass. Both losses are silent: the client saw the `INSERT` succeed.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=[], stay_alive=True)

# Far away, so nothing flushes on its own before the shutdown.
BUFFER_THRESHOLDS = "1, 100000, 100000, 1000000, 1000000000, 10000000, 1000000000"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_buffer_destination_in_another_database(started_cluster):
    # `za` sorts before `zb`, so `za` is shut down (and its tables released) first.
    node.query("CREATE DATABASE IF NOT EXISTS za")
    node.query("CREATE DATABASE IF NOT EXISTS zb")
    node.query("CREATE TABLE za.mt (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(f"CREATE TABLE zb.buf (x UInt64) ENGINE = Buffer(za, mt, {BUFFER_THRESHOLDS})")

    node.query("INSERT INTO zb.buf SELECT number FROM numbers(100)")
    assert node.query("SELECT count() FROM za.mt") == "0\n"

    node.restart_clickhouse()

    assert node.query("SELECT count() FROM za.mt") == "100\n"

    node.query("DROP DATABASE zb SYNC")
    node.query("DROP DATABASE za SYNC")


def test_chain_of_buffer_tables(started_cluster):
    # Every destination sorts before its source, which is the order the shutdown walks.
    node.query("CREATE DATABASE IF NOT EXISTS d3")
    node.query("CREATE TABLE d3.mt (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(f"CREATE TABLE d3.a1 (x UInt64) ENGINE = Buffer(d3, mt, {BUFFER_THRESHOLDS})")
    node.query(f"CREATE TABLE d3.b2 (x UInt64) ENGINE = Buffer(d3, a1, {BUFFER_THRESHOLDS})")
    node.query(f"CREATE TABLE d3.c3 (x UInt64) ENGINE = Buffer(d3, b2, {BUFFER_THRESHOLDS})")

    node.query("INSERT INTO d3.c3 SELECT number FROM numbers(100)")
    assert node.query("SELECT count() FROM d3.mt") == "0\n"
    assert node.query("SELECT count() FROM d3.c3") == "100\n"

    node.restart_clickhouse()

    assert node.query("SELECT count() FROM d3.mt") == "100\n"

    node.query("DROP DATABASE d3 SYNC")
