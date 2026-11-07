"""A graceful shutdown must hand a `Buffer` table's rows over to its destination.

Databases shut down one at a time in name order, so a destination in an earlier-sorting database is
already gone when the `Buffer` prepares for shutdown, and a chain of `Buffer` tables moves rows at
most one link per pass. Both losses are silent: the client saw the `INSERT` succeed.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/remote_servers.xml"], stay_alive=True
)
shard = cluster.add_instance("shard", stay_alive=True)

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


def create_distributed_over_delay_threshold(database):
    # A block queued while the sends are stopped keeps the pending bytes over the threshold.
    node.query(f"CREATE DATABASE {database}")
    shard.query(f"CREATE DATABASE {database}")
    shard.query(f"CREATE TABLE {database}.data (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(
        f"CREATE TABLE {database}.dist (x UInt64) "
        f"ENGINE = Distributed(shard_cluster, {database}, data) "
        "SETTINGS bytes_to_delay_insert = 1, max_delay_to_insert = 3600"
    )
    node.query(f"SYSTEM STOP DISTRIBUTED SENDS {database}.dist")
    node.query(f"INSERT INTO {database}.dist SELECT number FROM numbers(10)")


def restart_node_while_shard_is_down():
    # Nothing can drain the queue of `dist` during the shutdown.
    shard.stop_clickhouse(kill=True)
    node.stop_clickhouse(stop_wait_sec=60)
    assert not node.clickhouse_forced_stop
    node.start_clickhouse()
    shard.start_clickhouse()


def drop_database(database):
    node.query(f"DROP DATABASE {database} SYNC")
    shard.query(f"DROP DATABASE {database} SYNC")


def test_buffer_into_distributed_over_delay_threshold(started_cluster):
    # The `Buffer` holds 100 rows for `dist`, whose stopped queue holds 10 more.
    create_distributed_over_delay_threshold("dist_buf")
    node.query(
        f"CREATE TABLE dist_buf.buf (x UInt64) ENGINE = Buffer(dist_buf, dist, {BUFFER_THRESHOLDS})"
    )
    node.query("INSERT INTO dist_buf.buf SELECT number + 10 FROM numbers(100)")
    assert node.query("SELECT count() FROM dist_buf.buf") == "100\n"

    restart_node_while_shard_is_down()

    node.query("SYSTEM FLUSH DISTRIBUTED dist_buf.dist")
    assert shard.query("SELECT count() FROM dist_buf.data") == "110\n"

    drop_database("dist_buf")


def test_async_insert_into_distributed_over_delay_threshold(started_cluster):
    # The asynchronous insert queue holds 5 rows for `dist`, whose stopped queue holds 10 more.
    create_distributed_over_delay_threshold("dist_async")
    node.query(
        "INSERT INTO dist_async.dist SETTINGS async_insert = 1, wait_for_async_insert = 0, "
        "async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_min_ms = 600000, "
        "async_insert_busy_timeout_max_ms = 600000 VALUES (1000), (1001), (1002), (1003), (1004)"
    )
    assert (
        node.query(
            "SELECT count() FROM system.asynchronous_inserts "
            "WHERE database = 'dist_async' AND table = 'dist'"
        )
        == "1\n"
    )

    restart_node_while_shard_is_down()

    node.query("SYSTEM FLUSH DISTRIBUTED dist_async.dist")
    # A delivered block keeps the settings of its `INSERT`, so it is queued again.
    shard.query("SYSTEM FLUSH ASYNC INSERT QUEUE")
    assert shard.query("SELECT count() FROM dist_async.data") == "15\n"

    drop_database("dist_async")
