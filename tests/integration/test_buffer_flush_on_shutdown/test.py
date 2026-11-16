"""A graceful shutdown must hand a `Buffer` table's rows over to its destination.

Databases shut down one at a time in name order, so a destination in an earlier-sorting database is
already gone when the `Buffer` prepares for shutdown, and a chain of `Buffer` tables moves rows at
most one link per pass. Both losses are silent: the client saw the `INSERT` succeed.
"""

import threading
import time

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


def create_buffer_over_merge_tree(database):
    # Only a graceful shutdown moves these rows to `mt`.
    node.query(f"CREATE TABLE {database}.mt (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query(
        f"CREATE TABLE {database}.buf (x UInt64) ENGINE = Buffer({database}, mt, {BUFFER_THRESHOLDS})"
    )
    node.query(f"INSERT INTO {database}.buf SELECT number FROM numbers(100)")
    assert node.query(f"SELECT count() FROM {database}.mt") == "0\n"


def wait_until(query, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.query(query) == expected:
            return
        time.sleep(0.1)
    assert False, f"{query} did not return {expected!r} in {timeout} s"


def start_delayed_insert(database, query_id, table="dist", settings=""):
    errors = []

    def insert():
        try:
            node.http_query(
                f"INSERT INTO {database}.{table} SETTINGS async_insert = 0{settings} VALUES (1)",
                method="POST",
                params={"query_id": query_id},
                timeout=600,
            )
        except Exception as e:
            errors.append(str(e))

    thread = threading.Thread(target=insert, daemon=True)
    thread.start()
    # A one-row `INSERT` still running after a second is waiting in the delay of `dist`.
    wait_until(
        f"SELECT count() FROM system.processes WHERE query_id = '{query_id}' AND elapsed >= 1",
        "1\n",
    )
    return thread, errors


def test_client_insert_delayed_over_threshold_at_shutdown(started_cluster):
    create_distributed_over_delay_threshold("dist_cli")
    create_buffer_over_merge_tree("dist_cli")
    thread, errors = start_delayed_insert("dist_cli", "dist_cli_delayed_insert")

    restart_node_while_shard_is_down()

    thread.join(60)
    assert not thread.is_alive()
    assert errors
    assert node.query("SELECT count() FROM dist_cli.mt") == "100\n"
    node.query("SYSTEM FLUSH DISTRIBUTED dist_cli.dist")
    # The cancelled `INSERT` still queued its block.
    assert shard.query("SELECT count() FROM dist_cli.data") == "11\n"

    drop_database("dist_cli")


def test_refresh_delayed_over_threshold_at_shutdown(started_cluster):
    create_distributed_over_delay_threshold("dist_rmv")
    create_buffer_over_merge_tree("dist_rmv")
    node.query(
        "CREATE MATERIALIZED VIEW dist_rmv.rmv REFRESH EVERY 1 HOUR "
        "APPEND TO dist_rmv.dist AS SELECT 1 AS x"
    )
    # A refresh of `SELECT 1` still running after 2 seconds is waiting in the delay of `dist`.
    refresh_status = "SELECT status FROM system.view_refreshes WHERE database = 'dist_rmv' AND view = 'rmv'"
    wait_until(refresh_status, "Running\n")
    time.sleep(2)
    assert node.query(refresh_status) == "Running\n"

    restart_node_while_shard_is_down()

    assert node.query("SELECT count() FROM dist_rmv.mt") == "100\n"

    drop_database("dist_rmv")


def test_kill_delayed_insert(started_cluster):
    create_distributed_over_delay_threshold("dist_kill")
    thread, errors = start_delayed_insert("dist_kill", "dist_kill_delayed_insert")

    node.query(
        "KILL QUERY WHERE query_id = 'dist_kill_delayed_insert' SYNC", timeout=60
    )

    thread.join(60)
    assert not thread.is_alive()
    assert len(errors) == 1 and "QUERY_WAS_CANCELLED" in errors[0], errors
    node.query("SYSTEM START DISTRIBUTED SENDS dist_kill.dist")
    node.query("SYSTEM FLUSH DISTRIBUTED dist_kill.dist")
    assert shard.query("SELECT count() FROM dist_kill.data") == "11\n"

    drop_database("dist_kill")


@pytest.mark.parametrize("foreground", [0, 1])
def test_kill_insert_delayed_by_local_shard(started_cluster, foreground):
    # `outer` writes its local shard into `dist` through a nested pipeline.
    database = f"dist_nested_{foreground}"
    query_id = f"{database}_delayed_insert"
    create_distributed_over_delay_threshold(database)
    node.query(
        f"CREATE TABLE {database}.outer (x UInt64) "
        f"ENGINE = Distributed(local_cluster, {database}, dist)"
    )
    thread, errors = start_delayed_insert(
        database,
        query_id,
        table="outer",
        settings=f", prefer_localhost_replica = 1, distributed_foreground_insert = {foreground}",
    )

    node.query(f"KILL QUERY WHERE query_id = '{query_id}' SYNC", timeout=60)

    thread.join(60)
    assert not thread.is_alive()
    assert len(errors) == 1 and "QUERY_WAS_CANCELLED" in errors[0], errors

    drop_database(database)


def test_async_insert_flush_cancelled_in_delay_at_shutdown(started_cluster):
    # The rows were acknowledged before their flush started to wait in the delay of `dist`.
    create_distributed_over_delay_threshold("dist_inflight")
    node.query(
        "INSERT INTO dist_inflight.dist SETTINGS async_insert = 1, wait_for_async_insert = 0, "
        "async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_min_ms = 100, "
        "async_insert_busy_timeout_max_ms = 100 VALUES (1000), (1001), (1002), (1003), (1004)"
    )
    wait_until(
        "SELECT count() FROM system.asynchronous_inserts "
        "WHERE database = 'dist_inflight' AND table = 'dist'",
        "0\n",
    )
    wait_until(
        "SELECT count() FROM system.processes "
        "WHERE is_internal AND position(query, 'dist_inflight') > 0 AND elapsed >= 1",
        "1\n",
    )

    restart_node_while_shard_is_down()

    node.query("SYSTEM FLUSH DISTRIBUTED dist_inflight.dist")
    shard.query("SYSTEM FLUSH ASYNC INSERT QUEUE")
    assert shard.query("SELECT count() FROM dist_inflight.data") == "15\n"

    drop_database("dist_inflight")
