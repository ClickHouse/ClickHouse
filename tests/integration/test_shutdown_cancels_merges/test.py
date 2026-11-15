#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", stay_alive=True)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/storage_conf.xml"],
    with_zookeeper=True,
    with_minio=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/storage_conf.xml"],
    user_configs=["configs/users_small_fetch_buffer.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shutdown_cancels_running_merge(started_cluster):
    """A graceful shutdown must cancel in-flight merges instead of waiting for
    them to finish. The `merge_tree_sequential_source_sleep_before_read`
    failpoint makes every reading step of the merge sleep for 10 seconds, so
    the merge of 16 parts runs for at least 160 seconds. Without cancellation
    the server cannot stop before the timeout below (the background executor's
    `wait` does not interrupt running tasks, and per-storage cancellation
    happens only after that wait). See the `Possible deadlock on shutdown`
    stress test failures where a merge applying patch parts blocked shutdown:
    https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=a013c79d6ac80373b820381527ea85823ab03834&name_0=MasterCI&name_1=Stress%20test%20%28azure%2C%20amd_msan%29
    """
    node.query("""
        CREATE TABLE t_shutdown_merge (k UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_parts_to_merge_at_once = 16
        """)

    node.query("SYSTEM STOP MERGES t_shutdown_merge")

    for i in range(16):
        node.query(
            f"INSERT INTO t_shutdown_merge SELECT number + {i} * 10000, number FROM numbers(10000)"
        )

    node.query("SYSTEM ENABLE FAILPOINT merge_tree_sequential_source_sleep_before_read")
    node.query("SYSTEM START MERGES t_shutdown_merge")

    # Wait until the background merge picks up the parts. With
    # min_parts_to_merge_at_once = 16 the only merge the selector can assign
    # takes all 16 parts at once.
    for _ in range(300):
        num_parts = node.query(
            "SELECT num_parts FROM system.merges WHERE table = 't_shutdown_merge'"
        ).strip()
        if num_parts:
            assert int(num_parts) == 16
            break
        time.sleep(0.2)
    else:
        raise Exception("The background merge did not start")

    start = time.time()
    assert node.stop_clickhouse(stop_wait_sec=90) is True
    elapsed = time.time() - start
    assert elapsed < 90, f"Shutdown took {elapsed} seconds"

    node.start_clickhouse()

    # The merge was aborted, the source parts and the data are intact.
    assert node.query("SELECT count() FROM t_shutdown_merge") == "160000\n"

    node.query("DROP TABLE t_shutdown_merge SYNC")


def test_shutdown_cancels_running_fetch(started_cluster):
    """A graceful shutdown must also cancel in-flight fetches of parts from
    other replicas: a fetch runs inside a single step of a background task, so
    the executors' `wait` would otherwise block for the whole download. The
    sender is throttled to 1 MiB/s and the part is ~150 MB of incompressible
    data, so without cancellation the fetch holds the shutdown for ~150
    seconds.
    """
    node1.query("""
        CREATE TABLE t_shutdown_fetch (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_shutdown_fetch', 'r1')
        ORDER BY k
        SETTINGS max_replicated_sends_network_bandwidth = 1048576
        """)
    node1.query(
        "INSERT INTO t_shutdown_fetch SELECT number, randomString(1000) FROM numbers(150000)"
    )

    node2.query("""
        CREATE TABLE t_shutdown_fetch (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_shutdown_fetch', 'r2')
        ORDER BY k
        """)

    for _ in range(300):
        fetches = node2.query(
            "SELECT count() FROM system.replicated_fetches WHERE table = 't_shutdown_fetch'"
        ).strip()
        if fetches != "0":
            break
        time.sleep(0.2)
    else:
        raise Exception("The fetch did not start")

    start = time.time()
    assert node2.stop_clickhouse(stop_wait_sec=90) is True
    elapsed = time.time() - start
    assert elapsed < 90, f"Shutdown took {elapsed} seconds"

    # The fetch was aborted by the cancellation of the `ReplicatedFetchList`
    # entry (the `ABORTED` message includes the part name), not by anything else.
    assert node2.contains_in_log("Fetching of part all_.* was cancelled")

    node2.start_clickhouse()

    # After the restart the fetch is retried; dropping the table cancels it
    # again, through the per-storage blockers.
    node2.query("DROP TABLE t_shutdown_fetch SYNC")
    node1.query("DROP TABLE t_shutdown_fetch SYNC")


def test_shutdown_cancels_running_zero_copy_fetch(started_cluster):
    """The zero-copy fetch branch (`remote_fs_metadata`) must participate in
    the shutdown cancellation too: the `ReplicatedFetchList` entry is created
    and the read callback is installed before the branch on the fetch mode.
    The `replicated_sends_sleep_before_file_send` failpoint makes the sender
    flush and then sleep 5 seconds before each metadata file, and the part is
    wide with 20 columns (several dozen files), so without cancellation the
    metadata transfer holds the shutdown for well over 90 seconds. The
    receiver observes the cancellation between the refills of its read
    buffer, and the whole metadata stream is a few KB, so node4 shrinks
    `max_read_buffer_size_remote_fs` to 1 KiB (see the comment in
    `users_small_fetch_buffer.xml`).
    """
    columns = ", ".join(f"v{i} String" for i in range(20))
    node3.query(f"""
        CREATE TABLE t_shutdown_zc_fetch (k UInt64, {columns})
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_shutdown_zc_fetch', 'r1')
        ORDER BY k
        SETTINGS storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1,
            min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
        """)
    node3.query(
        f"INSERT INTO t_shutdown_zc_fetch SELECT * FROM generateRandom('k UInt64, {columns}') LIMIT 100"
    )

    node3.query("SYSTEM ENABLE FAILPOINT replicated_sends_sleep_before_file_send")

    node4.query(f"""
        CREATE TABLE t_shutdown_zc_fetch (k UInt64, {columns})
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_shutdown_zc_fetch', 'r2')
        ORDER BY k
        SETTINGS storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1,
            min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
        """)

    for _ in range(300):
        fetches = node4.query(
            "SELECT count() FROM system.replicated_fetches WHERE table = 't_shutdown_zc_fetch'"
        ).strip()
        if fetches != "0":
            break
        time.sleep(0.2)
    else:
        raise Exception("The fetch did not start")

    start = time.time()
    assert node4.stop_clickhouse(stop_wait_sec=90) is True
    elapsed = time.time() - start
    assert elapsed < 90, f"Shutdown took {elapsed} seconds"

    # The fetch took the zero-copy branch and was aborted by the cancellation
    # of the `ReplicatedFetchList` entry (the `ABORTED` message).
    assert node4.contains_in_log("metadata onto disk")
    assert node4.contains_in_log("Fetching of part all_.* was cancelled")

    node3.query("SYSTEM DISABLE FAILPOINT replicated_sends_sleep_before_file_send")
    node4.start_clickhouse()

    node4.query("DROP TABLE t_shutdown_zc_fetch SYNC")
    node3.query("DROP TABLE t_shutdown_zc_fetch SYNC")
