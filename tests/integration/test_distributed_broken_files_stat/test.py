import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Single-node setup: the Distributed table points back at the same node via the
# `test_cluster` definition, and each `INSERT` is issued with
# `prefer_localhost_replica = 0` so the engine routes through the async-insert
# queue (creating `.bin` files on disk) instead of taking the local shortcut.
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _shard_queue_path(node, table):
    """Return the per-shard async-insert queue directory inside the container."""
    data_path = (
        node.query(
            f"SELECT arrayElement(data_paths, 1) FROM system.tables "
            f"WHERE database = 'default' AND name = '{table}'"
        )
        .strip()
        .rstrip("/")
    )
    # The actual subdirectory name (shardN_replicaM, shardN_all_replicas, or the
    # legacy `user@host:port` form) depends on the cluster definition and the
    # `use_compact_format_in_distributed_parts_names` setting, so discover it
    # from disk rather than hardcoding.
    listing = (
        node.exec_in_container(["bash", "-c", f"ls -1 {data_path}"])
        .strip()
        .splitlines()
    )
    subdirs = [name for name in listing if name and name not in ("broken", "tmp")]
    assert (
        len(subdirs) == 1
    ), f"Expected exactly one async-insert queue subdir under {data_path}, got: {listing}"
    return f"{data_path}/{subdirs[0]}"


def test_broken_files_count_initialized_from_disk(started_cluster):
    """
    Regression test for the `broken_files` counter in
    `DistributedAsyncInsertDirectoryQueue::initializeFilesFromDisk`.

    When the server starts, `initializeFilesFromDisk` scans the per-shard
    `broken/` subdirectory and accumulated `broken_bytes_count` for every broken
    `.bin` file found there, but it never incremented the `broken_files`
    counter. As a result `system.distribution_queue.broken_data_files` (and the
    `BrokenDistributedFilesToInsert` metric) was always reported as 0 for files
    that were already in `broken/` when the server started, even though
    `broken_data_compressed_bytes` was non-zero -- an inconsistency that hides
    accumulating broken files from monitoring.
    """
    node.query("DROP TABLE IF EXISTS dist SYNC")
    node.query("DROP TABLE IF EXISTS local SYNC")

    node.query("CREATE TABLE local (x UInt32) ENGINE = MergeTree ORDER BY tuple()")
    node.query(
        "CREATE TABLE dist (x UInt32) ENGINE = Distributed(test_cluster, default, local)"
        " SETTINGS background_insert_batch = 1"
    )

    # Stop sends so the inserts accumulate as separate `.bin` files in the queue.
    node.query("SYSTEM STOP DISTRIBUTED SENDS dist")

    # `prefer_localhost_replica = 0` forces the Distributed engine to treat the
    # local replica as remote and route the insert through the async-insert
    # queue (otherwise it would take the local shortcut and bypass `.bin` file
    # creation entirely).
    insert_settings = {"prefer_localhost_replica": 0}
    node.query("INSERT INTO dist VALUES (1)", settings=insert_settings)
    node.query("INSERT INTO dist VALUES (2)", settings=insert_settings)
    node.query("INSERT INTO dist VALUES (3)", settings=insert_settings)

    queue_path = _shard_queue_path(node, "dist")
    bin_files = (
        node.exec_in_container(["bash", "-c", f"ls {queue_path}/*.bin | sort"])
        .strip()
        .splitlines()
    )
    assert len(bin_files) == 3, f"Expected 3 .bin files, got: {bin_files}"

    # A hard kill keeps the queued `.bin` files on disk: a graceful shutdown
    # calls `StorageDistributed::flushAndPrepareForShutdown`, which flushes all
    # pending files to the remote shard regardless of `SYSTEM STOP DISTRIBUTED
    # SENDS`.
    node.stop_clickhouse(kill=True)

    # Move every queued file into the per-shard `broken/` subdirectory so the
    # server observes pre-existing broken files when it scans the queue on the
    # next startup (`initializeFilesFromDisk`). The shard directory stays
    # non-empty (its `broken/` subdir holds the files), so the directory queue
    # -- and its `system.distribution_queue` row -- is recreated on startup.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {queue_path}/broken && mv {queue_path}/*.bin {queue_path}/broken/",
        ]
    )
    broken_on_disk = node.exec_in_container(
        ["bash", "-c", f"ls {queue_path}/broken/*.bin | wc -l"]
    ).strip()
    assert (
        broken_on_disk == "3"
    ), f"Expected 3 files moved into broken/, got: {broken_on_disk}"

    node.start_clickhouse()

    # With the fix, `broken_data_files` reflects the three broken files found on
    # disk during initialization. Without it, the count stayed 0.
    assert_eq_with_retry(
        node,
        "SELECT broken_data_files FROM system.distribution_queue "
        "WHERE database = 'default' AND table = 'dist'",
        "3\n",
    )

    # `broken_data_compressed_bytes` was always accumulated correctly; assert it
    # is non-zero to show the file count was the sole inconsistency.
    broken_bytes_positive = node.query(
        "SELECT broken_data_compressed_bytes > 0 FROM system.distribution_queue "
        "WHERE database = 'default' AND table = 'dist'"
    ).strip()
    assert (
        broken_bytes_positive == "1"
    ), f"Expected non-zero broken_data_compressed_bytes, got: {broken_bytes_positive}"
