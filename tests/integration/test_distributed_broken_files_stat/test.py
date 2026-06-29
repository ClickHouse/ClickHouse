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


def test_recover_batch_validates_each_file(started_cluster):
    """
    Regression test for `DistributedAsyncInsertBatch::recoverBatch`.

    When recovering a previously serialized batch (`current_batch.txt`) after a
    restart, `recoverBatch` validated `files.back()` -- the last file -- on
    every loop iteration instead of the current `file`. A broken file in any
    position but the last therefore went undetected: `recoverBatch` returned
    `true`, the whole batch was sent, and the broken file made `sendBatch`
    throw. With `background_insert_split_batch_on_failure = 0` (the default) the
    entire batch -- including the *valid* files -- was then marked broken,
    losing their data.

    With the fix `recoverBatch` inspects each file, detects the broken one,
    discards the stale batch, and lets the files be reprocessed individually:
    only the broken file is marked broken and the valid file is delivered.
    """
    node.query("DROP TABLE IF EXISTS recover_dist SYNC")
    node.query("DROP TABLE IF EXISTS recover_local SYNC")

    node.query(
        "CREATE TABLE recover_local (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    node.query(
        "CREATE TABLE recover_dist (x UInt32)"
        " ENGINE = Distributed(test_cluster, default, recover_local)"
        " SETTINGS background_insert_batch = 1"
    )

    # Stop sends so the inserts accumulate as separate `.bin` files we can
    # corrupt and reference from a hand-crafted `current_batch.txt`. The stop
    # is in-memory and does not survive the restart below, so the background
    # sender resumes -- and triggers batch recovery -- as soon as the server
    # starts again.
    node.query("SYSTEM STOP DISTRIBUTED SENDS recover_dist")

    insert_settings = {"prefer_localhost_replica": 0}
    # The first insert gets the lower file index (it will be corrupted and
    # listed *first* in the batch, so it is not the last file); the second
    # insert gets the higher index and stays valid.
    node.query("INSERT INTO recover_dist VALUES (10)", settings=insert_settings)
    node.query("INSERT INTO recover_dist VALUES (20)", settings=insert_settings)

    queue_path = _shard_queue_path(node, "recover_dist")
    indices = sorted(
        int(line.rsplit("/", 1)[-1].split(".", 1)[0])
        for line in node.exec_in_container(
            ["bash", "-c", f"ls {queue_path}/*.bin"]
        )
        .strip()
        .splitlines()
    )
    assert len(indices) == 2, f"Expected 2 .bin files, got: {indices}"
    broken_idx, good_idx = indices[0], indices[1]

    # Stop the server so we can craft the on-disk batch state by hand.
    node.stop_clickhouse(kill=True)

    # Corrupt the lower-index file. A leading byte `0x47` ('G') is read as a
    # query size of 71, and `readStrict` then fails with `CANNOT_READ_ALL_DATA`
    # -- one of the codes `isDistributedSendBroken` treats as a broken file.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"printf 'GARBAGE_BROKEN_HEADER' > {queue_path}/{broken_idx}.bin",
        ]
    )

    # Recreate `current_batch.txt` listing the broken file first and the valid
    # file last, so the old (buggy) code -- which always re-read `files.back()`
    # -- would have validated only the valid trailing file and missed the
    # corruption entirely.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '%s\\n%s\\n' {broken_idx} {good_idx} > {queue_path}/current_batch.txt",
        ]
    )

    node.start_clickhouse()

    # With the fix the valid file is delivered to the local table ...
    assert_eq_with_retry(node, "SELECT count() FROM recover_local", "1\n")
    assert (
        node.query("SELECT x FROM recover_local").strip() == "20"
    ), "The surviving row must come from the valid (non-corrupted) file"

    # ... and exactly one file (the corrupted one) is marked broken, rather than
    # the whole batch being discarded as it was before the fix.
    assert_eq_with_retry(
        node,
        "SELECT broken_data_files FROM system.distribution_queue "
        "WHERE database = 'default' AND table = 'recover_dist'",
        "1\n",
    )
