import base64
import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_dist = cluster.add_instance(
    "node_dist",
    main_configs=["configs/remote_servers.xml"],
    stay_alive=True,
)
node_shard = cluster.add_instance(
    "node_shard",
    main_configs=["configs/remote_servers.xml"],
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
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables "
        f"WHERE database = 'default' AND name = '{table}'"
    ).strip().rstrip("/")
    # The actual subdirectory name (shardN_replicaM, shardN_all_replicas, or the
    # legacy `user@host:port` form) depends on the cluster definition and the
    # `use_compact_format_in_distributed_parts_names` setting, so discover it
    # from disk rather than hardcoding.
    listing = node.exec_in_container(
        ["bash", "-c", f"ls -1 {data_path}"]
    ).strip().splitlines()
    subdirs = [name for name in listing if name and name != "broken" and name != "tmp"]
    assert len(subdirs) == 1, (
        f"Expected exactly one async-insert queue subdir under {data_path}, got: {listing}"
    )
    return f"{data_path}/{subdirs[0]}"


def test_recover_batch_with_broken_middle_file(started_cluster):
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/101745.

    `DistributedAsyncInsertBatch::recoverBatch` iterated over `files` but read
    `files.back()` inside the loop, so it only validated the last file's header.
    When the last file was intact but a middle file was corrupted, `recoverBatch`
    returned `true`, then `sendBatch` failed on the broken file and the entire
    batch (including the intact files) was moved to `broken/`, silently losing
    the rows from the intact files.

    The fix makes the loop read the current `file` variable instead, so the
    broken middle file is detected during recovery, `recoverBatch` returns
    `false`, `current_batch.txt` is removed, and the intact files are
    re-processed individually so their rows reach the remote shard.
    """
    node_dist.query("DROP TABLE IF EXISTS dist SYNC")
    node_dist.query("DROP TABLE IF EXISTS local SYNC")
    node_shard.query("DROP TABLE IF EXISTS local SYNC")

    node_shard.query("CREATE TABLE local (x UInt32) ENGINE = MergeTree ORDER BY tuple()")
    # `local` on `node_dist` is only used to satisfy the Distributed engine signature;
    # all rows are routed to `node_shard` via the cluster definition.
    node_dist.query("CREATE TABLE local (x UInt32) ENGINE = MergeTree ORDER BY tuple()")
    node_dist.query(
        "CREATE TABLE dist (x UInt32) ENGINE = Distributed(test_cluster, default, local)"
        " SETTINGS background_insert_batch = 1, background_insert_split_batch_on_failure = 0"
    )

    # Stop sends so the inserts accumulate as separate `.bin` files in the queue.
    node_dist.query("SYSTEM STOP DISTRIBUTED SENDS dist")

    # Three separate inserts -> three `.bin` files (1.bin, 2.bin, 3.bin).
    node_dist.query("INSERT INTO dist VALUES (1)")
    node_dist.query("INSERT INTO dist VALUES (2)")
    node_dist.query("INSERT INTO dist VALUES (3)")

    queue_path = _shard_queue_path(node_dist, "dist")
    bin_files = node_dist.exec_in_container(
        ["bash", "-c", f"ls {queue_path}/*.bin | sort"]
    ).strip().splitlines()
    assert len(bin_files) == 3, f"Expected 3 .bin files, got: {bin_files}"

    # Extract the numeric indices of the .bin files (e.g. 1, 2, 3) and pick the
    # middle one to corrupt.
    indices = sorted(int(os.path.basename(p).removesuffix(".bin")) for p in bin_files)
    middle_idx = indices[1]
    middle_file = f"{queue_path}/{middle_idx}.bin"

    # Simulate the abnormal-shutdown state described in the issue: a
    # `current_batch.txt` referencing all three files, plus a corrupted middle
    # file. The server must observe this state on startup.
    node_dist.stop_clickhouse()

    current_batch_lines = "".join(f"{i}\n" for i in indices)
    encoded = base64.b64encode(current_batch_lines.encode()).decode()
    node_dist.exec_in_container(
        [
            "bash",
            "-c",
            f"echo {encoded} | base64 --decode > {queue_path}/current_batch.txt",
        ]
    )
    # Truncate the middle file so reading its header fails with
    # ATTEMPT_TO_READ_AFTER_EOF, which `isDistributedSendBroken` treats as broken.
    node_dist.exec_in_container(["truncate", "-s", "0", middle_file])

    node_dist.start_clickhouse()

    # Drain the queue. With the fix, `recoverBatch` detects the broken middle
    # file, returns false, `current_batch.txt` is removed, and the surviving
    # files are re-processed individually.
    node_dist.query("SYSTEM START DISTRIBUTED SENDS dist")
    node_dist.query("SYSTEM FLUSH DISTRIBUTED dist")

    # The two intact files must have made it through; the corrupted file is
    # moved to `broken/`.
    rows = node_shard.query("SELECT x FROM local ORDER BY x").strip().splitlines()
    assert rows == [str(i) for i in indices if i != middle_idx], (
        f"Expected the two intact files to be delivered, got: {rows}"
    )

    broken_files = node_dist.exec_in_container(
        ["bash", "-c", f"ls {queue_path}/broken/ 2>/dev/null || true"]
    ).strip().splitlines()
    assert f"{middle_idx}.bin" in broken_files, (
        f"Expected {middle_idx}.bin in broken/, got: {broken_files}"
    )
