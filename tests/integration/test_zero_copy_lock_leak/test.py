#!/usr/bin/env python3

"""
Regression test: zero-copy lock nodes must be removed when a part is removed.

StorageReplicatedMergeTree::unlockSharedData() decides whether to remove a
part's zero-copy lock (and shared blobs) based on:

    if (part.getDataPartStorage().existsFile(FILE_FOR_REFERENCES_CHECK))  // "checksums.txt"
        ... reach unlockSharedDataByID() and remove the lock ...
    else
        // "looks temporary, because checksums.txt file doesn't exists, blobs can be removed"
        return {true, {}};                                               // <-- lock NOT removed

In DataPartStorageOnDiskBase::remove() the can-remove decision runs *after*
the part directory is renamed to delete_tmp_<...> (moveDirectory). Once a
commit stopped updating `part_dir` to follow that rename, existsFile() began
checking the original (now non-existent) path, returned false, took the
"looks temporary" branch, skipped unlockSharedDataByID(), and leaked the
persistent /clickhouse/zero_copy/... lock node (and deleted the shared blob
without consulting the lock).

This is NOT specific to packed parts: DataPartStorageOnDiskFull::existsFile()
is a pure part_dir path check, so any storage type is affected.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

TABLE_ZK_PATH = "/clickhouse/tables/zc_leak"
ZC_ROOT = "/clickhouse/zero_copy/zero_copy_s3"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        for name in ("node1", "node2"):
            cluster.add_instance(
                name,
                main_configs=["configs/storage_conf.xml"],
                macros={"replica": name},
                with_minio=True,
                with_zookeeper=True,
            )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create(node, replica):
    node.query(
        f"""
        CREATE TABLE zc_leak (a Int64, b String)
        ENGINE = ReplicatedMergeTree('{TABLE_ZK_PATH}', '{replica}')
        ORDER BY a
        SETTINGS
            storage_policy = 's3',
            allow_remote_fs_zero_copy_replication = 1,
            -- make obsolete-part cleanup (which triggers unlockSharedData) fast:
            old_parts_lifetime = 1,
            cleanup_delay_period = 1,
            max_cleanup_delay_period = 1
        """
    )


def _shared_id(node):
    return node.query(
        f"SELECT value FROM system.zookeeper "
        f"WHERE path = '{TABLE_ZK_PATH}' AND name = 'table_shared_id'"
    ).strip()


def _count_part_locks(node, shared_id):
    # Number of per-part lock nodes under the table's zero-copy subtree.
    # Tolerate the subtree (or its parent) not existing yet / anymore -> 0.
    out = node.query(
        f"""
        SELECT count()
        FROM system.zookeeper
        WHERE path = '{ZC_ROOT}/{shared_id}'
        SETTINGS allow_unrestricted_reads_from_keeper = 1
        """,
        ignore_error=True,
    ).strip()
    return int(out) if out.isdigit() else 0


def test_zero_copy_lock_removed_on_part_removal(started_cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    for n in (node1, node2):
        n.query("DROP TABLE IF EXISTS zc_leak SYNC")

    _create(node1, "node1")
    _create(node2, "node2")

    # One part on node1, replicated (zero-copy fetch) to node2.
    node1.query("INSERT INTO zc_leak SELECT number, toString(number) FROM numbers(16)")
    node2.query("SYSTEM SYNC REPLICA zc_leak", timeout=30)

    shared_id = _shared_id(node1)
    assert shared_id, "table_shared_id not found in ZooKeeper"

    # The zero-copy lock for the part must have been created.
    assert _count_part_locks(node1, shared_id) >= 1, "no zero-copy lock was created"

    # Remove the part. Obsolete-part cleanup then calls unlockSharedData, which
    # runs after the delete_tmp rename inside DataPartStorageOnDiskBase::remove().
    node1.query("TRUNCATE TABLE zc_leak")
    node2.query("SYSTEM SYNC REPLICA zc_leak", timeout=30)

    # After the parts are gone everywhere, every per-part zero-copy lock node
    # must have been removed. On an affected build they leak and this stays > 0.
    # (_count_part_locks tolerates the subtree itself being removed -> 0 = clean.)
    deadline = time.time() + 60
    remaining = None
    while time.time() < deadline:
        remaining = max(
            _count_part_locks(node1, shared_id),
            _count_part_locks(node2, shared_id),
        )
        if remaining == 0:
            break
        time.sleep(1)

    assert remaining == 0, (
        f"zero-copy lock leaked: {remaining} per-part node(s) still under "
        f"{ZC_ROOT}/{shared_id} after all parts were removed"
    )

    for n in (node1, node2):
        n.query("DROP TABLE IF EXISTS zc_leak SYNC")
