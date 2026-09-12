#!/usr/bin/env python3

"""
Regression test for the zero-copy bookkeeping of `invalidated_system_columns.txt`.

A mutation that does not touch a part clones it with hardlinks and records every hardlinked
file of the source part in the source part's zero-copy node in Keeper.
`unlockSharedDataByID` later uses that list as `files_not_to_remove`, so a file listed there
keeps the source blob alive.

`invalidated_system_columns.txt` must not appear in that list when the clone owns its own
copy of the file: `cloneAndLoadDataPart` propagates the source part's invalidated set, and a
non-empty set makes `freeze` remove the inherited file and write a fresh one. Recording it
anyway puts the source object into `files_not_to_remove`, so it is kept for a child that does
not reference it and leaks once the source part is gone.
"""

import pytest

from helpers.blobs import list_blobs, wait_blobs_synchronization
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

ZC_ROOT = "/clickhouse/zero_copy/zero_copy_s3"
SRC_ZK_PATH = "/clickhouse/tables/zc_inv_src"
DST_ZK_PATH = "/clickhouse/tables/zc_inv_dst"
INVALIDATED_FILE = "invalidated_system_columns.txt"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create(node, name, zk_path):
    node.query(f"""
        CREATE TABLE {name} (p UInt8, x UInt64, y UInt64)
        ENGINE = ReplicatedMergeTree('{zk_path}', '1')
        PARTITION BY p ORDER BY x
        SETTINGS
            storage_policy = 's3',
            allow_remote_fs_zero_copy_replication = 1,
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            -- the file is inherited by a hardlink only with Full part storage,
            -- which is where the destination rewrites it
            min_bytes_for_full_part_storage = 0,
            -- keep the source part (and its zero-copy node with the hardlink list) around
            -- long enough for the assertions below
            old_parts_lifetime = 3000
        """)


def _shared_id(node, zk_path):
    return node.query(
        f"SELECT value FROM system.zookeeper "
        f"WHERE path = '{zk_path}' AND name = 'table_shared_id'"
    ).strip()


def _hardlink_lists(node, shared_id):
    """Values of the per-part zero-copy nodes: the newline-separated hardlink lists."""
    out = node.query(
        f"""
        SELECT name, value
        FROM system.zookeeper
        WHERE path = '{ZC_ROOT}/{shared_id}'
        SETTINGS allow_unrestricted_reads_from_keeper = 1
        """,
        ignore_error=True,
    )
    return [line.split("\t", 1) for line in out.splitlines() if line]


def test_untouched_part_clone_does_not_pin_invalidated_system_columns(started_cluster):
    node1 = cluster.instances["node1"]

    # A table that is dropped last, so the blob comparison is not racing the very first
    # lazy writes of the s3 disk.
    node1.query(
        "CREATE TABLE warming_up (id Int8) ENGINE = MergeTree ORDER BY id "
        "SETTINGS storage_policy = 's3'"
    )
    node1.query("INSERT INTO warming_up VALUES (1)")
    objects_before = list_blobs(cluster.minio_client)

    _create(node1, "zc_inv_src", SRC_ZK_PATH)
    _create(node1, "zc_inv_dst", DST_ZK_PATH)

    node1.query("INSERT INTO zc_inv_src VALUES (1, 1, 0)")
    node1.query("INSERT INTO zc_inv_src VALUES (1, 2, 0)")
    # _block_number/_block_offset are written physically on a real merge.
    node1.query("OPTIMIZE TABLE zc_inv_src PARTITION 1 FINAL")

    # Adopting the part into another table disclaims its persisted _block_number/_block_offset,
    # so the adopted part carries invalidated_system_columns.txt.
    node1.query("ALTER TABLE zc_inv_dst REPLACE PARTITION 1 FROM zc_inv_src")
    assert node1.query("SELECT count() FROM zc_inv_dst").strip() == "2"

    # A mutation whose predicate matches no row takes the untouched-part clone path. The
    # predicate must not be prunable by the partition, otherwise the replicated queue drops
    # the mutation for that part and no clone happens at all.
    node1.query(
        "ALTER TABLE zc_inv_dst UPDATE y = y + 1 WHERE y = 12345",
        settings={"mutations_sync": 2},
    )
    assert (
        int(
            node1.query(
                "SELECT value FROM system.events WHERE event = 'MutationUntouchedParts'"
            ).strip()
            or 0
        )
        >= 1
    ), "the mutation did not take the untouched-part clone path"
    assert (
        node1.query(
            "SELECT count() = uniqExact(_block_number, _block_offset) FROM zc_inv_dst"
        ).strip()
        == "1"
    )

    dst_shared_id = _shared_id(node1, DST_ZK_PATH)
    assert dst_shared_id, "table_shared_id of zc_inv_dst not found in ZooKeeper"

    lists = _hardlink_lists(node1, dst_shared_id)
    # The clone did register hardlinks from its source part, so the check below is not vacuous.
    assert any(
        value for _, value in lists
    ), f"no hardlink list was recorded under {ZC_ROOT}/{dst_shared_id}: {lists}"
    for name, value in lists:
        assert INVALIDATED_FILE not in value, (
            f"{INVALIDATED_FILE} is recorded as hardlinked from part {name}, "
            f"but the clone rewrites its own copy: {value}"
        )

    node1.query("DROP TABLE zc_inv_dst SYNC")
    node1.query("DROP TABLE zc_inv_src SYNC")

    # And nothing is left behind in the object storage once both tables are dropped.
    wait_blobs_synchronization(cluster.minio_client, objects_before)

    node1.query("DROP TABLE warming_up SYNC")
