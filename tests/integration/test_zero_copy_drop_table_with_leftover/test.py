#!/usr/bin/env python3

"""
Test that DROP TABLE with zero-copy replication doesn't get stuck when leftover
part directories exist on disk but aren't tracked in data_parts_by_info.

Before the fix, MergeTreeData::dropAllData threw ZERO_COPY_REPLICATION_ERROR
in this scenario, causing DatabaseCatalog to retry the drop forever.
After the fix, it calls removeSharedRecursive with keep_all_shared_data=true
to safely clean up local metadata while preserving shared S3 objects.

Regression test for https://github.com/ClickHouse/ClickHouse/issues/82676
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.blobs import list_blobs, wait_blobs_count_synchronization

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_table_with_leftover_part_directory(started_cluster):
    """
    Create a ReplicatedMergeTree table on S3 with zero-copy replication,
    insert data, then create a fake leftover part directory on disk.
    DROP TABLE must succeed without getting stuck.
    """
    node1 = cluster.instances["node1"]

    objects_before = list_blobs(cluster.minio_client)

    node1.query(
        """
        CREATE TABLE test_leftover (key Int64)
        ENGINE = ReplicatedMergeTree('/test/tables/test_leftover', '1')
        ORDER BY key
        SETTINGS storage_policy = 's3',
                 allow_remote_fs_zero_copy_replication = 1
        """
    )

    node1.query("INSERT INTO test_leftover SELECT number FROM numbers(100)")
    node1.query("SYSTEM FLUSH LOGS")

    assert node1.query("SELECT count() FROM test_leftover") == "100\n"

    # Get the table's data path on disk
    data_path = node1.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables "
        "WHERE database = 'default' AND name = 'test_leftover'"
    ).strip()

    # Create a fake part directory that is NOT tracked in data_parts_by_info.
    # This simulates a broken part that couldn't be loaded, or a part left
    # behind by an interrupted fetch operation.
    node1.exec_in_container(
        ["bash", "-c", f"mkdir -p {data_path}/all_999_999_0"]
    )
    # Put some file in it so the directory is non-empty
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"echo 'fake' > {data_path}/all_999_999_0/columns.txt",
        ]
    )

    # DROP TABLE must complete successfully. Before the fix, this would throw
    # ZERO_COPY_REPLICATION_ERROR and DatabaseCatalog would retry forever.
    node1.query("DROP TABLE test_leftover SYNC", timeout=60)

    # Verify the table is gone
    assert (
        node1.query(
            "SELECT count() FROM system.tables "
            "WHERE database = 'default' AND name = 'test_leftover'"
        )
        == "0\n"
    )

    # Verify S3 objects are cleaned up eventually
    wait_blobs_count_synchronization(cluster.minio_client, len(objects_before))


def test_drop_table_with_broken_part(started_cluster):
    """
    Create a table with zero-copy replication, insert data into two parts,
    corrupt one part so it can't be loaded after restart, then drop the table.
    The broken part on disk triggers the new removeSharedRecursive code path.
    """
    node1 = cluster.instances["node1"]

    objects_before = list_blobs(cluster.minio_client)

    node1.query(
        """
        CREATE TABLE test_broken (key Int64)
        ENGINE = ReplicatedMergeTree('/test/tables/test_broken', '1')
        ORDER BY key
        SETTINGS storage_policy = 's3',
                 allow_remote_fs_zero_copy_replication = 1,
                 min_bytes_for_wide_part = 1
        """
    )

    node1.query("INSERT INTO test_broken SELECT number FROM numbers(100)")
    node1.query("INSERT INTO test_broken SELECT number FROM numbers(100, 100)")

    assert node1.query("SELECT count() FROM test_broken") == "200\n"

    parts = (
        node1.query(
            "SELECT name FROM system.parts "
            "WHERE table = 'test_broken' AND active ORDER BY name"
        )
        .strip()
        .split("\n")
    )
    assert len(parts) == 2

    data_path = node1.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables "
        "WHERE database = 'default' AND name = 'test_broken'"
    ).strip()

    # Corrupt the first part by removing columns.txt — it won't load on restart
    node1.exec_in_container(
        ["bash", "-c", f"rm {data_path}/{parts[0]}/columns.txt"]
    )

    # Restart the server so the table reloads and the broken part is not tracked
    node1.restart_clickhouse()

    # DROP TABLE must complete successfully
    node1.query("DROP TABLE test_broken SYNC", timeout=60)

    assert (
        node1.query(
            "SELECT count() FROM system.tables "
            "WHERE database = 'default' AND name = 'test_broken'"
        )
        == "0\n"
    )

    wait_blobs_count_synchronization(cluster.minio_client, len(objects_before) + 1)


def test_drop_database_with_leftover_part(started_cluster):
    """
    The original bug manifested as DROP DATABASE getting permanently stuck.
    Verify that dropping an entire database works when a table has leftover parts.
    """
    node1 = cluster.instances["node1"]

    objects_before = list_blobs(cluster.minio_client)

    node1.query("CREATE DATABASE test_drop_db")

    node1.query(
        """
        CREATE TABLE test_drop_db.test_tbl (key Int64)
        ENGINE = ReplicatedMergeTree('/test/tables/test_drop_db_tbl', '1')
        ORDER BY key
        SETTINGS storage_policy = 's3',
                 allow_remote_fs_zero_copy_replication = 1
        """
    )

    node1.query("INSERT INTO test_drop_db.test_tbl SELECT number FROM numbers(50)")
    assert node1.query("SELECT count() FROM test_drop_db.test_tbl") == "50\n"

    data_path = node1.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables "
        "WHERE database = 'test_drop_db' AND name = 'test_tbl'"
    ).strip()

    # Create a fake leftover part directory
    node1.exec_in_container(
        ["bash", "-c", f"mkdir -p {data_path}/all_888_888_0"]
    )
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"echo 'orphan' > {data_path}/all_888_888_0/data.bin",
        ]
    )

    # DROP DATABASE must complete — this was permanently stuck before the fix
    node1.query("DROP DATABASE test_drop_db SYNC", timeout=60)

    assert (
        node1.query(
            "SELECT count() FROM system.databases WHERE name = 'test_drop_db'"
        )
        == "0\n"
    )

    wait_blobs_count_synchronization(cluster.minio_client, len(objects_before))
