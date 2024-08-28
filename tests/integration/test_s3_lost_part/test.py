# Tag no-fasttest: requires S3

import logging
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    with_minio=True,
    macros={"replica": "replica1"},
)

replica2 = cluster.add_instance(
    "replica2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    with_minio=True,
    macros={"replica": "replica2"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def create_replicated_table(node, table_name):
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/shard1/{table_name}', '{{replica}}')"
    )

    node.query_with_retry(
        f"""
        CREATE TABLE {table_name}
        (
            id Int64
        ) 
        ENGINE={engine}
        ORDER BY id
        SETTINGS
            storage_policy='s3',
            allow_remote_fs_zero_copy_replication=1
        """
    )


def test_s3_lost_part(start_cluster):
    create_replicated_table(replica1, "no_key_found_disk_repl")
    create_replicated_table(replica2, "no_key_found_disk_repl")

    replica1.query("INSERT INTO no_key_found_disk_repl VALUES (1)")
    data = replica1.query("SELECT * FROM no_key_found_disk_repl").strip()
    assert data == "1"

    uuid = replica1.query(
        """
        SELECT uuid
        FROM system.tables
        WHERE name = 'no_key_found_disk_repl'
        """
    ).strip()
    assert uuid

    remote_pathes = (
        replica1.query(
            f"""
        SELECT remote_path
        FROM system.remote_data_paths
        WHERE
            local_path LIKE '%{uuid}%'
            AND local_path LIKE '%.bin%'
        ORDER BY ALL
        """
        )
        .strip()
        .split()
    )
    assert len(remote_pathes) > 0

    for path in remote_pathes:
        assert cluster.minio_client.stat_object(cluster.minio_bucket, path).size > 0
        cluster.minio_client.remove_object(cluster.minio_bucket, path)

    error = replica2.query_and_get_error("SELECT * FROM no_key_found_disk_repl").strip()
    assert (
        "DB::Exception: The specified key does not exist. This error happened for S3 disk."
        in error
    )

    data = replica2.query("SELECT * FROM no_key_found_disk_repl").strip()
    assert data == ""

