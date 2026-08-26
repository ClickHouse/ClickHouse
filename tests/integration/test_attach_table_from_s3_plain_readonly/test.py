import logging
import os
import re
from pathlib import Path

import pytest
from minio.error import S3Error

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def upload_to_minio(minio_client, bucket_name, local_path, minio_path=""):
    local_path = Path(local_path)
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = Path(root) / file
            minio_object_name = minio_path + str(
                local_file_path.relative_to(local_path)
            )

            try:
                with open(local_file_path, "rb") as data:
                    file_stat = os.stat(local_file_path)
                    minio_client.put_object(
                        bucket_name, minio_object_name, data, file_stat.st_size
                    )
                logging.info(f"Uploaded {local_file_path} to {minio_object_name}")
            except S3Error as e:
                logging.error(f"Error uploading {local_file_path}: {e}")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_attach_table_from_s3_plain_readonly(started_cluster):
    # Create an atomic DB with mergetree sample data
    node1.query(
        """
    create database local_db;

    create table local_db.test_table (num UInt32) engine=MergeTree() order by num;

    insert into local_db.test_table (*) Values (5)
    """
    )

    assert int(node1.query("select num from local_db.test_table limit 1")) == 5

    # Copy local MergeTree data into minio bucket
    table_data_path = os.path.join(node1.path, f"database/store")
    minio = cluster.minio_client
    upload_to_minio(
        minio, cluster.minio_bucket, table_data_path, "data/disks/disk_s3_plain/store/"
    )

    # Drop the non-replicated table, we don't need it anymore
    table_uuid = node1.query(
        "SELECT uuid FROM system.tables WHERE database='local_db' AND table='test_table'"
    ).strip()
    node1.query("drop table local_db.test_table SYNC;")

    # Create a replicated database
    node1.query(
        "create database s3_plain_test_db ENGINE = Replicated('/test/s3_plain_test_db', 'shard1', 'replica1');"
    )
    node2.query(
        "create database s3_plain_test_db ENGINE = Replicated('/test/s3_plain_test_db', 'shard1', 'replica2');"
    )

    # Create a MergeTree table at one node, by attaching the merge tree data
    node1.query(
        f"""
    attach table s3_plain_test_db.test_table UUID '{table_uuid}' (num UInt32)
    engine=MergeTree()
    order by num
    settings storage_policy = 's3_plain_readonly'
    """
    )

    # Check that both nodes can query and get result.
    assert int(node1.query("select num from s3_plain_test_db.test_table limit 1")) == 5
    assert int(node2.query("select num from s3_plain_test_db.test_table limit 1")) == 5
