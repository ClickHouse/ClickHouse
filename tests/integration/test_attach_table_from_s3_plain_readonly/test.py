import re
import os
import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from minio.error import S3Error
from pathlib import Path

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

def upload_to_minio(minio_client, bucket_name, local_path, minio_path=''):
    local_path = Path(local_path)
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = Path(root) / file
            minio_object_name = minio_path + str(local_file_path.relative_to(local_path))

            try:
                with open(local_file_path, 'rb') as data:
                    file_stat = os.stat(local_file_path)
                    minio_client.put_object(bucket_name, minio_object_name, data, file_stat.st_size)
                logging.info(f'Uploaded {local_file_path} to {minio_object_name}')
            except S3Error as e:
                logging.info(f'Error uploading {local_file_path}: {e}')


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_attach_table_from_s3_plain_readonly(started_cluster):
    node.query(
    """
    create database local_db;

    create table local_db.test_table (num UInt32) engine=MergeTree() order by num;

    insert into local_db.test_table (*) Values (5)
    """
    )

    assert int(node.query("select num from local_db.test_table limit 1")) == 5
    
    # Copy local file into minio bucket
    table_data_path = os.path.join(node.path, f"database/store")
    minio = cluster.minio_client
    upload_to_minio(minio, cluster.minio_bucket, table_data_path, "data/disks/disk_s3_plain/")

    ### remove
    s3_objects = list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    for s3_object in s3_objects:
        logging.info("bianpengyuan Existing S3 object: %s", s3_object.object_name)
    ### remove

    # Create a replicated database, and attach the merge tree data disk
    table_uuid = node.query("SELECT uuid FROM system.tables WHERE database='local_db' AND table='test_table'").strip()
    node.query(
    f"""
    drop table local_db.test_table SYNC;

    create database s3_plain_test_db ENGINE = Replicated('/test/s3_plain_test_db', 'shard1', 'replica1');
    attach table s3_plain_test_db.test_table UUID '{table_uuid}' (num UInt32)
    engine=MergeTree()
    order by num
    settings
        disk=disk(type=s3_plain,
            endpoint='http://minio1:9001/root/data/disks/disk_s3_plain/',
            access_key_id='minio',
            secret_access_key='minio123');
    """
    )

    assert int(node.query("select num from s3_plain_test_db.test_table limit 1")) == 5
