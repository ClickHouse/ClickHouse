import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
S3_DATA = [
    "data/clickhouse/part1.csv",
]


def create_buckets_s3(cluster):
    minio = cluster.minio_client

    for file in S3_DATA:
        minio.fput_object(
            bucket_name=cluster.minio_bucket,
            object_name=file,
            file_path=os.path.join(SCRIPT_DIR, file),
        )
    for obj in minio.list_objects(cluster.minio_bucket, recursive=True):
        print(obj.object_name)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # Until 24.10, query level settings were specified in the .sql file
        cluster.add_instance(
            "old_node",
            image="clickhouse/clickhouse-server",
            tag="24.9.2.42",
            with_zookeeper=True,
            with_installed_binary=True,
            with_minio=True,
            stay_alive=True,
        )

        cluster.add_instance(
            "new_node",
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def test_query_settings_in_create_recover_lost_replica(started_cluster):
    old_node = started_cluster.instances["old_node"]
    old_node.query("DROP DATABASE IF EXISTS replicated_lost_replica SYNC")
    old_node.query(
        "CREATE DATABASE replicated_lost_replica ENGINE = Replicated('/test/replicated_lost_replica', 'shard1', 'replica' || '1');"
    )
    old_node.query("DROP TABLE IF EXISTS replicated_lost_replica.b")
    old_node.query(
        f"""CREATE TABLE replicated_lost_replica.b Engine = S3('http://minio1:9001/root/data/clickhouse/part1.csv', 'minio', '{minio_secret_key}') SETTINGS s3_create_new_file_on_insert = 1;"""
    )

    new_node = started_cluster.instances["new_node"]
    new_node.query("DROP DATABASE IF EXISTS replicated_lost_replica SYNC")
    # Adding new replica will trigger the `recoverLostReplica` method.
    new_node.query(
        "CREATE DATABASE replicated_lost_replica ENGINE = Replicated('/test/replicated_lost_replica', 'shard1', 'replica' || '2');"
    )
    new_node.query("SYSTEM SYNC DATABASE REPLICA replicated_lost_replica")
