import csv
import logging
import os
import shutil
from uuid import uuid4

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
S3_DATA = []

generated_rows = 0

def create_buckets_s3(cluster):
    global generated_rows

    minio = cluster.minio_client

    for file_number in range(100):
        file_name = f"data/generated/file_{file_number}.csv"
        os.makedirs(os.path.join(SCRIPT_DIR, "data/generated/"), exist_ok=True)
        S3_DATA.append(file_name)
        with open(os.path.join(SCRIPT_DIR, file_name), "w+", encoding="utf-8") as f:
            # a String, b UInt64
            data = []

            # Make all files a bit different
            for number in range(100 + file_number):
                data.append(
                    ["str_" + str(number + file_number) * 10, number + file_number]
                )
                generated_rows += 1

            writer = csv.writer(f)
            writer.writerows(data)

    for file in S3_DATA:
        minio.fput_object(
            bucket_name=cluster.minio_bucket,
            object_name=file,
            file_path=os.path.join(SCRIPT_DIR, file),
        )
    for obj in minio.list_objects(cluster.minio_bucket, recursive=True):
        print(obj.object_name)


def run_s3_mocks(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            ("s3_mock.py", "resolver", "8080"),
        ],
    )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/cluster.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
    with_zookeeper=True,
    macros={"replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/cluster.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    macros={"replica": "node2"},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/cluster.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    macros={"replica": "node3"},
)

@pytest.fixture(scope="module")
def started_cluster():
    try:

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)

        run_s3_mocks(cluster)

        yield cluster
    finally:
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated/"))
        cluster.shutdown()


def test_distributed_insert_select_to_rmt(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"
    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/b0612a53-5f58-4df0-a28f-a154e0cdb797/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} SELECT * FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=2;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == generated_rows
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )


def test_distributed_insert_select_to_rmt_limit(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"
    limit = generated_rows - 1000

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/32c614a9-13af-43c5-848c-a3f62a78e390/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} SELECT * FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) LIMIT {limit} SETTINGS parallel_distributed_insert_select=2;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == limit
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )


def test_distributed_insert_select_to_rmt_cte_const(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/178ed413-b055-46eb-9657-442811aef552/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} WITH 1 + 1 as two SELECT a, b + (select sum(number) from numbers(10) where number < 2) FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=2;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == generated_rows
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )
