from email.errors import HeaderParseError
import logging
import os
import csv
import shutil
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
S3_DATA = [
    "data/clickhouse/part1.csv",
    "data/clickhouse/part123.csv",
    "data/database/part2.csv",
    "data/database/partition675.csv",
]


def create_buckets_s3(cluster):
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
                data.append([str(number + file_number) * 10, number + file_number])

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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s0_0_0",
            main_configs=["configs/cluster.xml"],
            macros={"replica": "node1", "shard": "shard1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_0_1",
            main_configs=["configs/cluster.xml"],
            macros={"replica": "replica2", "shard": "shard1"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_1_0",
            main_configs=["configs/cluster.xml"],
            macros={"replica": "replica1", "shard": "shard2"},
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)

        yield cluster
    finally:
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated/"))
        cluster.shutdown()


def test_select_all(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    pure_s3 = node.query(
        """
    SELECT * from s3(
        'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    ORDER BY (name, value, polygon)"""
    )
    # print(pure_s3)
    s3_distibuted = node.query(
        """
    SELECT * from s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') ORDER BY (name, value, polygon)"""
    )
    # print(s3_distibuted)

    assert TSV(pure_s3) == TSV(s3_distibuted)


def test_count(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    pure_s3 = node.query(
        """
    SELECT count(*) from s3(
        'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(pure_s3)
    s3_distibuted = node.query(
        """
    SELECT count(*) from s3Cluster(
        'cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(s3_distibuted)

    assert TSV(pure_s3) == TSV(s3_distibuted)


def test_count_macro(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    s3_macro = node.query(
        """
    SELECT count(*) from s3Cluster(
        '{default_cluster_macro}', 'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(s3_distibuted)
    s3_distibuted = node.query(
        """
    SELECT count(*) from s3Cluster(
        'cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(s3_distibuted)

    assert TSV(s3_macro) == TSV(s3_distibuted)


def test_union_all(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    pure_s3 = node.query(
        """
    SELECT * FROM
    (
        SELECT * from s3(
            'http://minio1:9001/root/data/{clickhouse,database}/*',
            'minio', 'minio123', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
        UNION ALL
        SELECT * from s3(
            'http://minio1:9001/root/data/{clickhouse,database}/*',
            'minio', 'minio123', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    )
    ORDER BY (name, value, polygon)
    """
    )
    # print(pure_s3)
    s3_distibuted = node.query(
        """
    SELECT * FROM
    (
        SELECT * from s3Cluster(
            'cluster_simple',
            'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
        UNION ALL
        SELECT * from s3Cluster(
            'cluster_simple',
            'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    )
    ORDER BY (name, value, polygon)
    """
    )
    # print(s3_distibuted)

    assert TSV(pure_s3) == TSV(s3_distibuted)


def test_wrong_cluster(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    error = node.query_and_get_error(
        """
    SELECT count(*) from s3Cluster(
        'non_existent_cluster',
        'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    UNION ALL
    SELECT count(*) from s3Cluster(
        'non_existent_cluster',
        'http://minio1:9001/root/data/{clickhouse,database}/*',
        'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    """
    )

    assert "not found" in error


def test_ambiguous_join(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    result = node.query(
        """
    SELECT l.name, r.value from s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') as l
    JOIN s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') as r
    ON l.name = r.name
    """
    )
    assert "AMBIGUOUS_COLUMN_NAME" not in result


def test_skip_unavailable_shards(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    result = node.query(
        """
    SELECT count(*) from s3Cluster(
        'cluster_non_existent_port',
        'http://minio1:9001/root/data/clickhouse/part1.csv',
        'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    SETTINGS skip_unavailable_shards = 1
    """
    )

    assert result == "10\n"


def test_unskip_unavailable_shards(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    error = node.query_and_get_error(
        """
    SELECT count(*) from s3Cluster(
        'cluster_non_existent_port',
        'http://minio1:9001/root/data/clickhouse/part1.csv',
        'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    """
    )

    assert "NETWORK_ERROR" in error


def test_distributed_insert_select_with_replicated(started_cluster):
    first_replica_first_shard = started_cluster.instances["s0_0_0"]
    second_replica_first_shard = started_cluster.instances["s0_0_1"]

    first_replica_first_shard.query(
        """DROP TABLE IF EXISTS insert_select_replicated_local ON CLUSTER 'first_shard' SYNC;"""
    )

    first_replica_first_shard.query(
        """
    CREATE TABLE insert_select_replicated_local ON CLUSTER 'first_shard' (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/insert_select_with_replicated', '{replica}')
    ORDER BY (a, b);
        """
    )

    for replica in [first_replica_first_shard, second_replica_first_shard]:
        replica.query(
            """
            SYSTEM STOP FETCHES;
            """
        )
        replica.query(
            """
            SYSTEM STOP MERGES;
            """
        )

    first_replica_first_shard.query(
        """
    INSERT INTO insert_select_replicated_local SELECT * FROM s3Cluster(
        'first_shard',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', 'minio123', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=1;
        """
    )

    for replica in [first_replica_first_shard, second_replica_first_shard]:
        replica.query(
            """
            SYSTEM FLUSH LOGS;
            """
        )

    assert (
        int(
            second_replica_first_shard.query(
                """SELECT count(*) FROM system.query_log WHERE not is_initial_query and query ilike '%s3Cluster%';"""
            ).strip()
        )
        != 0
    )

    # Check whether we inserted at least something
    assert (
        int(
            second_replica_first_shard.query(
                """SELECT count(*) FROM insert_select_replicated_local;"""
            ).strip()
        )
        != 0
    )

    first_replica_first_shard.query(
        """DROP TABLE IF EXISTS insert_select_replicated_local ON CLUSTER 'first_shard' SYNC;"""
    )


def test_parallel_distributed_insert_select_with_schema_inference(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    node.query(
        """DROP TABLE IF EXISTS parallel_insert_select ON CLUSTER 'first_shard' SYNC;"""
    )

    node.query(
        """
    CREATE TABLE parallel_insert_select ON CLUSTER 'first_shard' (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/insert_select_with_replicated', '{replica}')
    ORDER BY (a, b);
        """
    )

    node.query(
        """
    INSERT INTO parallel_insert_select SELECT * FROM s3Cluster(
        'first_shard',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', 'minio123', 'CSV'
    ) SETTINGS parallel_distributed_insert_select=1, use_structure_from_insertion_table_in_table_functions=0;
        """
    )

    node.query("SYSTEM SYNC REPLICA parallel_insert_select")

    actual_count = int(
        node.query(
            "SELECT count() FROM s3('http://minio1:9001/root/data/generated/*.csv', 'minio', 'minio123', 'CSV','a String, b UInt64')"
        )
    )

    count = int(node.query("SELECT count() FROM parallel_insert_select"))
    assert count == actual_count
