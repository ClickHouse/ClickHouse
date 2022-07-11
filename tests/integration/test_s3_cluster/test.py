import logging
import os

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


def test_distributed_insert_select(started_cluster):
    first_replica_first_shard = started_cluster.instances["s0_0_0"]
    second_replica_first_shard = started_cluster.instances["s0_0_1"]
    first_replica_second_shard = started_cluster.instances["s0_1_0"]

    first_replica_first_shard.query(
        """
    CREATE TABLE insert_select_local ON CLUSTER 'cluster_simple' (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/insert_select', '{replica}')
    ORDER BY (a, b);
        """
    )

    first_replica_first_shard.query(
        """
    CREATE TABLE insert_select_distributed ON CLUSTER 'cluster_simple' as insert_select_local
    ENGINE = Distributed('cluster_simple', default, insert_select_local, b % 2);
        """
    )

    for file_number in range(100):
        first_replica_first_shard.query(
            """
        INSERT INTO TABLE FUNCTION s3('http://minio1:9001/root/data/generated/file_{}.csv', 'minio', 'minio123', 'CSV','a String, b UInt64')
        SELECT repeat('{}', 10), number from numbers(100);
            """.format(file_number, file_number)
        )

    first_replica_first_shard.query(
        """
    INSERT INTO insert_select_distributed SELECT * FROM s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', 'minio123', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=1;
        """
    )

    for line in first_replica_first_shard.query(
        """SELECT * FROM insert_select_local;""").strip().split('\n'):
        _, b = line.split()
        assert int(b) % 2 == 0

    for line in second_replica_first_shard.query(
        """SELECT * FROM insert_select_local;""").strip().split('\n'):
        _, b = line.split()
        assert int(b) % 2 == 0

    for line in first_replica_second_shard.query(
        """SELECT * FROM insert_select_local;""").strip().split('\n'):
        _, b = line.split()
        assert int(b) % 2 == 1


def test_distributed_insert_select_with_replicated(started_cluster):
    first_replica_first_shard = started_cluster.instances["s0_0_0"]
    second_replica_first_shard = started_cluster.instances["s0_0_1"]

    first_replica_first_shard.query(
        """
    CREATE TABLE insert_select_replicated_local ON CLUSTER 'first_shard' (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/insert_select', '{replica}')
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

    for file_number in range(100):
        first_replica_first_shard.query(
            """
        INSERT INTO TABLE FUNCTION s3('http://minio1:9001/root/data/generated_replicated/file_{}.csv', 'minio', 'minio123', 'CSV','a String, b UInt64')
        SELECT repeat('{}', 10), number from numbers(100);
            """.format(file_number, file_number)
        )


    first_replica_first_shard.query(
        """
    INSERT INTO insert_select_replicated_local SELECT * FROM s3Cluster(
        'first_shard',
        'http://minio1:9001/root/data/generated_replicated/*.csv', 'minio', 'minio123', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=1;
        """
    )

    first = int(first_replica_first_shard.query("""SELECT count(*) FROM insert_select_replicated_local""").strip())
    second = int(second_replica_first_shard.query("""SELECT count(*) FROM insert_select_replicated_local""").strip())

    assert first != 0
    assert second != 0
    assert first + second == 100 * 100
