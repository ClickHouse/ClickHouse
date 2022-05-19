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
            "s0_0_0", main_configs=["configs/cluster.xml"], with_minio=True
        )
        cluster.add_instance("s0_0_1", main_configs=["configs/cluster.xml"])
        cluster.add_instance("s0_1_0", main_configs=["configs/cluster.xml"])

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
