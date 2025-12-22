import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_s3_non_deterministic_partition_by(started_cluster):
    """
    Test that the S3 table engine doesn't cache the results of non-deterministic functions (e.g. now()) when calculating the partition key.
    """

    node.query(
        f"""
        CREATE TABLE t
        (
            s String
        )
        ENGINE = S3('http://minio1:9001/{started_cluster.minio_bucket}/{{_partition_id}}.parquet', 'minio', 'ClickHouse_Minio_P@ssw0rd')
        PARTITION BY toString(now64(9))
        """
    )

    node.query("INSERT INTO t VALUES ('first')")
    time.sleep(0.1)
    node.query("INSERT INTO t VALUES ('second')")

    parquet_files = [
        f.object_name
        for f in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, recursive=True)
        if f.object_name.endswith('.parquet')
    ]
    assert len(parquet_files) == 2
    assert parquet_files[0] != parquet_files[1]

    node.query("DROP TABLE t")
