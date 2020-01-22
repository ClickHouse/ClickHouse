import logging

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


# Creates S3 bucket for tests and allows anonymous read-write access to it.
def prepare_s3_bucket(cluster):
    minio_client = cluster.minio_client

    if minio_client.bucket_exists(cluster.minio_bucket):
        minio_client.remove_bucket(cluster.minio_bucket)

    minio_client.make_bucket(cluster.minio_bucket)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", config_dir="configs", with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        yield cluster
    finally:
        cluster.shutdown()


def test_tinylog_s3(cluster):
    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("CREATE TABLE s3_test (id UInt64) Engine=TinyLog")
    node.query("INSERT INTO s3_test SELECT number FROM numbers(3)")
    assert node.query("SELECT * FROM s3_test") == "0\n1\n2\n"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 2
    node.query("INSERT INTO s3_test SELECT number + 3 FROM numbers(3)")
    assert node.query("SELECT * FROM s3_test") == "0\n1\n2\n3\n4\n5\n"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 2
    node.query("DROP TABLE s3_test")
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 0
