import logging

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node",
                             main_configs=["configs/minio.xml", "configs/ssl.xml", "configs/config.d/log_conf.xml"],
                             with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "log_engine,files_overhead,files_overhead_per_insert",
    [("TinyLog", 1, 1), ("Log", 2, 1), ("StripeLog", 1, 2)])
def test_log_family_s3(cluster, log_engine, files_overhead, files_overhead_per_insert):
    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("CREATE TABLE s3_test (id UInt64) Engine={}".format(log_engine))

    node.query("INSERT INTO s3_test SELECT number FROM numbers(5)")
    assert node.query("SELECT * FROM s3_test") == "0\n1\n2\n3\n4\n"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == files_overhead_per_insert + files_overhead

    node.query("INSERT INTO s3_test SELECT number + 5 FROM numbers(3)")
    assert node.query("SELECT * FROM s3_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n"
    assert len(
        list(minio.list_objects(cluster.minio_bucket, 'data/'))) == files_overhead_per_insert * 2 + files_overhead

    node.query("INSERT INTO s3_test SELECT number + 8 FROM numbers(1)")
    assert node.query("SELECT * FROM s3_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
    assert len(
        list(minio.list_objects(cluster.minio_bucket, 'data/'))) == files_overhead_per_insert * 3 + files_overhead

    node.query("TRUNCATE TABLE s3_test")
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 0

    node.query("DROP TABLE s3_test")
