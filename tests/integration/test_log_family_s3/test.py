import logging
import sys

import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node",
                             main_configs=["configs/minio.xml", "configs/ssl.xml"],
                             with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def assert_objects_count(cluster, objects_count, path='data/'):
    minio = cluster.minio_client
    s3_objects = list(minio.list_objects(cluster.minio_bucket, path))
    if objects_count != len(s3_objects):
        for s3_object in s3_objects:
            object_meta = minio.stat_object(cluster.minio_bucket, s3_object.object_name)
            logging.info("Existing S3 object: %s", str(object_meta))
        assert objects_count == len(s3_objects)


@pytest.mark.parametrize(
    "log_engine,files_overhead,files_overhead_per_insert",
    [("TinyLog", 1, 1), ("Log", 2, 1), ("StripeLog", 1, 2)])
def test_log_family_s3(cluster, log_engine, files_overhead, files_overhead_per_insert):
    node = cluster.instances["node"]

    node.query("CREATE TABLE s3_test (id UInt64) ENGINE={} SETTINGS disk = 's3'".format(log_engine))

    node.query("INSERT INTO s3_test SELECT number FROM numbers(5)")
    assert node.query("SELECT * FROM s3_test") == "0\n1\n2\n3\n4\n"
    assert_objects_count(cluster, files_overhead_per_insert + files_overhead)

    node.query("INSERT INTO s3_test SELECT number + 5 FROM numbers(3)")
    assert node.query("SELECT * FROM s3_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n"
    assert_objects_count(cluster, files_overhead_per_insert * 2 + files_overhead)

    node.query("INSERT INTO s3_test SELECT number + 8 FROM numbers(1)")
    assert node.query("SELECT * FROM s3_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
    assert_objects_count(cluster, files_overhead_per_insert * 3 + files_overhead)

    node.query("TRUNCATE TABLE s3_test")
    assert_objects_count(cluster, 0)

    node.query("DROP TABLE s3_test")
