import logging
import time

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", main_configs=["configs/config.d/s3.xml"], macros={'replica': '1'},
                             with_minio=True,
                             with_zookeeper=True)
        cluster.add_instance("node2", main_configs=["configs/config.d/s3.xml"], macros={'replica': '2'},
                             with_minio=True,
                             with_zookeeper=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def get_large_objects_count(cluster, size=100):
    minio = cluster.minio_client
    counter = 0
    for obj in minio.list_objects(cluster.minio_bucket, 'data/'):
        if obj.size >= size:
            counter = counter + 1
    return counter


@pytest.mark.parametrize(
    "policy", ["s3"]
)
def test_s3_zero_copy_replication(cluster, policy):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
        CREATE TABLE s3_test ON CLUSTER test_cluster (id UInt32, value String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/s3_test', '{}')
        ORDER BY id
        SETTINGS storage_policy='{}'
        """
            .format('{replica}', policy)
    )

    node1.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")
    time.sleep(1)
    assert node1.query("SELECT * FROM s3_test order by id FORMAT Values") == "(0,'data'),(1,'data')"
    assert node2.query("SELECT * FROM s3_test order by id FORMAT Values") == "(0,'data'),(1,'data')"

    # Based on version 20.x - should be only one file with size 100+ (checksums.txt), used by both nodes
    assert get_large_objects_count(cluster) == 1

    node2.query("INSERT INTO s3_test VALUES (2,'data'),(3,'data')")
    time.sleep(1)
    assert node2.query("SELECT * FROM s3_test order by id FORMAT Values") == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"
    assert node1.query("SELECT * FROM s3_test order by id FORMAT Values") == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Based on version 20.x - two parts
    assert get_large_objects_count(cluster) == 2

    node1.query("OPTIMIZE TABLE s3_test")

    time.sleep(1)

    # Based on version 20.x - after merge, two old parts and one merged
    assert get_large_objects_count(cluster) == 3

    # Based on version 20.x - after cleanup - only one merged part
    countdown = 60
    while countdown > 0:
        if get_large_objects_count(cluster) == 1:
            break
        time.sleep(1)
        countdown -= 1
    assert get_large_objects_count(cluster) == 1

    node1.query("DROP TABLE IF EXISTS s3_test NO DELAY")
    node2.query("DROP TABLE IF EXISTS s3_test NO DELAY")

