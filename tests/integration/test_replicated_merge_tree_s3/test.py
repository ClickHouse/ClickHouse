import logging
import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance("node1", config_dir="configs", macros={'cluster': 'test1'}, with_minio=True, with_zookeeper=True)
        cluster.add_instance("node2", config_dir="configs", macros={'cluster': 'test1'}, with_zookeeper=True)
        cluster.add_instance("node3", config_dir="configs", macros={'cluster': 'test1'}, with_zookeeper=True)

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_PER_PART = FILES_OVERHEAD_PER_COLUMN * 3 + 2 + 6


def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign*(i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster):
    create_table_statement = """
        CREATE TABLE s3_test (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=ReplicatedMergeTree('/clickhouse/{cluster}/tables/test/s3', '{instance}')
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS storage_policy='s3'
        """

    for node in cluster.instances.values():
        node.query(create_table_statement)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in cluster.instances.values():
        node.query("DROP TABLE IF EXISTS s3_test")

    minio = cluster.minio_client
    # Remove extra objects to prevent tests cascade failing
    for obj in list(minio.list_objects(cluster.minio_bucket, 'data/')):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


def test_insert_select_replicated(cluster):
    create_table(cluster)

    all_values = ""
    for node_idx in range(1, 4):
        node = cluster.instances["node" + str(node_idx)]
        values = generate_values("2020-01-0" + str(node_idx), 4096)
        node.query("INSERT INTO s3_test VALUES {}".format(values), settings={"insert_quorum": 3})
        if node_idx != 1:
            all_values += ","
        all_values += values

    for node_idx in range(1, 4):
        node = cluster.instances["node" + str(node_idx)]
        assert node.query("SELECT * FROM s3_test order by dt, id FORMAT Values", settings={"select_sequential_consistency": 1}) == all_values

    minio = cluster.minio_client
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 3 * (FILES_OVERHEAD + FILES_OVERHEAD_PER_PART * 3)
