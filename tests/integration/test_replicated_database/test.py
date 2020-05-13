import time
import logging

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', macros={'replica': 'test1'}, with_zookeeper=True)
node2 = cluster.add_instance('node2', macros={'replica': 'test2'}, with_zookeeper=True)

all_nodes = [node1, node2]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in all_nodes:
            node.query("DROP DATABASE IF EXISTS testdb")
            node.query("CREATE DATABASE testdb ENGINE = Replicated('/clickhouse/databases/test1', '{replica}');")
        yield cluster

    finally:
        cluster.shutdown()


def test_db(started_cluster):
    DURATION_SECONDS = 5
    node1.query("CREATE TABLE testdb.replicated_table  (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree(d, k, 8192);")

    time.sleep(DURATION_SECONDS)
    logging.info(node2.query("desc table testdb.replicated_table"))
    assert node1.query("desc table testdb.replicated_table") == node2.query("desc table testdb.replicated_table")
