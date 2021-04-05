import os
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.hdfs_api import HDFSApi

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=[
    'configs/storage.xml',
    'configs/log_conf.xml'], with_hdfs=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_read_write(started_cluster):
    node1.query("DROP TABLE IF EXISTS simple_test")
    node1.query("CREATE TABLE simple_test (id UInt64) Engine=TinyLog SETTINGS disk = 'hdfs'")
    node1.query("INSERT INTO simple_test SELECT number FROM numbers(3)")
    node1.query("INSERT INTO simple_test SELECT number FROM numbers(3, 3)")
    assert node1.query("SELECT * FROM simple_test") == "0\n1\n2\n3\n4\n5\n"

