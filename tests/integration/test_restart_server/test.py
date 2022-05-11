import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', stay_alive=True)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_memory_database(start_cluster):
    node.query("CREATE DATABASE test ENGINE Memory")
    node.query("CREATE TABLE test.test_table(a String) ENGINE Memory")
    node.query("DROP DATABASE test")
    node.restart_clickhouse(kill=True)
    assert node.query("SHOW DATABASES LIKE 'test'").strip() == ""

