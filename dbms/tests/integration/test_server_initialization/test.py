import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('dummy', clickhouse_path_dir='clickhouse_path')

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_sophisticated_default(started_cluster):
    instance.query("INSERT INTO sophisticated_default (c) VALUES (0)")
    assert instance.query("SELECT a, b, c FROM sophisticated_default") == "3\t9\t0\n"

