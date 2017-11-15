import pytest

from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        instance = cluster.add_instance('dummy', clickhouse_path_dir='clickhouse_path')
        cluster.start()

        cluster_fail = ClickHouseCluster(__file__, name='fail')
        instance_fail = cluster_fail.add_instance('dummy_fail', clickhouse_path_dir='clickhouse_path_fail')
        with pytest.raises(Exception):
            cluster_fail.start()
        cluster_fail.shutdown() # cleanup

        yield cluster

    finally:
        cluster.shutdown()


def test_sophisticated_default(started_cluster):
    instance = started_cluster.instances['dummy']
    instance.query("INSERT INTO sophisticated_default (c) VALUES (0)")
    assert instance.query("SELECT a, b, c FROM sophisticated_default") == "3\t9\t0\n"

