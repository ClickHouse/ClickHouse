import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query("CREATE TABLE local_table(id UInt32, data JSON) ENGINE = MergeTree ORDER BY id")
            node.query("CREATE TABLE dist_table AS local_table ENGINE = Distributed(test_cluster, default, local_table)")

        yield cluster

    finally:
        cluster.shutdown()


def test_distributed_type_object(started_cluster):
    node1.query('INSERT INTO local_table FORMAT JSONEachRow {"id": 1, "data": {"k1": 10}}')
    node2.query('INSERT INTO local_table FORMAT JSONEachRow {"id": 2, "data": {"k1": 20}}')

    expected = TSV("10\n20\n")
    assert TSV(node1.query("SELECT data.k1 FROM dist_table ORDER BY id")) == expected

    node1.query('INSERT INTO local_table FORMAT JSONEachRow {"id": 3, "data": {"k1": "str1"}}')

    expected = TSV("10\n20\nstr1\n")
    assert TSV(node1.query("SELECT data.k1 FROM dist_table ORDER BY id")) == expected
