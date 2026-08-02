from ast import literal_eval

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)
node3 = cluster.add_instance("node3", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for i, node in enumerate((node1, node2, node3)):
            node_name = "node" + str(i + 1)
            node.query(
                """
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', '{}')
                PARTITION BY date ORDER BY id
                """.format(
                    node_name
                )
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_replica_is_active(start_cluster):
    query_result = node1.query(
        "select replica_is_active from system.replicas where table = 'test_table'"
    )
    assert literal_eval(query_result) == {"node1": 1, "node2": 1, "node3": 1}

    node3.stop()
    query_result = node1.query(
        "select replica_is_active from system.replicas where table = 'test_table'"
    )
    assert literal_eval(query_result) == {"node1": 1, "node2": 1, "node3": 0}

    node2.stop()
    query_result = node1.query(
        "select replica_is_active from system.replicas where table = 'test_table'"
    )
    assert literal_eval(query_result) == {"node1": 1, "node2": 0, "node3": 0}
