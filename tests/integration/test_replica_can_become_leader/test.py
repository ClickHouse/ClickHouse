import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/notleader.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/notleaderignorecase.xml"], with_zookeeper=True
)
node3 = cluster.add_instance("node3", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for i, node in enumerate((node1, node2)):
            node.query(
                """
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', '{}')
                PARTITION BY date ORDER BY id
                """.format(
                    i
                )
            )

        with pytest.raises(QueryRuntimeException):
            node3.query(
                """
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', '{}')
                PARTITION BY date ORDER BY id SETTINGS replicated_can_become_leader=0sad
                """.format(
                    3
                )
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_can_become_leader(start_cluster):
    assert (
        node1.query(
            "select can_become_leader from system.replicas where table = 'test_table'"
        )
        == "0\n"
    )
    assert (
        node2.query(
            "select can_become_leader from system.replicas where table = 'test_table'"
        )
        == "0\n"
    )
