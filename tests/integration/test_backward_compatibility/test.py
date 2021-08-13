import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True, image='yandex/clickhouse-server', tag='19.17.8.54', stay_alive=True, with_installed_binary=True)
node2 = cluster.add_instance('node2', main_configs=['configs/wide_parts_only.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for i, node in enumerate([node1, node2]):
            node.query(
                '''CREATE TABLE t(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/t', '{}')
                PARTITION BY toYYYYMM(date)
                ORDER BY id'''.format(i))

        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability1(start_cluster):
    node2.query("INSERT INTO t VALUES (today(), 1)")
    node1.query("SYSTEM SYNC REPLICA t", timeout=10)

    assert node1.query("SELECT count() FROM t") == "1\n"
