import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True, image='yandex/clickhouse-server', tag='21.2', with_installed_binary=True, stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_modulo_partition_key_after_update(start_cluster):
    node1.query("CREATE TABLE test (id Int64, v UInt64, value String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/table1', '1', v) PARTITION BY id % 20 ORDER BY (id, v)")
    node1.query("INSERT INTO test SELECT number, number, toString(number) FROM numbers(10)")
    expected = node1.query("SELECT number, number, toString(number) FROM numbers(10)")
    partition_data = node1.query("SELECT partition, name FROM system.parts WHERE table='test' ORDER BY partition")
    assert(expected == node1.query("SELECT * FROM test ORDER BY id"))
    node1.restart_with_latest_version(signal=9)
    assert(expected == node1.query("SELECT * FROM test ORDER BY id"))
    assert(partition_data == node1.query("SELECT partition, name FROM system.parts WHERE table='test' ORDER BY partition"))
