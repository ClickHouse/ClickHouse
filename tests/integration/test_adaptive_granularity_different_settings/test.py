import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)

# no adaptive granularity by default
node3 = cluster.add_instance('node3', image='yandex/clickhouse-server', tag='19.9.5.36', with_installed_binary=True,
                             stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_attach_detach(start_cluster):
    node1.query("""
        CREATE TABLE test (key UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/test', '1')
        ORDER BY tuple()
        SETTINGS index_granularity_bytes = 0""")

    node1.query("INSERT INTO test VALUES (1), (2)")

    node2.query("""
        CREATE TABLE test (key UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/test', '2')
        ORDER BY tuple() SETTINGS enable_mixed_granularity_parts = 0""")

    node2.query("INSERT INTO test VALUES (3), (4)")

    node1.query("SYSTEM SYNC REPLICA test", timeout=10)
    node2.query("SYSTEM SYNC REPLICA test", timeout=10)

    assert node1.query("SELECT COUNT() FROM  test") == "4\n"
    assert node2.query("SELECT COUNT() FROM  test") == "4\n"

    node1.query("DETACH TABLE test")
    node2.query("DETACH TABLE test")

    node1.query("ATTACH TABLE test")
    node2.query("ATTACH TABLE test")

    assert node1.query("SELECT COUNT() FROM  test") == "4\n"
    assert node2.query("SELECT COUNT() FROM  test") == "4\n"


def test_mutate_with_mixed_granularity(start_cluster):
    node3.query("""
        CREATE TABLE test (date Date, key UInt64, value1 String, value2 String)
        ENGINE = MergeTree
        ORDER BY key PARTITION BY date""")

    node3.query(
        "INSERT INTO test SELECT toDate('2019-10-01') + number % 5, number, toString(number), toString(number * number) FROM numbers(500)")

    assert node3.query("SELECT COUNT() FROM test") == "500\n"

    node3.restart_with_latest_version()

    assert node3.query("SELECT COUNT() FROM test") == "500\n"

    node3.query("ALTER TABLE test MODIFY SETTING enable_mixed_granularity_parts = 1")

    node3.query(
        "INSERT INTO test SELECT toDate('2019-10-01') + number % 5, number, toString(number), toString(number * number) FROM numbers(500, 500)")

    assert node3.query("SELECT COUNT() FROM test") == "1000\n"
    assert node3.query("SELECT COUNT() FROM test WHERE key % 100 == 0") == "10\n"

    node3.query("ALTER TABLE test DELETE WHERE key % 100 == 0", settings={"mutations_sync": "2"})

    assert node3.query("SELECT COUNT() FROM test WHERE key % 100 == 0") == "0\n"
