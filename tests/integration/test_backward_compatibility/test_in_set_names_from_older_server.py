import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=False, use_distributed_plan=False)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for node in (node1, node2):
            node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")
            node.query("INSERT INTO t VALUES (1), (2)")
            node.query("CREATE TABLE t2 (x UInt64) ENGINE = MergeTree ORDER BY x")
            node.query("INSERT INTO t2 VALUES (3)")
        yield cluster

    finally:
        cluster.shutdown()


# The older server names the `IN` sets after its own query tree, so the names differ from the initiator's.
def test_in_set_names_from_older_server(start_cluster):
    assert node1.query("SELECT countIf(number IN (SELECT x FROM t)) FROM remote('node1,node2', numbers(4))") == "4\n"
    assert node1.query("SELECT number IN (SELECT x FROM t) AS k, count() FROM remote('node1,node2', numbers(4)) GROUP BY k ORDER BY k") == "0\t4\n1\t4\n"
    assert node1.query("SELECT countIf(number IN (SELECT x FROM t)), countIf(number IN (SELECT x FROM t2)) FROM remote('node1,node2', numbers(4))") == "4\t2\n"
