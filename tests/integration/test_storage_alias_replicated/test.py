import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/config_alias.xml"],
    with_zookeeper=True,
    macros={"replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/config_alias.xml"],
    with_zookeeper=True,
    macros={"replica": "node2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_alias_with_replicated(started_cluster):
    node1.query("CREATE DATABASE test_rmt ENGINE = Replicated('/clickhouse/databases/test_rmt', 'shard1', 'node1')")
    node2.query("CREATE DATABASE test_rmt ENGINE = Replicated('/clickhouse/databases/test_rmt', 'shard1', 'node2')")

    node1.query("CREATE TABLE test_rmt.rmt_table (id UInt32, value String) ENGINE = ReplicatedMergeTree ORDER BY id")
    node1.query("CREATE TABLE test_rmt.alias_rmt ENGINE = Alias('rmt_table')")

    node1.query("INSERT INTO test_rmt.alias_rmt VALUES (1, 'one')")

    assert node2.query("SELECT * FROM test_rmt.rmt_table") == "1\tone\n"

    node1.query("ALTER TABLE test_rmt.alias_rmt ADD COLUMN time DateTime DEFAULT now()")

    node2.query("SYSTEM SYNC REPLICA test_rmt.rmt_table")

    assert "time" in node1.query("DESCRIBE test_rmt.alias_rmt")
    assert "time" in node2.query("DESCRIBE test_rmt.rmt_table")
    assert "time" in node2.query("DESCRIBE test_rmt.alias_rmt")

    node2.query("INSERT INTO test_rmt.alias_rmt VALUES (2, 'two', now())")

    assert "2\ttwo" in node1.query("SELECT id, value FROM test_rmt.rmt_table WHERE id = 2")
    assert "2\ttwo" in node2.query("SELECT id, value FROM test_rmt.alias_rmt WHERE id = 2")

    node1.query("DROP DATABASE test_rmt SYNC")
    node2.query("DROP DATABASE test_rmt SYNC")