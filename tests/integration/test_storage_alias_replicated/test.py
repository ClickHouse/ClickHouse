import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/config_alias.xml"],
    with_zookeeper=True,
    macros={"shard": "shard1", "replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/config_alias.xml"],
    with_zookeeper=True,
    macros={"shard": "shard1", "replica": "node2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_alias_with_replicated(started_cluster):
    node1.query("CREATE DATABASE test_rmt ENGINE = Replicated('/clickhouse/databases/test_rmt', '{shard}', '{replica}')")
    node2.query("CREATE DATABASE test_rmt ENGINE = Replicated('/clickhouse/databases/test_rmt', '{shard}', '{replica}')")

    node1.query("CREATE TABLE test_rmt.rmt_table (id UInt32, value String) ENGINE = ReplicatedMergeTree ORDER BY id")
    node1.query("CREATE TABLE test_rmt.alias_rmt ENGINE = Alias('rmt_table')")

    node1.query("INSERT INTO test_rmt.alias_rmt VALUES (1, 'one')")

    node2.query("SYSTEM SYNC REPLICA test_rmt.rmt_table")
    assert node2.query("SELECT * FROM test_rmt.rmt_table") == "1\tone\n"

    node1.query("ALTER TABLE test_rmt.alias_rmt ADD COLUMN time DateTime DEFAULT now()")

    node2.query("SYSTEM SYNC DATABASE REPLICA test_rmt")
    assert "time" in node1.query("DESCRIBE test_rmt.alias_rmt")
    assert "time" in node2.query("DESCRIBE test_rmt.rmt_table")
    assert "time" in node2.query("DESCRIBE test_rmt.alias_rmt")

    node2.query("INSERT INTO test_rmt.alias_rmt VALUES (2, 'two', now())")

    node1.query("SYSTEM SYNC REPLICA test_rmt.rmt_table")
    assert "2\ttwo" in node1.query("SELECT id, value FROM test_rmt.rmt_table WHERE id = 2")
    assert "2\ttwo" in node2.query("SELECT id, value FROM test_rmt.alias_rmt WHERE id = 2")

    node1.query("DROP DATABASE test_rmt SYNC")
    node2.query("DROP DATABASE test_rmt SYNC")

def test_alias_empty_args_with_replicated(started_cluster):
    node1.query("CREATE DATABASE d0 ENGINE = Replicated('/clickhouse/path/d0', '{shard}', '{replica}')")

    error = node1.query_and_get_error("CREATE TABLE d0.t0 (c0 Int) ENGINE = Alias()")
    assert "NUMBER_OF_ARGUMENTS_DOESNT_MATCH" in error
    
    node1.query("CREATE TABLE d0.source (c0 Int) ENGINE = MergeTree ORDER BY c0")
    node1.query("CREATE TABLE d0.alias_table ENGINE = Alias('source')")
    node1.query("INSERT INTO d0.source VALUES (1)")
    
    assert node1.query("SELECT * FROM d0.alias_table") == "1\n"
    
    node1.query("DROP DATABASE d0 SYNC")