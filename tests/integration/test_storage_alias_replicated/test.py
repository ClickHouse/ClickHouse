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


def test_alias_with_parallel_replicas(started_cluster):
    """Verify that an Alias support parallel replicas read."""
    node1.query(
        "CREATE DATABASE test_pr ENGINE = Replicated('/clickhouse/databases/test_pr', '{shard}', '{replica}')"
    )
    node2.query(
        "CREATE DATABASE test_pr ENGINE = Replicated('/clickhouse/databases/test_pr', '{shard}', '{replica}')"
    )

    node1.query(
        "CREATE TABLE test_pr.base_table (id UInt32, value String) "
        "ENGINE = ReplicatedMergeTree ORDER BY id "
        "SETTINGS index_granularity = 1"
    )
    node1.query("CREATE TABLE test_pr.alias_table ENGINE = Alias('base_table')")
    # Ensure alias_table DDL has replicated to node2 before we send it work.
    node2.query("SYSTEM SYNC DATABASE REPLICA test_pr")

    node1.query(
        "INSERT INTO test_pr.base_table SELECT number, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA test_pr.base_table")

    result = node1.query(
        "SELECT sum(id) FROM test_pr.alias_table",
        settings={
            "enable_parallel_replicas": 1,
            "max_parallel_replicas": 2,
            "cluster_for_parallel_replicas": "test_cluster",
            "log_comment": "test_alias_parallel_replicas",
        },
    )

    assert result.strip() == "499500"

    # Verify parallel replicas were actually used
    node1.query("SYSTEM FLUSH LOGS")
    used_count = node1.query(
        "SELECT ProfileEvents['ParallelReplicasUsedCount'] "
        "FROM system.query_log "
        "WHERE log_comment = 'test_alias_parallel_replicas' "
        "  AND is_initial_query = 1 "
        "  AND type = 'QueryFinish' "
        "ORDER BY query_start_time DESC LIMIT 1"
    )
    assert int(used_count.strip()) > 0, (
        f"Expected ParallelReplicasUsedCount > 0, got {used_count.strip()}"
    )

    node1.query("DROP DATABASE test_pr SYNC")
    node2.query("DROP DATABASE test_pr SYNC")


def test_alias_check_table(started_cluster):
    """Verify that CHECK TABLE and CHECK DATABASE correctly delegate to the target table when the database contains an Alias table."""
    node1.query("CREATE DATABASE check_db")
    node1.query(
        "CREATE TABLE check_db.base_table (id UInt32) ENGINE = MergeTree ORDER BY id"
    )
    node1.query("CREATE TABLE check_db.alias_table ENGINE = Alias('base_table')")
    node1.query("INSERT INTO check_db.base_table VALUES (1), (2), (3)")

    # CHECK TABLE on alias must delegate to the target and report success.
    result = node1.query(
        "CHECK TABLE check_db.alias_table",
        settings={"check_query_single_value_result": 1},
    )
    assert result.strip() == "1", f"Expected 1, got: {result.strip()}"

    # CHECK DATABASE must not abort when the database contains alias tables.
    node1.query("CHECK DATABASE check_db")

    node1.query("DROP DATABASE check_db SYNC")