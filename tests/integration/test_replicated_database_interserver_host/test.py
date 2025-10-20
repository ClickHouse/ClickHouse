import pytest
import urllib.parse
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    macros={"replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
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


def test_replicated_database_uses_interserver_host(started_cluster):
    """Test that DatabaseReplicated uses interserver_http_host for replica registration."""

    node1.query(
        "CREATE DATABASE test_db ENGINE = Replicated('/clickhouse/databases/test_db', 'shard1', 'node1')"
    )
    node2.query(
        "CREATE DATABASE test_db ENGINE = Replicated('/clickhouse/databases/test_db', 'shard1', 'node2')"
    )

    node1.query("SYSTEM SYNC DATABASE REPLICA test_db")
    node2.query("SYSTEM SYNC DATABASE REPLICA test_db")

    zk_path = "/clickhouse/databases/test_db/replicas"
    host_ids = node1.query(
        f"SELECT value FROM system.zookeeper WHERE path = '{zk_path}'"
    )
    host_ids_decoded = urllib.parse.unquote(host_ids)

    # Verify that IP addresses are used instead of hostnames
    assert "node1" not in host_ids_decoded and "node2" not in host_ids_decoded, \
        f"Expected IP addresses instead of hostnames, got: {host_ids_decoded}"

    # Verify that TCP port (9000) is used, not interserver_http_port (9009)
    assert ":9000:" in host_ids_decoded, \
        f"Expected TCP port 9000 in host_id, got: {host_ids_decoded}"

    node1.query("CREATE TABLE test_db.test_table (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id")
    node2.query("SYSTEM SYNC DATABASE REPLICA test_db")

    tables_on_node2 = node2.query("SHOW TABLES FROM test_db")
    assert "test_table" in tables_on_node2

    node1.query("INSERT INTO test_db.test_table VALUES (1), (2), (3)")
    node2.query("SYSTEM SYNC REPLICA test_db.test_table")

    count_on_node2 = node2.query("SELECT count() FROM test_db.test_table")
    assert count_on_node2.strip() == "3"

    node1.query("DROP DATABASE test_db SYNC")
    node2.query("DROP DATABASE test_db SYNC")
