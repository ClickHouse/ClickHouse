import pytest
import urllib.parse
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    macros={"replica": "node2"},
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # Replace NODE_NAME placeholder with actual IP addresses in config
        import os
        config_path = os.path.join(
            os.path.dirname(__file__), "configs/config.xml"
        )

        for node in [node1, node2]:
            config_content = open(config_path).read()
            config_content = config_content.replace("NODE_NAME", node.ip_address)

            # Debug: print the config and IP address
            print(f"Configuring {node.name} with IP {node.ip_address}")
            print(f"Config content:\n{config_content}")

            node.exec_in_container(
                ["bash", "-c", 'echo "${NEW_CONFIG}" > /etc/clickhouse-server/config.d/interserver_host.xml'],
                environment={"NEW_CONFIG": config_content},
            )

            # Verify the file was written
            result = node.exec_in_container(
                ["cat", "/etc/clickhouse-server/config.d/interserver_host.xml"]
            )
            print(f"Verification - Config file on {node.name}:\n{result}")

            # IMPORTANT: interserver_http_host is only loaded at startup, not by SYSTEM RELOAD CONFIG
            # So we need to restart ClickHouse
            print(f"Restarting ClickHouse on {node.name} to apply interserver_http_host config...")
            node.restart_clickhouse()

            # Verify the setting was applied
            interserver_host = node.query("SELECT value FROM system.server_settings WHERE name = 'interserver_http_host'")
            print(f"Verification - interserver_http_host setting on {node.name}: {interserver_host}")

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
