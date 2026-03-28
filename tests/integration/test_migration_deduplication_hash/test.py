import logging
import pytest
from contextlib import contextmanager

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node_2512",
            macros={"replica": "node_2512"},
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag="25.12",
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_old",
            macros={"replica": "node_old"},
            main_configs=[
                "configs/migration_old.xml",
            ],
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_compatible",
            macros={"replica": "node_compatible"},
            main_configs=[
                "configs/migration_compatible.xml",
            ],
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_new",
            macros={"replica": "node_new"},
            main_configs=[
                "configs/migration_new.xml",
            ],
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_restart",
            macros={"replica": "node_restart"},
            main_configs=[
                "configs/migration_restart.xml",
            ],
            with_zookeeper=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@contextmanager
def with_tables(nodes, create_query, drop_query):
    try:
        for node in nodes:
            node.query(create_query)

        yield
    finally:
        for node in nodes:
            node.query(drop_query)


@pytest.mark.parametrize("insert_type", ["sync", "async"])
def test_replicated_table_migration(cluster, insert_type):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_replicated_table_migration;

    CREATE TABLE test_replicated_table_migration (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_replicated_table_migration/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_replicated_table_migration SYNC"

    node_old = cluster.instances["node_old"]
    node_compatible = cluster.instances["node_compatible"]
    node_new = cluster.instances["node_new"]

    all_nodes = [node_old, node_compatible, node_new]

    def check_insert_deduplicated_across_nodes(nodes, insert_type):
        for node in nodes:
            async_insert = insert_type == "async"
            node.query(f"INSERT INTO test_replicated_table_migration SETTINGS async_insert={1 if async_insert else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_replicated_table_migration")

        for node in nodes:
            result = node.query("SELECT * FROM test_replicated_table_migration ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_replicated_table_migration")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_replicated_table_migration")

    with with_tables(
        nodes=all_nodes,
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        check_insert_deduplicated_across_nodes([node_old, node_compatible], insert_type)
        check_insert_deduplicated_across_nodes([node_compatible, node_old], insert_type)

        check_insert_deduplicated_across_nodes([node_compatible, node_new], insert_type)
        check_insert_deduplicated_across_nodes([node_new, node_compatible], insert_type)



def test_not_replicated_table_migration(cluster):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_not_replicated_table_migration;

    CREATE TABLE test_not_replicated_table_migration (key UInt32, value UInt32)
    ENGINE=MergeTree
    PRIMARY KEY key ORDER BY key
    SETTINGS non_replicated_deduplication_window = 10000
"""

    drop_table_query = "DROP TABLE IF EXISTS test_not_replicated_table_migration SYNC"

    config_old = \
"""
<clickhouse>
    <insert_deduplication_version>old_separate_hashes</insert_deduplication_version>
</clickhouse>
"""

    config_compatible = \
"""
<clickhouse>
    <insert_deduplication_version>compatible_double_hashes</insert_deduplication_version>
</clickhouse>
"""

    config_new = \
"""
<clickhouse>
    <insert_deduplication_version>new_unified_hash</insert_deduplication_version>
</clickhouse>
"""

    def check_insert_deduplicated_across_configs(node, first_config, second_config):
        node.replace_config("/etc/clickhouse-server/config.d/migration_restart.xml", first_config)
        node.restart_clickhouse()

        node.query("INSERT INTO test_not_replicated_table_migration VALUES (1, 100), (2, 200), (3, 300)")

        node.replace_config("/etc/clickhouse-server/config.d/migration_restart.xml", second_config)
        node.restart_clickhouse()

        node.query("INSERT INTO test_not_replicated_table_migration VALUES (1, 100), (2, 200), (3, 300)")

        result = node.query("SELECT * FROM test_not_replicated_table_migration ORDER BY key")
        assert result == "1\t100\n2\t200\n3\t300\n"

        node.query("TRUNCATE TABLE test_not_replicated_table_migration")

    node = cluster.instances["node_restart"]

    with with_tables(
        nodes=[node],
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        check_insert_deduplicated_across_configs(node, config_old, config_compatible)
        check_insert_deduplicated_across_configs(node, config_compatible, config_new)


@pytest.mark.parametrize("first_insert_type", ["sync", "async"])
@pytest.mark.parametrize("second_insert_type", ["sync", "async"])
@pytest.mark.parametrize("first_node", ["node_compatible", "node_new"])
@pytest.mark.parametrize("second_node", ["node_compatible", "node_new"])
def test_sync_async_deduplicated(cluster, first_insert_type, second_insert_type, first_node, second_node):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_sync_async_deduplicated;

    CREATE TABLE test_sync_async_deduplicated (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_sync_async_deduplicated/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_sync_async_deduplicated SYNC"

    def check_insert_deduplicated_across_nodes(node1, node2, type_first, type_second):
        async_type_first = type_first == "async"
        node1.query(f"INSERT INTO test_sync_async_deduplicated SETTINGS async_insert={1 if async_type_first else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")
        async_type_second = type_second == "async"
        node2.query(f"INSERT INTO test_sync_async_deduplicated SETTINGS async_insert={1 if async_type_second else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")

        nodes = [node1, node2]

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated")

        for node in nodes:
            result = node.query("SELECT * FROM test_sync_async_deduplicated ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_sync_async_deduplicated")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated")

    node1 = cluster.instances[first_node]
    node2 = cluster.instances[second_node]

    with with_tables(
        nodes=[node1] if (first_node == second_node) else [node1, node2],
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        check_insert_deduplicated_across_nodes(node1, node2, first_insert_type, second_insert_type)


@pytest.mark.parametrize("insert_type", ["sync", "async"])
def test_sync_async_deduplicated_with_2512(cluster, insert_type):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_sync_async_deduplicated_with_2512;

    CREATE TABLE test_sync_async_deduplicated_with_2512 (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_sync_async_deduplicated_with_2512/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_sync_async_deduplicated_with_2512 SYNC"

    node_2512 = cluster.instances["node_2512"]
    node_compatible = cluster.instances["node_compatible"]
    nodes = [node_2512, node_compatible]

    with with_tables(
        nodes=nodes,
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        async_insert = insert_type == "async"
        node_2512.query(f"INSERT INTO test_sync_async_deduplicated_with_2512 SETTINGS async_insert={1 if async_insert else 0}, insert_deduplicate=1, async_insert_deduplicate=1 VALUES (1, 100), (2, 200), (3, 300)")
        node_compatible.query(f"INSERT INTO test_sync_async_deduplicated_with_2512 SETTINGS async_insert={1 if async_insert else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated_with_2512")

        for node in nodes:
            result = node.query("SELECT * FROM test_sync_async_deduplicated_with_2512 ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_sync_async_deduplicated_with_2512")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated_with_2512")
