import pytest

from helpers.cluster import ClickHouseCluster, CLICKHOUSE_CI_MIN_TESTED_VERSION

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.add_instance(
            "node",
            with_zookeeper=False,
            image="clickhouse/clickhouse-server",
            tag=max(
                CLICKHOUSE_CI_MIN_TESTED_VERSION, "24.5"
            ),  # older version doesn't support SETTINGS
            stay_alive=True,
            with_installed_binary=True,
        )
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


# Test for compatibility of rocksdb tables when upgrade rocksdb version to 9.2 then rollback
# Rocksdb table created by newer version should be readable by older version
def test_data_compatibility_when_rollback(start_cluster):
    try:
        # Start with current version and create rocksdb table
        node = cluster.instances["node"]
        node.restart_with_latest_version()
        create_query1 = """CREATE TABLE dict1 (key UInt64, value String)
            ENGINE = EmbeddedRocksDB()
            PRIMARY KEY key"""
        create_query2 = """CREATE TABLE dict2 (key UInt64, value String)
            ENGINE = EmbeddedRocksDB()
            PRIMARY KEY key
            SETTINGS optimize_for_bulk_insert = 0"""

        node.query(create_query1)
        node.query(create_query2)
        node.query("INSERT INTO dict1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        node.query("INSERT INTO dict2 VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        # Restart with older version, we should be able to read the data
        node.restart_with_original_version()
        assert node.query("SELECT count() FROM dict1 WHERE key > 1") == "2\n"
        assert node.query("SELECT count() FROM dict2 WHERE key > 1") == "2\n"

    finally:
        node.query("DROP TABLE IF EXISTS dict1")
        node.query("DROP TABLE IF EXISTS dict2")
