import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config_information.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_check_can_drop_partition_and_table(start_cluster):
    node.query(
        "DROP TABLE IF EXISTS test_drop SYNC SETTINGS max_table_size_to_drop=12345678;"
    )
    node.query("CREATE TABLE test_drop (a UInt64) ENGINE=MergeTree() ORDER BY tuple();")
    node.query("INSERT INTO test_drop SELECT * FROM system.numbers_mt LIMIT 1000000;")
    node.query("OPTIMIZE TABLE test_drop FINAL;")

    assert "was not dropped" in node.query_and_get_error(
        "ALTER TABLE test_drop DROP PARTITION ID 'all';"
    )
    node.query(
        "ALTER TABLE test_drop DROP PARTITION ID 'all' SETTINGS max_partition_size_to_drop=12345678;"
    )

    node.query("INSERT INTO test_drop SELECT * FROM system.numbers_mt LIMIT 100000;")

    assert "was not dropped" in node.query_and_get_error("DROP TABLE test_drop SYNC;")
    node.query("DROP TABLE test_drop SYNC SETTINGS max_table_size_to_drop=12345678;")
