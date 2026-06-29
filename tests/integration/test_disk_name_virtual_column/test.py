import pytest

from helpers.cluster import ClickHouseCluster

disk_types = {
    "disk_local1": "Local",
    "disk_local2": "Local",
    "disk_local3": "Local",
}


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=(
                ["configs/config.xml"]
            ),
            with_minio=False,
            with_remote_database_disk=False,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_disk_name_virtual_column_multiple_disks(cluster):
    node = cluster.instances["node"]

    create_query_statement = """
        CREATE TABLE test_disk_name_multi{disk_number} (
            id UInt32,
            value String
        ) ENGINE = MergeTree()
        ORDER BY id
        PARTITION by id
        SETTINGS storage_policy = 'disk_local{disk_number}'
    """

    # Create one table for each storage policy
    node.query(create_query_statement.format(disk_number="1"))
    node.query(create_query_statement.format(disk_number="2"))
    node.query(create_query_statement.format(disk_number="3"))

    try:
        # Insert data into each table
        node.query("INSERT INTO test_disk_name_multi1 VALUES (1, 'test1')")
        node.query("INSERT INTO test_disk_name_multi2 VALUES (2, 'test2')")
        node.query("INSERT INTO test_disk_name_multi3 VALUES (3, 'test3')")

        # Verify data is distributed across tables and disks
        result = node.query("SELECT id, value, _disk_name FROM merge('default', '^test_disk_name_multi') ORDER BY id")
        assert result == "1\ttest1\tdisk_local1\n2\ttest2\tdisk_local2\n3\ttest3\tdisk_local3\n"

        result = node.query("SELECT id, value FROM test_disk_name_multi1 WHERE _disk_name = 'disk_local1' ORDER BY id")
        assert result == "1\ttest1\n"

        result = node.query("SELECT id, value FROM test_disk_name_multi2 WHERE _disk_name = 'disk_local2' ORDER BY id")
        assert result == "2\ttest2\n"

        result = node.query("SELECT id, value FROM test_disk_name_multi3 WHERE _disk_name = 'disk_local3' ORDER BY id")
        assert result == "3\ttest3\n"

    finally:
        node.query("DROP TABLE IF EXISTS test_disk_name_multi1")
        node.query("DROP TABLE IF EXISTS test_disk_name_multi2")
        node.query("DROP TABLE IF EXISTS test_disk_name_multi3")
