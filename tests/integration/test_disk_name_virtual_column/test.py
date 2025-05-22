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
    
    # Create a table with storage policy
    node.query("""
        CREATE TABLE test_disk_name_multi (
            id UInt32,
            value String
        ) ENGINE = MergeTree()
        ORDER BY id
        PARTITION by id
        SETTINGS storage_policy = 'default'
    """)
    
    try:
        # Insert data in multiple parts
        node.query("INSERT INTO test_disk_name_multi VALUES (1, 'test1'), (2, 'test2')")
        
        # Move first part to local disk 1
        node.query("ALTER TABLE test_disk_name_multi MOVE PARTITION 1 TO DISK 'disk_local2'")
        
        # Move second part to local disk 2
        node.query("ALTER TABLE test_disk_name_multi MOVE PARTITION 2 TO DISK 'disk_local3'")
        
        # Verify data is distributed across disks
        result = node.query("SELECT id, value, _disk_name FROM test_disk_name_multi ORDER BY id")
        assert result == "1\ttest1\tdisk_local2\n2\ttest2\tdisk_local3\n"
        
        result = node.query("SELECT id, value FROM test_disk_name_multi WHERE _disk_name = 'disk_local1' ORDER BY id")
        assert result == ""
        
        result = node.query("SELECT id, value FROM test_disk_name_multi WHERE _disk_name = 'disk_local2' ORDER BY id")
        assert result == "1\ttest1\n"
        
        result = node.query("SELECT id, value FROM test_disk_name_multi WHERE _disk_name = 'disk_local3' ORDER BY id")
        assert result == "2\ttest2\n"
        
    finally:
        node.query("DROP TABLE IF EXISTS test_disk_name_multi")
