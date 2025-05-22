import pytest

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.test_tools import TSV

disk_types = {
    "default": "Local",
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


def test_disk_name_virtual_column_basic(cluster):
    node = cluster.instances["node"]
    
    # Create a table with storage policy
    node.query("""
        CREATE TABLE test_disk_name (
            id UInt32,
            value String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS storage_policy = 'default'
    """)
    
    try:
        # Insert some data
        node.query("INSERT INTO test_disk_name VALUES (1, 'test1'), (2, 'test2')")
        
        # Verify data is on default disk
        result = node.query("SELECT id, value, _disk_name FROM test_disk_name ORDER BY id")
        assert result == "1\ttest1\tdefault\n2\ttest2\tdefault\n"
        
        # Move data to local disk 1
        node.query("ALTER TABLE test_disk_name MOVE PARTITION tuple() TO DISK 'disk_local1'")
        
        # Verify data is now on local disk 1
        result = node.query("SELECT id, value, _disk_name FROM test_disk_name ORDER BY id")
        assert result == "1\ttest1\tdisk_local1\n2\ttest2\tdisk_local1\n"
        
        # Test filtering by _disk_name
        result = node.query("SELECT id, value FROM test_disk_name WHERE _disk_name = 'disk_local1' ORDER BY id")
        assert result == "1\ttest1\n2\ttest2\n"
        
        # Test filtering with non-matching disk name
        result = node.query("SELECT id, value FROM test_disk_name WHERE _disk_name = 'default' ORDER BY id")
        assert result == ""
        
    finally:
        node.query("DROP TABLE IF EXISTS test_disk_name")


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
        node.query("INSERT INTO test_disk_name_multi VALUES (3, 'test3'), (4, 'test4')")
        
        # Move first part to local disk 1
        node.query("ALTER TABLE test_disk_name_multi MOVE PARTITION 1 TO DISK 'disk_local1'")
        
        # Move second part to local disk 2
        node.query("ALTER TABLE test_disk_name_multi MOVE PARTITION 3 TO DISK 'disk_local2'")
        
        # Verify data is distributed across disks
        result = node.query("SELECT id, value, _disk_name FROM test_disk_name_multi ORDER BY id")
        assert result == "1\ttest1\tdisk_local1\n2\ttest2\tdisk_local1\n3\ttest3\tdisk_local2\n4\ttest4\tdisk_local2\n"
        
        # Test filtering by _disk_name for local disk 1
        result = node.query("SELECT id, value FROM test_disk_name_multi WHERE _disk_name = 'disk_local1' ORDER BY id")
        assert result == "1\ttest1\n2\ttest2\n"
        
        # Test filtering by _disk_name for local disk 2
        result = node.query("SELECT id, value FROM test_disk_name_multi WHERE _disk_name = 'disk_local2' ORDER BY id")
        assert result == "3\ttest3\n4\ttest4\n"
        
        # Test filtering with non-matching disk name
        result = node.query("SELECT id, value FROM test_disk_name_multi WHERE _disk_name = 'disk_local3' ORDER BY id")
        assert result == ""
        
    finally:
        node.query("DROP TABLE IF EXISTS test_disk_name_multi")


def test_disk_name_virtual_column_all_disks(cluster):
    node = cluster.instances["node"]
    
    # Create a table with storage policy
    node.query("""
        CREATE TABLE test_disk_name_all (
            id UInt32,
            value String
        ) ENGINE = MergeTree()
        ORDER BY id
        PARTITION by id
        SETTINGS storage_policy = 'default'
    """)
    
    try:
        # Insert data in multiple parts
        node.query("INSERT INTO test_disk_name_all VALUES (1, 'test1')")
        node.query("INSERT INTO test_disk_name_all VALUES (2, 'test2')")
        node.query("INSERT INTO test_disk_name_all VALUES (3, 'test3')")
        node.query("INSERT INTO test_disk_name_all VALUES (4, 'test4')")
        
        # Move data to different local disks
        node.query("ALTER TABLE test_disk_name_all MOVE PARTITION 1 TO DISK 'disk_local1'")
        node.query("ALTER TABLE test_disk_name_all MOVE PARTITION 2 TO DISK 'disk_local2'")
        node.query("ALTER TABLE test_disk_name_all MOVE PARTITION 3 TO DISK 'disk_local3'")
        # Leave partition 4 on default disk
        
        # Verify data is distributed across all disks
        result = node.query("SELECT id, value, _disk_name FROM test_disk_name_all ORDER BY id")
        assert result == "1\ttest1\tdisk_local1\n2\ttest2\tdisk_local2\n3\ttest3\tdisk_local3\n4\ttest4\tdefault\n"
        
        # Test filtering for each disk
        for disk in ['default', 'disk_local1', 'disk_local2', 'disk_local3']:
            result = node.query(f"SELECT id, value FROM test_disk_name_all WHERE _disk_name = '{disk}' ORDER BY id")
            if disk == 'default':
                assert result == "4\ttest4\n"
            else:
                disk_num = disk[-1]  # Get the number from disk name
                assert result == f"{disk_num}\ttest{disk_num}\n"
        
    finally:
        node.query("DROP TABLE IF EXISTS test_disk_name_all")