import pytest
import time
import os

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        try:
            node.query("DROP TABLE IF EXISTS __test_rocksdb_check SYNC")
            node.query("""
                CREATE TABLE __test_rocksdb_check (
                    id UInt32
                ) ENGINE = MergeTreeWithManifest('rocksdb')
                ORDER BY id
            """)
            node.query("DROP TABLE __test_rocksdb_check SYNC")
        except Exception as e:
            error_msg = str(e)
            if "NOT_IMPLEMENTED" in error_msg or "without RocksDB" in error_msg or "USE_ROCKSDB" in error_msg:
                pytest.skip("RocksDB is not available in this build")
            raise
        yield cluster
    finally:
        cluster.shutdown()


def test_basic_manifest_mergetree(started_cluster):
    node.query("DROP TABLE IF EXISTS test_manifest_basic SYNC")
    
    node.query("""
        CREATE TABLE test_manifest_basic (
            id UInt32,
            name String,
            value Int64
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
        SETTINGS index_granularity = 8192
    """)
    
    node.query("INSERT INTO test_manifest_basic VALUES (1, 'test1', 100), (2, 'test2', 200)")
    
    result = node.query("SELECT * FROM test_manifest_basic ORDER BY id")
    assert result.strip() == "1\ttest1\t100\n2\ttest2\t200"
    
    server_path = node.query("SELECT getContext()->getPath()").strip()
    manifest_path = f"{server_path}/manifest/default/test_manifest_basic"
    assert os.path.exists(manifest_path)
    
    node.restart_clickhouse()
    
    result_after_restart = node.query("SELECT * FROM test_manifest_basic ORDER BY id")
    assert result_after_restart.strip() == "1\ttest1\t100\n2\ttest2\t200"
    
    node.query("DROP TABLE test_manifest_basic SYNC")


def test_manifest_mergetree_without_storage_type(started_cluster):
    node.query("DROP TABLE IF EXISTS test_manifest_no_type SYNC")
    
    node.query("""
        CREATE TABLE test_manifest_no_type (
            id UInt32
        ) ENGINE = MergeTreeWithManifest()
        ORDER BY id
    """)
    
    node.query("INSERT INTO test_manifest_no_type VALUES (1), (2), (3)")
    result = node.query("SELECT count() FROM test_manifest_no_type")
    assert result.strip() == "3"
    
    node.query("DROP TABLE test_manifest_no_type SYNC")


def test_manifest_mergetree_show_create_table(started_cluster):
    node.query("DROP TABLE IF EXISTS test_show_create SYNC")
    
    node.query("""
        CREATE TABLE test_show_create (
            id UInt32
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
    """)
    
    result = node.query("SHOW CREATE TABLE test_show_create")
    assert "MergeTreeWithManifest('rocksdb')" in result
    assert "manifest_storage_type" not in result
    
    node.query("DROP TABLE test_show_create SYNC")


def test_manifest_mergetree_restart_persistence(started_cluster):
    node.query("DROP TABLE IF EXISTS test_restart_persistence SYNC")
    
    node.query("""
        CREATE TABLE test_restart_persistence (
            id UInt32,
            data String
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
    """)
    
    for i in range(5):
        node.query(f"INSERT INTO test_restart_persistence VALUES ({i}, 'data{i}')")
    
    result = node.query("SELECT count() FROM test_restart_persistence")
    assert result.strip() == "5"
    
    node.restart_clickhouse()
    
    result_after = node.query("SELECT count() FROM test_restart_persistence")
    assert result_after.strip() == "5"
    
    result_all = node.query("SELECT * FROM test_restart_persistence ORDER BY id")
    expected = "\n".join([f"{i}\tdata{i}" for i in range(5)])
    assert result_all.strip() == expected
    
    node.query("DROP TABLE test_restart_persistence SYNC")


def test_manifest_mergetree_merge_operations(started_cluster):
    node.query("DROP TABLE IF EXISTS test_merge_ops SYNC")
    
    node.query("""
        CREATE TABLE test_merge_ops (
            id UInt32,
            value Int64
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
        SETTINGS index_granularity = 8192
    """)
    
    for i in range(10):
        node.query(f"INSERT INTO test_merge_ops VALUES ({i}, {i * 10})")
    
    node.query("OPTIMIZE TABLE test_merge_ops FINAL")
    
    result = node.query("SELECT count() FROM test_merge_ops")
    assert result.strip() == "10"
    
    node.restart_clickhouse()
    result_after = node.query("SELECT count() FROM test_merge_ops")
    assert result_after.strip() == "10"
    
    node.query("DROP TABLE test_merge_ops SYNC")


def test_manifest_mergetree_multiple_tables(started_cluster):
    node.query("DROP TABLE IF EXISTS test_multi1 SYNC")
    node.query("DROP TABLE IF EXISTS test_multi2 SYNC")
    
    node.query("""
        CREATE TABLE test_multi1 (
            id UInt32
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
    """)
    
    node.query("""
        CREATE TABLE test_multi2 (
            id UInt32
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
    """)
    
    node.query("INSERT INTO test_multi1 VALUES (1), (2)")
    node.query("INSERT INTO test_multi2 VALUES (10), (20)")
    
    result1 = node.query("SELECT * FROM test_multi1 ORDER BY id")
    result2 = node.query("SELECT * FROM test_multi2 ORDER BY id")
    
    assert result1.strip() == "1\n2"
    assert result2.strip() == "10\n20"
    
    node.restart_clickhouse()
    
    result1_after = node.query("SELECT * FROM test_multi1 ORDER BY id")
    result2_after = node.query("SELECT * FROM test_multi2 ORDER BY id")
    
    assert result1_after.strip() == "1\n2"
    assert result2_after.strip() == "10\n20"
    
    node.query("DROP TABLE test_multi1 SYNC")
    node.query("DROP TABLE test_multi2 SYNC")


def test_manifest_mergetree_part_removal(started_cluster):
    node.query("DROP TABLE IF EXISTS test_part_removal SYNC")
    
    node.query("""
        CREATE TABLE test_part_removal (
            id UInt32,
            partition_id UInt32
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
        PARTITION BY partition_id
    """)
    
    node.query("INSERT INTO test_part_removal VALUES (1, 1), (2, 1)")
    node.query("INSERT INTO test_part_removal VALUES (3, 2), (4, 2)")
    
    node.query("ALTER TABLE test_part_removal DROP PARTITION 1")
    
    result = node.query("SELECT * FROM test_part_removal ORDER BY id")
    assert result.strip() == "3\t2\n4\t2"
    
    node.restart_clickhouse()
    result_after = node.query("SELECT * FROM test_part_removal ORDER BY id")
    assert result_after.strip() == "3\t2\n4\t2"
    
    node.query("DROP TABLE test_part_removal SYNC")


def test_manifest_mergetree_mutation_delete(started_cluster):
    node.query("DROP TABLE IF EXISTS test_mutation_delete SYNC")
    
    node.query("""
        CREATE TABLE test_mutation_delete (
            id UInt32,
            value Int32,
            status String
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
    """)
    
    node.query("INSERT INTO test_mutation_delete VALUES (1, 10, 'active'), (2, 20, 'inactive'), (3, 30, 'active')")
    
    result = node.query("SELECT * FROM test_mutation_delete ORDER BY id")
    assert result.strip() == "1\t10\tactive\n2\t20\tinactive\n3\t30\tactive"
    
    node.query("ALTER TABLE test_mutation_delete DELETE WHERE status = 'inactive'")
    
    for _ in range(30):
        if node.query("SELECT is_done FROM system.mutations WHERE table = 'test_mutation_delete' ORDER BY create_time DESC LIMIT 1").strip() == "1":
            break
        time.sleep(1)
    else:
        pytest.fail("Mutation did not complete in time")
    
    result_after = node.query("SELECT * FROM test_mutation_delete ORDER BY id")
    assert result_after.strip() == "1\t10\tactive\n3\t30\tactive"
    
    node.restart_clickhouse()
    result_after_restart = node.query("SELECT * FROM test_mutation_delete ORDER BY id")
    assert result_after_restart.strip() == "1\t10\tactive\n3\t30\tactive"
    
    node.query("DROP TABLE test_mutation_delete SYNC")


def test_manifest_mergetree_mutation_update(started_cluster):
    node.query("DROP TABLE IF EXISTS test_mutation_update SYNC")
    
    node.query("""
        CREATE TABLE test_mutation_update (
            id UInt32,
            value Int32,
            status String
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
    """)
    
    node.query("INSERT INTO test_mutation_update VALUES (1, 10, 'old'), (2, 20, 'old'), (3, 30, 'old')")
    
    result = node.query("SELECT * FROM test_mutation_update ORDER BY id")
    assert result.strip() == "1\t10\told\n2\t20\told\n3\t30\told"
    
    node.query("ALTER TABLE test_mutation_update UPDATE status = 'new' WHERE value > 15")
    
    for _ in range(30):
        if node.query("SELECT is_done FROM system.mutations WHERE table = 'test_mutation_update' ORDER BY create_time DESC LIMIT 1").strip() == "1":
            break
        time.sleep(1)
    else:
        pytest.fail("Mutation did not complete in time")
    
    result_after = node.query("SELECT * FROM test_mutation_update ORDER BY id")
    assert result_after.strip() == "1\t10\told\n2\t20\tnew\n3\t30\tnew"
    
    node.restart_clickhouse()
    result_after_restart = node.query("SELECT * FROM test_mutation_update ORDER BY id")
    assert result_after_restart.strip() == "1\t10\told\n2\t20\tnew\n3\t30\tnew"
    
    node.query("DROP TABLE test_mutation_update SYNC")


def test_manifest_mergetree_mutation_multiple_parts(started_cluster):
    node.query("DROP TABLE IF EXISTS test_mutation_multi SYNC")
    
    node.query("""
        CREATE TABLE test_mutation_multi (
            id UInt32,
            value Int32
        ) ENGINE = MergeTreeWithManifest('rocksdb')
        ORDER BY id
        SETTINGS index_granularity = 8192
    """)
    
    for i in range(5):
        node.query(f"INSERT INTO test_mutation_multi VALUES ({i}, {i * 10})")
    
    result = node.query("SELECT count() FROM test_mutation_multi")
    assert result.strip() == "5"
    
    node.query("ALTER TABLE test_mutation_multi DELETE WHERE value < 20")
    
    for _ in range(30):
        if node.query("SELECT is_done FROM system.mutations WHERE table = 'test_mutation_multi' ORDER BY create_time DESC LIMIT 1").strip() == "1":
            break
        time.sleep(1)
    else:
        pytest.fail("Mutation did not complete in time")
    
    result_after = node.query("SELECT * FROM test_mutation_multi ORDER BY id")
    assert result_after.strip() == "2\t20\n3\t30\n4\t40"
    
    node.restart_clickhouse()
    result_after_restart = node.query("SELECT * FROM test_mutation_multi ORDER BY id")
    assert result_after_restart.strip() == "2\t20\n3\t30\n4\t40"
    
    node.query("DROP TABLE test_mutation_multi SYNC")
