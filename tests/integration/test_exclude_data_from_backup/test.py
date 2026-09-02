import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
)

backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_except_data_from_table_single():
    """Test EXCEPT DATA FROM TABLE with a single MergeTree table"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t VALUES (1), (2), (3)")
    assert instance.query("SELECT count() FROM test.t") == "3\n"

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.t EXCEPT DATA FROM TABLE test.t TO {backup_name}")

    instance.query("DROP TABLE test.t")
    instance.query(f"RESTORE TABLE test.t FROM {backup_name}")

    # Data should NOT be restored (it was excluded), but table/schema should exist.
    assert instance.query("SELECT count() FROM test.t") == "0\n"
    assert instance.query(
        "SELECT name, type FROM system.columns WHERE database='test' AND table='t'"
    ) == "id\tUInt64\n"

    instance.query("DROP TABLE IF EXISTS test.t")


def test_except_data_from_tables_multiple_database_level():
    """Test EXCEPT DATA FROM TABLES with multiple tables at DATABASE level"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t1, test.t2, test.t3")
    instance.query("CREATE TABLE test.t1 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t2 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t3 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t1 VALUES (1), (2)")
    instance.query("INSERT INTO test.t2 VALUES (3), (4)")
    instance.query("INSERT INTO test.t3 VALUES (5), (6)")

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test EXCEPT DATA FROM TABLES t1, t2 TO {backup_name}")

    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    # t1 and t2 have no data (excluded), t3 has data
    assert instance.query("SELECT count() FROM test.t1") == "0\n"
    assert instance.query("SELECT count() FROM test.t2") == "0\n"
    assert instance.query("SELECT count() FROM test.t3") == "2\n"

    instance.query("DROP DATABASE test")


def test_except_data_from_table_all_level():
    """Test EXCEPT DATA FROM TABLE at ALL level"""
    instance.query("CREATE DATABASE IF NOT EXISTS db1")
    instance.query("CREATE DATABASE IF NOT EXISTS db2")
    instance.query("DROP TABLE IF EXISTS db1.t, db2.t")
    instance.query("CREATE TABLE db1.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE db2.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO db1.t VALUES (1)")
    instance.query("INSERT INTO db2.t VALUES (2)")

    backup_name = new_backup_name()
    instance.query(f"BACKUP ALL EXCEPT DATA FROM TABLE db1.t TO {backup_name}")

    instance.query("DROP DATABASE db1")
    instance.query("DROP DATABASE db2")
    instance.query(f"RESTORE ALL FROM {backup_name}")

    # db1.t has no data, db2.t has data
    assert instance.query("SELECT count() FROM db1.t") == "0\n"
    assert instance.query("SELECT count() FROM db2.t") == "1\n"

    instance.query("DROP DATABASE db1")
    instance.query("DROP DATABASE db2")


def test_except_data_coexists_with_except_tables():
    """Test that EXCEPT DATA FROM TABLE coexists with EXCEPT TABLES (full exclusion wins, no error)"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t1, test.t2, test.t3")
    instance.query("CREATE TABLE test.t1 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t2 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t3 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t1 VALUES (1)")
    instance.query("INSERT INTO test.t2 VALUES (2)")
    instance.query("INSERT INTO test.t3 VALUES (3)")

    backup_name = new_backup_name()
    # t1 is both fully excluded and data-excluded (redundancy, not conflict)
    # t2 is only data-excluded
    # t3 is backed up normally
    instance.query(
        f"BACKUP DATABASE test EXCEPT TABLES t1 EXCEPT DATA FROM TABLES t1, t2 TO {backup_name}"
    )

    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    # t1 should not exist (full exclusion wins)
    # t2 should exist with no data
    # t3 should have data
    result = instance.query("SELECT name FROM system.tables WHERE database='test' ORDER BY name")
    assert "t1" not in result
    assert instance.query("SELECT count() FROM test.t2") == "0\n"
    assert instance.query("SELECT count() FROM test.t3") == "1\n"

    instance.query("DROP DATABASE test")


def test_except_data_non_mergetree_engines():
    """Test EXCEPT DATA FROM TABLE with non-MergeTree engines (Log, Memory)"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.log_table, test.mem_table")
    instance.query("CREATE TABLE test.log_table (id UInt64) ENGINE = Log")
    instance.query("CREATE TABLE test.mem_table (id UInt64) ENGINE = Memory")
    instance.query("INSERT INTO test.log_table VALUES (1), (2)")
    instance.query("INSERT INTO test.mem_table VALUES (3), (4)")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP DATABASE test EXCEPT DATA FROM TABLES log_table, mem_table TO {backup_name}"
    )

    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    # Both tables should exist with no data
    assert instance.query("SELECT count() FROM test.log_table") == "0\n"
    assert instance.query("SELECT count() FROM test.mem_table") == "0\n"

    instance.query("DROP DATABASE test")


def test_except_data_materialized_view_propagation():
    """Test that EXCEPT DATA FROM TABLE on a MaterializedView excludes its inner table data"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.mv, test.src")
    instance.query("CREATE TABLE test.src (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query(
        "CREATE MATERIALIZED VIEW test.mv ENGINE = MergeTree ORDER BY id "
        "AS SELECT id FROM test.src"
    )
    instance.query("INSERT INTO test.src VALUES (1), (2), (3)")

    # Wait for MV to process
    import time
    time.sleep(1)

    assert instance.query("SELECT count() FROM test.src") == "3\n"
    assert instance.query("SELECT count() FROM test.mv") == "3\n"

    backup_name = new_backup_name()
    # Exclude data from the OUTER MaterializedView (not the inner table name)
    instance.query(f"BACKUP DATABASE test EXCEPT DATA FROM TABLE mv TO {backup_name}")

    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    # src has data, mv has no data (inner table was excluded via outer MV name)
    assert instance.query("SELECT count() FROM test.src") == "3\n"
    assert instance.query("SELECT count() FROM test.mv") == "0\n"

    instance.query("DROP DATABASE test")


def test_normal_backup_includes_data():
    """Baseline test: normal BACKUP includes data (no exclusion)"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t VALUES (1), (2), (3)")

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.t TO {backup_name}")

    instance.query("DROP TABLE test.t")
    instance.query(f"RESTORE TABLE test.t FROM {backup_name}")

    # Data SHOULD be restored normally
    assert instance.query("SELECT count() FROM test.t") == "3\n"

    instance.query("DROP TABLE IF EXISTS test.t")
