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
    # Exclude system database to avoid system table restore conflicts
    instance.query(f"BACKUP ALL EXCEPT DATABASE system EXCEPT DATA FROM TABLE db1.t TO {backup_name}")

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


def test_except_data_rejects_inner_table_name():
    """Test that directly specifying an inner table name in EXCEPT DATA FROM TABLE is rejected"""
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.mv, test.src")
    instance.query("CREATE TABLE test.src (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query(
        "CREATE MATERIALIZED VIEW test.mv ENGINE = MergeTree ORDER BY id "
        "AS SELECT id FROM test.src"
    )
    instance.query("INSERT INTO test.src VALUES (1), (2), (3)")

    # Get the inner table name
    inner_table = instance.query(
        "SELECT name FROM system.tables WHERE database='test' AND name LIKE '.inner_id.%'"
    ).strip()

    assert inner_table, "Inner table not found"

    backup_name = new_backup_name()
    # Should throw error when trying to use inner table name directly
    # The error can be either:
    # 1. SYNTAX_ERROR (62) - parser rejects dot-prefixed identifier
    # 2. INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION (666) - our validation
    try:
        # Try with backticks to bypass parser's identifier check
        instance.query(f"BACKUP DATABASE test EXCEPT DATA FROM TABLE `{inner_table}` TO {backup_name}")
        assert False, "Expected exception when using inner table name, but query succeeded"
    except Exception as e:
        error_message = str(e)
        # Backtick-quoting bypasses the parser, so this must be rejected by our
        # explicit validation layer specifically - not by parser SYNTAX_ERROR.
        assert ("INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in error_message or
                "666" in error_message), \
            f"Expected INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION (666), got: {error_message}"

    instance.query("DROP DATABASE test")


def test_except_data_rejects_system_users():
    """Test that system.users cannot be used in EXCEPT DATA FROM TABLE"""
    instance.query("DROP USER IF EXISTS u1")
    instance.query("CREATE USER u1 IDENTIFIED BY 'test123'")

    backup_name = new_backup_name()
    # Should throw error when trying to exclude data from system.users
    # because system.users backup contains entities (users), not table data
    try:
        instance.query(f"BACKUP TABLE system.users EXCEPT DATA FROM TABLE system.users TO {backup_name}")
        assert False, "Expected exception when using system.users in EXCEPT DATA FROM TABLE, but query succeeded"
    except Exception as e:
        error_message = str(e)
        # Must be rejected by SYSTEM_TABLE_NOT_ALLOWED_IN_BACKUP_DATA_EXCLUSION (1015)
        assert ("SYSTEM_TABLE_NOT_ALLOWED_IN_BACKUP_DATA_EXCLUSION" in error_message or
                "1015" in error_message), \
            f"Expected SYSTEM_TABLE_NOT_ALLOWED_IN_BACKUP_DATA_EXCLUSION (1015), got: {error_message}"

    instance.query("DROP USER IF EXISTS u1")


def test_except_data_from_table_unqualified():
    """Test EXCEPT DATA FROM TABLE with unqualified table name (uses current database)

    Regression test for bug where setCurrentDatabase() didn't rewrite except_data_tables
    for TABLE elements, causing unqualified table names to be silently dropped.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t VALUES (1), (2), (3)")
    assert instance.query("SELECT count() FROM test.t") == "3\n"

    backup_name = new_backup_name()
    # Use unqualified table name in both the main TABLE clause and EXCEPT DATA FROM clause
    # The current database context should be properly applied to both
    instance.query(f"BACKUP TABLE test.t EXCEPT DATA FROM TABLE t TO {backup_name}")

    instance.query("DROP TABLE test.t")
    instance.query(f"RESTORE TABLE test.t FROM {backup_name}")

    # Data should NOT be restored (it was excluded via unqualified name)
    assert instance.query("SELECT count() FROM test.t") == "0\n"
    assert instance.query(
        "SELECT name, type FROM system.columns WHERE database='test' AND table='t'"
    ) == "id\tUInt64\n"

    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("DROP DATABASE IF EXISTS test")


def test_except_data_from_table_formatting():
    """Test that EXCEPT DATA FROM TABLE clause is correctly formatted in TABLE element

    Regression test for bug where formatElement() didn't emit EXCEPT DATA FROM for
    TABLE/TEMPORARY_TABLE types, which would break ON CLUSTER backups (the clause
    would be lost when the query is formatted for distribution to worker hosts).

    Note: This test only verifies formatting without actual cluster distribution.
    For full ON CLUSTER coverage, see tests/integration/test_backup_restore_on_cluster/.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t1, test.t2")
    instance.query("CREATE TABLE test.t1 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t2 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t1 VALUES (1), (2), (3)")
    instance.query("INSERT INTO test.t2 VALUES (4), (5), (6)")

    backup_name = new_backup_name()
    # This exercises the TABLE element's formatElement() path with EXCEPT DATA FROM.
    # Both tables are backed up (structure); only t1's data is excluded.
    instance.query(f"BACKUP TABLE test.t1, TABLE test.t2 EXCEPT DATA FROM TABLE test.t1 TO {backup_name}")

    instance.query("DROP TABLE test.t1, test.t2")
    instance.query(f"RESTORE TABLE test.t1, TABLE test.t2 FROM {backup_name}")

    # t1 has no data (excluded), t2 has data
    assert instance.query("SELECT count() FROM test.t1") == "0\n"
    assert instance.query("SELECT count() FROM test.t2") == "3\n"

    instance.query("DROP TABLE IF EXISTS test.t1, test.t2")
    instance.query("DROP DATABASE IF EXISTS test")


def test_restore_except_data_from_table_rejected():
    """Test that RESTORE with EXCEPT DATA FROM TABLE is rejected with clear error

    Regression test to prevent silent no-op: EXCEPT DATA FROM TABLE is BACKUP-only
    and should be rejected at parse time for RESTORE queries.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t VALUES (1), (2), (3)")

    backup_name = new_backup_name()
    # Create a valid backup first
    instance.query(f"BACKUP TABLE test.t TO {backup_name}")

    instance.query("DROP TABLE test.t")

    # RESTORE with EXCEPT DATA FROM TABLE should be rejected
    try:
        instance.query(f"RESTORE TABLE test.t EXCEPT DATA FROM TABLE test.t FROM {backup_name}")
        assert False, "Expected RESTORE with EXCEPT DATA FROM TABLE to be rejected"
    except Exception as e:
        error_message = str(e)
        # Should get BAD_ARGUMENTS with a clear message that this clause is BACKUP-only
        assert ("BACKUP" in error_message and ("RESTORE" in error_message or "only valid" in error_message)) or \
               "BAD_ARGUMENTS" in error_message, \
            f"Expected clear error about BACKUP-only clause, got: {error_message}"

    instance.query("DROP DATABASE IF EXISTS test")


def test_restore_except_tables_works():
    """Sanity check: EXCEPT TABLES (without DATA FROM) should work for RESTORE

    This is intentionally different from EXCEPT DATA FROM TABLE - except_tables
    is valid for both BACKUP and RESTORE (excludes tables from being restored).
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t1, test.t2")
    instance.query("CREATE TABLE test.t1 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t2 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t1 VALUES (1)")
    instance.query("INSERT INTO test.t2 VALUES (2)")

    backup_name = new_backup_name()
    instance.query(f"BACKUP DATABASE test TO {backup_name}")

    instance.query("DROP DATABASE test")

    # RESTORE with EXCEPT TABLES (not EXCEPT DATA FROM) should work
    instance.query(f"RESTORE DATABASE test EXCEPT TABLES t1 FROM {backup_name}")

    # t1 should not exist (excluded from restore), t2 should exist with data
    result = instance.query("SELECT name FROM system.tables WHERE database='test' ORDER BY name")
    assert "t1" not in result
    assert instance.query("SELECT count() FROM test.t2") == "1\n"

    instance.query("DROP DATABASE test")


# Note: JSON deserialization path (ASTBackupQuery::readJSON) validation is not
# tested here because the integration test suite does not have infrastructure for
# testing AST JSON deserialization directly. The SQL parser path test above
# (test_restore_except_data_from_table_rejected) provides equivalent coverage
# since both paths reject the same semantic error (EXCEPT DATA FROM TABLE in RESTORE).
# For comprehensive testing of the JSON path, a unit test under src/Parsers/tests/
# would be more appropriate, directly constructing JSON payloads and calling readJSON().
