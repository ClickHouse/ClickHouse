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
    # Both tables are backed up (structure); only t1's data is excluded. The clause is
    # written on the t1 element, because on a single-table element it can only name that
    # element's own table.
    instance.query(f"BACKUP TABLE test.t1 EXCEPT DATA FROM TABLE test.t1, TABLE test.t2 TO {backup_name}")

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


def test_except_data_from_table_rejects_other_table_in_same_element():
    """A single-table element's clause may only name that element's own table.

    Regression test for cross-element contamination: the clause used to be merged into a
    database-wide exclusion set, so `BACKUP TABLE test.a EXCEPT DATA FROM TABLE test.b`
    was accepted and could take the data away from a `test.b` element of the same query
    (or silently do nothing when `test.b` wasn't part of the backup at all).
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.a, test.b")
    instance.query("CREATE TABLE test.a (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.b (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.a VALUES (1)")
    instance.query("INSERT INTO test.b VALUES (2)")

    # The clause names a table which is in the backup, but as a different element.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE test.b, TABLE test.a EXCEPT DATA FROM TABLE test.b TO {new_backup_name()}"
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)

    # The clause names a table which is not part of the backup at all: this used to be
    # accepted and silently do nothing.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE test.a EXCEPT DATA FROM TABLE test.b TO {new_backup_name()}"
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)

    # Same for a list: every name in it must be the element's own table.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE test.a EXCEPT DATA FROM TABLES test.a, test.b TO {new_backup_name()}"
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)

    instance.query("DROP DATABASE test")


def test_except_data_from_table_no_cross_element_contamination():
    """The clause of one single-table element must not touch another element's data.

    This is the accepted form of the query rejected above: each element carries its own
    clause, so only the data of the element that carries it is excluded.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.a, test.b")
    instance.query("CREATE TABLE test.a (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.b (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.a VALUES (1), (2), (3)")
    instance.query("INSERT INTO test.b VALUES (4), (5)")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE test.b, TABLE test.a EXCEPT DATA FROM TABLE test.a TO {backup_name}"
    )

    instance.query("DROP TABLE test.a, test.b")
    instance.query(f"RESTORE TABLE test.a, TABLE test.b FROM {backup_name}")

    # Only test.a lost its data; test.b was backed up by an element with no clause.
    assert instance.query("SELECT count() FROM test.a") == "0\n"
    assert instance.query("SELECT count() FROM test.b") == "2\n"

    instance.query("DROP DATABASE test")


def test_except_data_explicit_table_element_keeps_its_data():
    """An element asking for a table keeps its data even if a wider element excludes it.

    `EXCEPT DATA FROM TABLES t1` on the DATABASE element must not take the data away from
    the `TABLE test.t1` element of the same query, the same way `EXCEPT TABLES` doesn't
    remove a table which another element names explicitly. Data the user asked for is
    never dropped silently.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t1, test.t2")
    instance.query("CREATE TABLE test.t1 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.t2 (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t1 VALUES (1), (2)")
    instance.query("INSERT INTO test.t2 VALUES (3)")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP DATABASE test EXCEPT DATA FROM TABLES t1, TABLE test.t1 TO {backup_name}"
    )

    instance.query("DROP DATABASE test")
    instance.query(f"RESTORE DATABASE test FROM {backup_name}")

    assert instance.query("SELECT count() FROM test.t1") == "2\n"
    assert instance.query("SELECT count() FROM test.t2") == "1\n"

    instance.query("DROP DATABASE test")


def test_except_data_database_element_scoped_to_its_own_database():
    """A DATABASE element's clause only affects the tables of that element's database."""
    instance.query("CREATE DATABASE IF NOT EXISTS db1")
    instance.query("CREATE DATABASE IF NOT EXISTS db2")
    instance.query("DROP TABLE IF EXISTS db1.t, db2.t")
    instance.query("CREATE TABLE db1.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE db2.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO db1.t VALUES (1)")
    instance.query("INSERT INTO db2.t VALUES (2)")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP DATABASE db1 EXCEPT DATA FROM TABLE t, DATABASE db2 TO {backup_name}"
    )

    instance.query("DROP DATABASE db1")
    instance.query("DROP DATABASE db2")
    instance.query(f"RESTORE DATABASE db1, DATABASE db2 FROM {backup_name}")

    assert instance.query("SELECT count() FROM db1.t") == "0\n"
    assert instance.query("SELECT count() FROM db2.t") == "1\n"

    instance.query("DROP DATABASE db1")
    instance.query("DROP DATABASE db2")


def test_except_data_temporary_table_rejects_database_qualified_name():
    """A temporary table has no database, so a qualified name in the clause is rejected.

    It used to be accepted and silently ignored, because the collector only looked at
    exclusions whose database name matched `_temporary_and_external_tables`.
    """
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TEMPORARY TABLE tmp EXCEPT DATA FROM TABLE test.tmp TO {new_backup_name()}"
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)


def test_except_data_from_table_qualified_name_on_unqualified_element():
    """The clause may state the database even when the element itself leaves it out.

    `BACKUP TABLE t EXCEPT DATA FROM TABLE test.t` names the same object twice when the
    current database is `test`, so it must be accepted. The parser cannot decide that on
    its own - the element's database comes from the current database, which it doesn't
    know - so it defers the comparison to `Element::setCurrentDatabase`. It used to
    reject the query outright instead.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t VALUES (1), (2), (3)")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE t EXCEPT DATA FROM TABLE test.t TO {backup_name}", database="test"
    )

    instance.query("DROP TABLE test.t")
    instance.query(f"RESTORE TABLE test.t FROM {backup_name}")

    # The structure came back, the data did not: the deferred comparison resolved to
    # "the clause names this element's own object".
    assert instance.query("SELECT count() FROM test.t") == "0\n"

    instance.query("DROP DATABASE test")


def test_except_data_from_table_qualified_name_mismatched_database_rejected():
    """A clause naming a different database is still rejected, wherever it is detected.

    The unqualified-element form can only be decided once the current database is known,
    so it is caught by `Element::setCurrentDatabase`; the qualified form is caught by the
    parser. Both must fail - accepting either would exclude the data of the element's own
    object while the user asked for another table's.
    """
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query("CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO test.t VALUES (1)")

    # Deferred: the element is unqualified, so only the current database (`test`) decides
    # that `other` is wrong. `other` is never looked up, so this is our error, not a
    # missing-database one.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE t EXCEPT DATA FROM TABLE other.t TO {new_backup_name()}",
            database="test",
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)

    # Immediate: both database names are known at parse time.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE test.t EXCEPT DATA FROM TABLE other.t TO {new_backup_name()}"
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)

    # Two different databases in one clause cannot both be the element's own object,
    # whatever it resolves to, so this is decided at parse time even when unqualified.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE t EXCEPT DATA FROM TABLES test.t, other.t TO {new_backup_name()}",
            database="test",
        )
    assert "own object" in str(exc_info.value), str(exc_info.value)

    instance.query("DROP DATABASE test")


def test_except_data_from_table_deferred_database_survives_formatting():
    """A clause database the parser could not yet check must survive formatting.

    `BACKUP ... ON CLUSTER` is formatted on the initiator while the elements are still
    unresolved and parsed again on every host, so a database name dropped here would turn
    a query that each host must reject into one that silently excludes its own data.
    """
    formatted = instance.query(
        "SELECT formatQuery($$BACKUP TABLE t EXCEPT DATA FROM TABLE other.t "
        "TO Disk('backups', 'fmt/')$$)"
    )
    assert "EXCEPT DATA FROM TABLE other.t" in formatted, formatted

    # When the element states its own database, that one is used (they are equal by then).
    formatted = instance.query(
        "SELECT formatQuery($$BACKUP TABLE test.t EXCEPT DATA FROM TABLE test.t "
        "TO Disk('backups', 'fmt/')$$)"
    )
    assert "EXCEPT DATA FROM TABLE test.t" in formatted, formatted


def test_except_data_tables_json_database_element_rejects_foreign_database():
    """`clickhouse_json` must enforce the DATABASE element's own invariant too.

    `parseExceptDataTables` makes every entry of a DATABASE element's clause name that
    element's database, filling in an omitted one. `readJSON` accepted any database, and
    `BackupEntriesCollector::gatherDatabaseMetadata` then dropped every entry whose
    database is not the one being gathered - so the exclusion silently did nothing.

    The JSON is derived from a valid query and then edited, so the test does not depend on
    the exact serialization envelope. `"database":"db1"` only ever appears inside the
    `except_data_tables` entry: the element itself uses `"database_name"`.
    """
    valid_json_sql = (
        "parseQueryToJSON($$BACKUP DATABASE db1 EXCEPT DATA FROM TABLE db1.t "
        "TO Disk('backups', 'json/')$$)"
    )

    # Sanity check: unedited, the round trip works and keeps the clause.
    formatted = instance.query(f"SELECT formatQueryFromJSON({valid_json_sql})")
    assert "EXCEPT DATA FROM TABLE t" in formatted, formatted

    # An entry naming another database is a no-op, so it must be rejected.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"SELECT formatQueryFromJSON(replaceAll({valid_json_sql}, "
            "'\"database\":\"db1\"', '\"database\":\"db2\"'))"
        )
    assert "does not belong to database" in str(exc_info.value), str(exc_info.value)

    # An entry naming no database matches no database at all - the same no-op.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"SELECT formatQueryFromJSON(replaceAll({valid_json_sql}, "
            "'\"database\":\"db1\"', '\"database\":\"\"'))"
        )
    assert "does not belong to database" in str(exc_info.value), str(exc_info.value)

    # An entry naming no table matches no table at all.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"SELECT formatQueryFromJSON(replaceAll({valid_json_sql}, "
            "'\"table\":\"t\"', '\"table\":\"\"'))"
        )
    assert "Empty table name" in str(exc_info.value), str(exc_info.value)


def test_except_data_json_table_element_rejects_inconsistent_clause_database():
    """`except_data_database` is the deferred clause database, and must stay consistent.

    It only exists while a single-object element's own database is unresolved, so it is
    meaningless without `except_data` and cannot disagree with a database the element does
    state - the two comparisons the parser and `setCurrentDatabase` make. `clickhouse_json`
    must not be able to build an element the parser would have refused.
    """
    valid_json_sql = (
        "parseQueryToJSON($$BACKUP TABLE db1.t EXCEPT DATA FROM TABLE db1.t "
        "TO Disk('backups', 'json/')$$)"
    )

    # Disagrees with the element's own database.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"SELECT formatQueryFromJSON(replaceAll({valid_json_sql}, "
            "'\"except_data\":true', "
            "'\"except_data\":true,\"except_data_database\":\"db2\"'))"
        )
    assert "except_data_database" in str(exc_info.value), str(exc_info.value)

    # Present without the clause it belongs to.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"SELECT formatQueryFromJSON(replaceAll({valid_json_sql}, "
            "'\"except_data\":true', '\"except_data_database\":\"db1\"'))"
        )
    assert "requires 'except_data'" in str(exc_info.value), str(exc_info.value)
