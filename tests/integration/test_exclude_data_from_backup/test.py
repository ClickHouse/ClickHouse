import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import PostgresManager, check_tables_are_synchronized

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    with_postgres=True,
)

pg_manager = PostgresManager()

backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


def backup_entries(backup_id):
    """Every entry recorded in the backup, the deduplicated ones included.

    Listing the files with `find` is not enough. With `deduplicate_files` on - the default - an entry
    whose content already appeared in the same backup is recorded as a reference and no second file is
    written, so a path can be absent from the file listing while the backup does hold it. `.backup`
    names every entry.
    """
    manifest = instance.exec_in_container(
        ["bash", "-c", f"cat /backups/{backup_id}/.backup"]
    )
    return re.findall(r"<name>(.*?)</name>", manifest)


def create_materialized_postgresql_table(pg_table, rows):
    """A standalone `MaterializedPostgreSQL` table in `default`, synchronized, and its nested table name.

    The table name is the caller's to keep short: the replication slot name PostgreSQL is asked for is
    derived from it and from the database name, and PostgreSQL caps a slot name at 63 characters.
    """
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    instance.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers({rows})"
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE default.{pg_table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
        ORDER BY key
        """
    )
    check_tables_are_synchronized(
        instance,
        pg_table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    nested_table = instance.query(
        "SELECT toString(uuid) || '_nested' FROM system.tables "
        f"WHERE database = 'default' AND name = '{pg_table}'"
    ).strip()
    assert nested_table.endswith("_nested"), nested_table
    return nested_table


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        pg_manager.init(
            instance,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
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
    # 2. INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION - our validation
    try:
        # Try with backticks to bypass parser's identifier check
        instance.query(f"BACKUP DATABASE test EXCEPT DATA FROM TABLE `{inner_table}` TO {backup_name}")
        assert False, "Expected exception when using inner table name, but query succeeded"
    except Exception as e:
        error_message = str(e)
        # Backtick-quoting bypasses the parser, so this must be rejected by our
        # explicit validation layer specifically - not by parser SYNTAX_ERROR.
        assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in error_message, \
            f"Expected INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION, got: {error_message}"

    instance.query("DROP DATABASE test")


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


def test_except_data_from_system_users_keeps_entry_without_entities():
    """`EXCEPT DATA FROM TABLE system.users` keeps the table entry but restores no users.

    `system.users` is put in a backup as access entities rather than as table data, so the
    clause suppresses exactly those entities instead of doing nothing. The control arm - the
    same backup without the clause - restores the user, which is what makes this a regression
    guard rather than a test that would also pass if the backup were simply broken.
    """
    instance.query("DROP USER IF EXISTS u_except_data")
    instance.query("CREATE USER u_except_data")

    excluded_backup = new_backup_name()
    instance.query(
        f"BACKUP TABLE system.users EXCEPT DATA FROM TABLE system.users TO {excluded_backup}"
    )

    control_backup = new_backup_name()
    instance.query(f"BACKUP TABLE system.users TO {control_backup}")

    instance.query("DROP USER u_except_data")

    # The element is in the backup, so the RESTORE itself succeeds ...
    instance.query(f"RESTORE TABLE system.users FROM {excluded_backup}")
    # ... but it carries no entities, so the user does not come back.
    assert (
        instance.query("SELECT count() FROM system.users WHERE name = 'u_except_data'")
        == "0\n"
    )

    # Control: without the clause, the same RESTORE does bring the user back.
    instance.query(f"RESTORE TABLE system.users FROM {control_backup}")
    assert (
        instance.query("SELECT count() FROM system.users WHERE name = 'u_except_data'")
        == "1\n"
    )

    instance.query("DROP USER IF EXISTS u_except_data")


def test_except_data_from_materialized_postgresql_nested_table():
    """A standalone MaterializedPostgreSQL table keeps its rows in a `<uuid>_nested` table.

    That nested table is internal, so it must not be reachable as a table of its own: naming it in
    the clause is rejected, and it is never enumerated as a separate element of a database backup.
    Its data reaches the backup only through the outer table, which is what makes
    `EXCEPT DATA FROM TABLE <outer>` able to suppress it.

    Before the fix `BackupUtils::isInnerTable` recognised only the `.inner_id.*` families, so the
    nested table was collected on its own and its rows were written to the backup even when the
    outer table had been excluded.
    """
    pg_table = "mpg_except_data"

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    instance.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers(1000)"
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE default.{pg_table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
        ORDER BY key
        """
    )
    check_tables_are_synchronized(
        instance,
        pg_table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    nested_table = instance.query(
        "SELECT toString(uuid) || '_nested' FROM system.tables "
        f"WHERE database = 'default' AND name = '{pg_table}'"
    ).strip()
    assert nested_table.endswith("_nested"), nested_table

    def backup_files(backup_id):
        listing = instance.exec_in_container(
            [
                "bash",
                "-c",
                f"find /backups/{backup_id} -type f | sed 's|/backups/{backup_id}/||' | sort",
            ]
        )
        return [line for line in listing.splitlines() if line]

    # 1. Control: without the clause the rows are in the backup, under the OUTER table's path.
    instance.query("BACKUP DATABASE default TO Disk('backups', 'mpg_control/')")
    control = backup_files("mpg_control")
    # The nested table must not appear as an element of its own. Backup paths escape the table
    # name (`-` becomes `%2D`), so match the `_nested` marker rather than the raw UUID name.
    assert not any(
        path.startswith("data/default/") and "_nested/" in path for path in control
    ), f"nested table backed up as its own element: {control}"
    assert any(
        path.startswith(f"data/default/{pg_table}/") for path in control
    ), f"outer table has no data in the control backup: {control}"

    # 2. Excluding the outer table suppresses the nested rows too.
    instance.query(
        f"BACKUP DATABASE default EXCEPT DATA FROM TABLE {pg_table} "
        "TO Disk('backups', 'mpg_excluded/')"
    )
    excluded = backup_files("mpg_excluded")
    assert not any(
        path.startswith("data/default/") and "_nested/" in path for path in excluded
    ), f"nested table backed up as its own element: {excluded}"
    assert not any(
        path.startswith(f"data/default/{pg_table}/") for path in excluded
    ), f"outer table data was written despite EXCEPT DATA FROM TABLE: {excluded}"

    # 3. Naming the nested table directly is rejected - it is an inner table.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP DATABASE default EXCEPT DATA FROM TABLE `{nested_table}` "
            "TO Disk('backups', 'mpg_rejected/')"
        )
    assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in str(exc_info.value), str(
        exc_info.value
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")


def test_except_data_from_materialized_postgresql_restore_into_replacing_mergetree():
    """`EXCEPT DATA FROM TABLE` on a standalone `MaterializedPostgreSQL` table, carried through `RESTORE`.

    `test_except_data_from_materialized_postgresql_nested_table` only inspects the files inside the backup.
    This test takes the same exclusion all the way through a restore, because "the definition is backed up,
    so the table restores empty" does not hold for this engine. `RESTORE` refuses to recreate a
    `MaterializedPostgreSQL` table at all: it would start replicating from the live PostgreSQL source as soon
    as it was created, mixing the backup snapshot with the current remote state, so
    `registerStorageMaterializedPostgreSQL` throws `NOT_IMPLEMENTED` before the table exists. That rejection
    is a property of the engine - an ordinary full backup hits it just the same - and is what
    `docs/concepts/features/backup-restore/overview.mdx` documents, rather than promising an empty restored
    table for every engine.

    The supported flow is a restore under a different name into a `ReplacingMergeTree` created beforehand
    with the structure of the nested table (including the `_sign` and `_version` columns). Both halves are
    pinned here: with the clause that target stays empty, and without it the same restore brings the rows
    across - so the empty result cannot be passing for some unrelated reason.
    """
    pg_table = "mpg_except_data_restore"
    rows = 50

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    instance.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers({rows})"
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE default.{pg_table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
        ORDER BY key
        """
    )
    check_tables_are_synchronized(
        instance,
        pg_table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    control_backup = new_backup_name()
    excluded_backup = new_backup_name()
    instance.query(f"BACKUP TABLE default.{pg_table} TO {control_backup}")
    instance.query(
        f"BACKUP TABLE default.{pg_table} EXCEPT DATA FROM TABLE default.{pg_table} "
        f"TO {excluded_backup}"
    )

    def make_target(name):
        instance.query(f"DROP TABLE IF EXISTS default.{name} SYNC")
        instance.query(
            f"""
            CREATE TABLE default.{name}
            (
                key Int32,
                value Int32,
                _sign Int8 MATERIALIZED 1,
                _version UInt64 MATERIALIZED 1
            )
            ENGINE = ReplacingMergeTree(_version) ORDER BY key
            """
        )

    # 1. Control: the plain backup restores the rows into a pre-created ReplacingMergeTree.
    # `allow_different_table_def` is required because the definition in the backup is the
    # MaterializedPostgreSQL engine, which is deliberately restored into a plain ReplacingMergeTree.
    make_target("mpg_restored_control")
    instance.query(
        f"RESTORE TABLE default.{pg_table} AS default.mpg_restored_control "
        f"FROM {control_backup} SETTINGS allow_different_table_def = 1"
    )
    assert rows == int(
        instance.query("SELECT count() FROM default.mpg_restored_control")
    ), "the supported restore path must bring the rows across without the clause"
    assert "1225" == instance.query(
        "SELECT sum(key) FROM default.mpg_restored_control"
    ).strip()

    # 2. With the clause the very same restore succeeds and leaves the target empty.
    make_target("mpg_restored_excluded")
    instance.query(
        f"RESTORE TABLE default.{pg_table} AS default.mpg_restored_excluded "
        f"FROM {excluded_backup} SETTINGS allow_different_table_def = 1"
    )
    assert 0 == int(
        instance.query("SELECT count() FROM default.mpg_restored_excluded")
    ), "EXCEPT DATA FROM TABLE must leave the restored target empty"

    # 3. Restoring the table in place is rejected either way - this is the claim the docs no longer make.
    # The target has to be dropped first, otherwise RESTORE fails because the table already exists.
    instance.query(f"DROP TABLE default.{pg_table} SYNC")
    error = instance.query_and_get_error(
        f"RESTORE TABLE default.{pg_table} FROM {excluded_backup}",
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert "from a backup is not supported" in error, error
    assert "MaterializedPostgreSQL" in error, error
    assert "0" == instance.query(f"EXISTS TABLE default.{pg_table}").strip(), (
        "the rejected restore must fail closed before creating the table, leaving no live "
        "MaterializedPostgreSQL table replicating from the live PostgreSQL source"
    )

    instance.query("DROP TABLE IF EXISTS default.mpg_restored_control SYNC")
    instance.query("DROP TABLE IF EXISTS default.mpg_restored_excluded SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")


def test_materialized_postgresql_nested_table_named_by_its_own_element_is_backed_up():
    """An element naming the nested table itself asks for it, and that request wins.

    The nested table stays out of a `DATABASE` or `ALL` backup because nothing named it, and
    `BackupUtils::findInnerTables` recognises it from that same enumeration - which contains the outer
    table, and so can tell which table owns a nested table of that name. A single-table element naming
    only the nested table enumerates only that table, so there is no outer table in it to recognise the
    name by, and the element named it explicitly in any case.

    That is the same rule the rest of the clause follows: a table named by an element of its own is
    backed up even when a wider element excludes it. It also matches `master`, where the nested table
    has never been hidden from an element naming it, and an `Ordinary` database, whose tables have no
    UUID for the name to be derived from at all.
    """
    pg_table = "mpg_named_directly"

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    instance.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers(30)"
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE default.{pg_table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
        ORDER BY key
        """
    )
    check_tables_are_synchronized(
        instance,
        pg_table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    nested_table = instance.query(
        "SELECT toString(uuid) || '_nested' FROM system.tables "
        f"WHERE database = 'default' AND name = '{pg_table}'"
    ).strip()
    assert nested_table.endswith("_nested"), nested_table

    def backup_files(backup_id):
        listing = instance.exec_in_container(
            [
                "bash",
                "-c",
                f"find /backups/{backup_id} -type f | sed 's|/backups/{backup_id}/||' | sort",
            ]
        )
        return [line for line in listing.splitlines() if line]

    # 1. Nothing names it: a DATABASE element leaves the nested table out entirely.
    instance.query("BACKUP DATABASE default TO Disk('backups', 'mpg_named_db/')")
    from_database = backup_files("mpg_named_db")
    assert not any(
        "_nested" in path for path in from_database
    ), f"nested table reached a DATABASE backup: {from_database}"

    # 2. An element of its own names it, so it is backed up like any other table.
    instance.query(
        f"BACKUP TABLE default.`{nested_table}` TO Disk('backups', 'mpg_named_table/')"
    )
    named_directly = backup_files("mpg_named_table")
    assert any(
        path.startswith("metadata/default/") and "_nested" in path
        for path in named_directly
    ), f"the element naming the nested table did not back it up: {named_directly}"
    assert any(
        path.startswith("data/default/") and "_nested" in path
        for path in named_directly
    ), f"the nested table was backed up without its data: {named_directly}"

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")


def test_nested_table_named_by_its_own_element_beside_a_database_element_is_backed_up():
    """A wider element in the same query must not decide the answer for the element beside it.

    The trace from the review:

        BACKUP DATABASE default, TABLE default.`<uuid>_nested` TO ...

    A nested table is recognised through the outer table whose UUID its name is derived from, and the
    `DATABASE` element is what brings that outer table into the enumeration. So adding the `DATABASE`
    element is what made the nested table recognisable, and the table the user had named by hand was
    dropped as an inner table and then reported as `UNKNOWN_TABLE` - the element naming it lost to an
    element that had not named it at all.

    A table named by an element of its own is backed up even when a wider element would drop it. That
    is the rule `EXCEPT TABLES` already follows, and the invariant
    `test_materialized_postgresql_nested_table_named_by_its_own_element_is_backed_up` pins down for
    the single-element form; this is the same invariant with a `DATABASE` element beside it.
    """
    pg_table = "mpg_beside_db"

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    instance.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers(30)"
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE default.{pg_table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
        ORDER BY key
        """
    )
    check_tables_are_synchronized(
        instance,
        pg_table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    nested_table = instance.query(
        "SELECT toString(uuid) || '_nested' FROM system.tables "
        f"WHERE database = 'default' AND name = '{pg_table}'"
    ).strip()
    assert nested_table.endswith("_nested"), nested_table

    # 1. Control: the `DATABASE` element on its own still hides the nested table, so the difference
    #    below is the second element and nothing else.
    instance.query("BACKUP DATABASE default TO Disk('backups', 'mpg_beside_db_control/')")
    control = backup_entries("mpg_beside_db_control")
    assert not any(
        "_nested" in path for path in control
    ), f"nested table reached a DATABASE backup: {control}"

    # 2. The reported trace. This used to fail with `UNKNOWN_TABLE`.
    instance.query(
        f"BACKUP DATABASE default, TABLE default.`{nested_table}` "
        "TO Disk('backups', 'mpg_beside_db/')"
    )
    beside = backup_entries("mpg_beside_db")
    assert any(
        path.startswith("metadata/default/") and "_nested" in path for path in beside
    ), f"the element naming the nested table did not back it up: {beside}"
    assert any(
        path.startswith("data/default/") and "_nested" in path for path in beside
    ), f"the nested table was backed up without its data: {beside}"
    # The outer table is still backed up by the `DATABASE` element, rows and all.
    assert any(
        path.startswith(f"data/default/{pg_table}/") for path in beside
    ), f"outer table has no data: {beside}"

    # 3. Keeping the table does not make the clause legal on it: its data is still excluded through
    #    the outer table, so naming it here is still refused.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP DATABASE default, TABLE default.`{nested_table}` "
            f"EXCEPT DATA FROM TABLE default.`{nested_table}` TO {new_backup_name()}"
        )
    assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in str(exc_info.value), str(
        exc_info.value
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")


def test_except_data_on_a_nested_table_named_by_its_own_element_is_rejected():
    """Naming an inner table in the clause is rejected whatever else the query names.

    The trace from the review:

        BACKUP TABLE default.`<uuid>_nested` EXCEPT DATA FROM TABLE default.`<uuid>_nested` TO ...

    Before the fix the rejection was derived from the enumeration of the database, which holds only the
    tables the query selects. That enumeration contains the outer table only when some element asks for
    it, so the very same clause on the very same table was rejected in

        BACKUP TABLE default.pg_table, TABLE default.`<uuid>_nested` EXCEPT DATA FROM TABLE ...

    and accepted without the first element - the hidden table went into the backup with its definition
    and no rows instead. The clause names one table, so the answer must be about that table alone: it
    now comes from the live catalog, which knows the outer table whether or not the query mentions it.

    `test_materialized_postgresql_nested_table_named_by_its_own_element_is_backed_up` is the neighbour
    of this: an element naming the nested table *without* the clause still backs it up, as on `master`.
    Only the clause is rejected, and only because its data is excluded through the outer table.
    """
    # Short on purpose: the replication slot name PostgreSQL is asked for is derived from this and
    # from the database name, and PostgreSQL caps a slot name at 63 characters.
    pg_table = "mpg_except_named"

    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
    pg_manager.create_postgres_table(pg_table)
    instance.query(
        f"INSERT INTO postgres_database.{pg_table} SELECT number, number FROM numbers(30)"
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    instance.query(
        f"""
        SET allow_experimental_materialized_postgresql_table=1;
        CREATE TABLE default.{pg_table} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{cluster.postgres_ip}:{cluster.postgres_port}', 'postgres_database', '{pg_table}', 'postgres', '{pg_pass}')
        ORDER BY key
        """
    )
    check_tables_are_synchronized(
        instance,
        pg_table,
        postgres_database=pg_manager.get_default_database(),
        materialized_database="default",
    )

    nested_table = instance.query(
        "SELECT toString(uuid) || '_nested' FROM system.tables "
        f"WHERE database = 'default' AND name = '{pg_table}'"
    ).strip()
    assert nested_table.endswith("_nested"), nested_table

    # 1. The reported trace: the nested table is the only table the query names.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE default.`{nested_table}` "
            f"EXCEPT DATA FROM TABLE default.`{nested_table}` TO {new_backup_name()}"
        )
    assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in str(exc_info.value), str(
        exc_info.value
    )

    # 2. The same clause with the outer table named too. This arm was already rejected before the fix,
    #    and it is here to pin down that the two now answer alike: whether another element happens to
    #    name the outer table is not what decides.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE default.{pg_table}, TABLE default.`{nested_table}` "
            f"EXCEPT DATA FROM TABLE default.`{nested_table}` TO {new_backup_name()}"
        )
    assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in str(exc_info.value), str(
        exc_info.value
    )

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")

    # 3. Negative control. The rejection has to come from the outer table the name points at, not from
    #    the shape of the name: an ordinary table called `<uuid>_nested` that owns nothing takes the
    #    clause in the very same single-table form, and comes back empty.
    #
    #    The UUID is one no table carries, so the lookup that rejected the arms above finds nothing.
    nested_like = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0_nested"

    instance.query("DROP DATABASE IF EXISTS nested_name_single_db")
    instance.query("CREATE DATABASE nested_name_single_db")
    instance.query(
        f"CREATE TABLE nested_name_single_db.`{nested_like}` (id UInt64) "
        "ENGINE = MergeTree ORDER BY id"
    )
    instance.query(f"INSERT INTO nested_name_single_db.`{nested_like}` VALUES (1), (2), (3)")

    control_backup = new_backup_name()
    instance.query(
        f"BACKUP TABLE nested_name_single_db.`{nested_like}` "
        f"EXCEPT DATA FROM TABLE nested_name_single_db.`{nested_like}` TO {control_backup}"
    )
    instance.query(f"DROP TABLE nested_name_single_db.`{nested_like}`")
    instance.query(
        f"RESTORE TABLE nested_name_single_db.`{nested_like}` FROM {control_backup}"
    )
    assert (
        instance.query(f"SELECT count() FROM nested_name_single_db.`{nested_like}`")
        == "0\n"
    )

    instance.query("DROP DATABASE nested_name_single_db")


def test_except_data_overlapping_table_and_database_elements_keep_data():
    """A table's data survives when another element of the same query asks for the table itself.

    `BACKUP TABLE db.t EXCEPT DATA FROM TABLE db.t, DATABASE db` names `db.t` twice: once with the
    clause and once, through `DATABASE db`, without it. The element that did not write the clause is
    asking for the data, and that request has to win - writing data the user meant to drop is a much
    smaller harm than silently dropping data they asked for.

    Before the fix `isTableDataExcluded` returned the single-table element's answer and never looked
    at the `DATABASE` element, so the data was dropped.
    """
    instance.query("DROP DATABASE IF EXISTS overlap_db")
    instance.query("CREATE DATABASE overlap_db")
    instance.query("CREATE TABLE overlap_db.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO overlap_db.t VALUES (1), (2), (3)")

    # Control: the same clause with no second element really does drop the data. Without this arm
    # the assertion below would also pass if the clause had stopped working altogether.
    control_backup = new_backup_name()
    instance.query(
        f"BACKUP TABLE overlap_db.t EXCEPT DATA FROM TABLE overlap_db.t TO {control_backup}"
    )

    overlapping_backup = new_backup_name()
    instance.query(
        f"BACKUP TABLE overlap_db.t EXCEPT DATA FROM TABLE overlap_db.t, DATABASE overlap_db "
        f"TO {overlapping_backup}"
    )

    instance.query("DROP TABLE overlap_db.t")
    instance.query(f"RESTORE TABLE overlap_db.t FROM {control_backup}")
    assert instance.query("SELECT count() FROM overlap_db.t") == "0\n"

    instance.query("DROP TABLE overlap_db.t")
    instance.query(f"RESTORE TABLE overlap_db.t FROM {overlapping_backup}")
    assert instance.query("SELECT count() FROM overlap_db.t") == "3\n"

    instance.query("DROP DATABASE overlap_db")


def test_except_data_overlapping_database_and_all_elements_keep_data():
    """The same rule across a `DATABASE` element and an `ALL` element.

    `BACKUP DATABASE db, ALL EXCEPT DATA FROM TABLE db.t` writes the clause on the `ALL` element
    only. `DATABASE db` selects `db.t` without excluding its data, so the data stays.
    """
    instance.query("DROP DATABASE IF EXISTS overlap_db")
    instance.query("CREATE DATABASE overlap_db")
    instance.query("CREATE TABLE overlap_db.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO overlap_db.t VALUES (1), (2), (3)")

    # Control: the `ALL` element on its own drops the data, so the difference below is the second
    # element and nothing else. `system` is excluded to avoid system table restore conflicts.
    control_backup = new_backup_name()
    instance.query(
        f"BACKUP ALL EXCEPT DATABASE system EXCEPT DATA FROM TABLE overlap_db.t TO {control_backup}"
    )

    overlapping_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE overlap_db, ALL EXCEPT DATABASE system "
        f"EXCEPT DATA FROM TABLE overlap_db.t TO {overlapping_backup}"
    )

    instance.query("DROP DATABASE overlap_db")
    instance.query(f"RESTORE DATABASE overlap_db FROM {control_backup}")
    assert instance.query("SELECT count() FROM overlap_db.t") == "0\n"

    instance.query("DROP DATABASE overlap_db")
    instance.query(f"RESTORE DATABASE overlap_db FROM {overlapping_backup}")
    assert instance.query("SELECT count() FROM overlap_db.t") == "3\n"

    instance.query("DROP DATABASE overlap_db")


def test_except_data_excluded_by_every_element_is_still_excluded():
    """The fail-safe rule must not turn into "never exclude anything".

    Two cases where no element asks for the data, so it still has to be dropped: every element that
    selects the table excludes its data, and an element that excludes the table itself - which
    therefore expresses no wish about its data at all.
    """
    instance.query("DROP DATABASE IF EXISTS overlap_db")
    instance.query("CREATE DATABASE overlap_db")
    instance.query("CREATE TABLE overlap_db.t (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO overlap_db.t VALUES (1), (2), (3)")

    # Both elements select the table and both exclude its data.
    both_exclude = new_backup_name()
    instance.query(
        f"BACKUP DATABASE overlap_db EXCEPT DATA FROM TABLE overlap_db.t, "
        f"TABLE overlap_db.t EXCEPT DATA FROM TABLE overlap_db.t TO {both_exclude}"
    )

    # The `DATABASE` element does not select `t` at all, so it is not an element asking for its
    # data. It is written last because `EXCEPT TABLES t, TABLE ...` does not parse - the name list
    # swallows the comma before the next element, which is how `EXCEPT TABLES` behaves on master
    # too and is unrelated to this fix.
    except_tables = new_backup_name()
    instance.query(
        f"BACKUP TABLE overlap_db.t EXCEPT DATA FROM TABLE overlap_db.t, "
        f"DATABASE overlap_db EXCEPT TABLES t TO {except_tables}"
    )

    instance.query("DROP TABLE overlap_db.t")
    instance.query(f"RESTORE TABLE overlap_db.t FROM {both_exclude}")
    assert instance.query("SELECT count() FROM overlap_db.t") == "0\n"

    instance.query("DROP TABLE overlap_db.t")
    instance.query(f"RESTORE TABLE overlap_db.t FROM {except_tables}")
    assert instance.query("SELECT count() FROM overlap_db.t") == "0\n"

    instance.query("DROP DATABASE overlap_db")


def test_backup_ordinary_table_whose_name_looks_like_a_nested_table():
    """`<uuid>_nested` is not a reserved name, so an ordinary table may carry it.

    A standalone MaterializedPostgreSQL table names its nested table `<uuid of the outer table>_nested`,
    but nothing stops a user from creating a table of that shape. Recognising an inner table by the
    shape of its name alone therefore made ordinary tables vanish: `BACKUP DATABASE` skipped them,
    `BACKUP TABLE` could not find them, naming one in the clause was rejected, and `RESTORE ... AS`
    dropped the restored table on the floor. The outer table the name points at is what decides, and
    here there is none.

    `test_except_data_from_materialized_postgresql_nested_table` is the other half of this: a real
    nested table, whose outer table does exist, still has to be treated as inner.
    """
    nested_like = "01234567-89ab-cdef-0123-456789abcdef_nested"
    alias_like = "fedcba98-7654-3210-fedc-ba9876543210_nested"

    instance.query("DROP DATABASE IF EXISTS nested_name_db")
    instance.query("CREATE DATABASE nested_name_db")
    instance.query(
        f"CREATE TABLE nested_name_db.`{nested_like}` (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(f"INSERT INTO nested_name_db.`{nested_like}` VALUES (1), (2), (3)")
    instance.query("CREATE TABLE nested_name_db.plain (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("INSERT INTO nested_name_db.plain VALUES (1), (2)")

    # 1. A database backup enumerates it like any other table.
    database_backup = new_backup_name()
    instance.query(f"BACKUP DATABASE nested_name_db TO {database_backup}")
    instance.query("DROP DATABASE nested_name_db")
    instance.query(f"RESTORE DATABASE nested_name_db FROM {database_backup}")
    assert instance.query(f"SELECT count() FROM nested_name_db.`{nested_like}`") == "3\n"

    # 2. It can be named by a TABLE element, which used to fail with UNKNOWN_TABLE.
    table_backup = new_backup_name()
    instance.query(f"BACKUP TABLE nested_name_db.`{nested_like}` TO {table_backup}")
    instance.query(f"DROP TABLE nested_name_db.`{nested_like}`")
    instance.query(f"RESTORE TABLE nested_name_db.`{nested_like}` FROM {table_backup}")
    assert instance.query(f"SELECT count() FROM nested_name_db.`{nested_like}`") == "3\n"

    # 3. It can be named by EXCEPT DATA FROM TABLE: it is an ordinary table, so the clause applies
    #    to it instead of being rejected as an inner table name.
    excluded_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE nested_name_db EXCEPT DATA FROM TABLE `{nested_like}` TO {excluded_backup}"
    )
    instance.query("DROP DATABASE nested_name_db")
    instance.query(f"RESTORE DATABASE nested_name_db FROM {excluded_backup}")
    assert instance.query(f"SELECT count() FROM nested_name_db.`{nested_like}`") == "0\n"
    assert instance.query("SELECT count() FROM nested_name_db.plain") == "2\n"

    # 4. A restore alias of that shape is restored, not silently skipped. The RESTORE path checks
    #    the name *after* renaming, so this arm is the one that reaches it.
    alias_backup = new_backup_name()
    instance.query(f"BACKUP TABLE nested_name_db.plain TO {alias_backup}")
    instance.query(
        f"RESTORE TABLE nested_name_db.plain AS nested_name_db.`{alias_like}` FROM {alias_backup}"
    )
    assert instance.query(f"SELECT count() FROM nested_name_db.`{alias_like}`") == "2\n"

    instance.query("DROP DATABASE nested_name_db")


def test_except_tables_overlapping_database_and_all_elements_keep_table():
    """`EXCEPT TABLES` written on one element must not drop a table another element selects.

    The trace from the review:

        BACKUP DATABASE db EXCEPT DATA FROM TABLE db.t, ALL EXCEPT TABLES db.t

    `DATABASE db` selects `db.t` and asks only for its data to be left out, so `db.t` has to reach
    the backup empty. Before the fix the two elements' `EXCEPT TABLES` names were merged into one
    database-wide set, so `findTablesInDatabase` rejected `t` outright and neither its DDL nor its
    data was written.
    """
    instance.query("DROP DATABASE IF EXISTS except_tables_db")
    instance.query("CREATE DATABASE except_tables_db")
    instance.query(
        "CREATE TABLE except_tables_db.t (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_db.t VALUES (1), (2), (3)")

    # Control: the `ALL` element on its own really does drop the table, so the difference below is
    # the first element and nothing else.
    control_backup = new_backup_name()
    instance.query(
        f"BACKUP ALL EXCEPT DATABASE system EXCEPT TABLES except_tables_db.t TO {control_backup}"
    )

    overlapping_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_db EXCEPT DATA FROM TABLE except_tables_db.t, "
        f"ALL EXCEPT DATABASE system EXCEPT TABLES except_tables_db.t TO {overlapping_backup}"
    )

    instance.query("DROP DATABASE except_tables_db")
    instance.query(f"RESTORE DATABASE except_tables_db FROM {control_backup}")
    assert (
        instance.query(
            "SELECT count() FROM system.tables WHERE database = 'except_tables_db' AND name = 't'"
        )
        == "0\n"
    )

    instance.query("DROP DATABASE except_tables_db")
    instance.query(f"RESTORE DATABASE except_tables_db FROM {overlapping_backup}")
    # The DDL is back and the data is not: exactly what the first element asked for.
    assert (
        instance.query(
            "SELECT count() FROM system.tables WHERE database = 'except_tables_db' AND name = 't'"
        )
        == "1\n"
    )
    assert instance.query("SELECT count() FROM except_tables_db.t") == "0\n"

    instance.query("DROP DATABASE except_tables_db")


def test_except_tables_element_asking_for_the_whole_table_wins():
    """An element that wants the table outright beats another element's `EXCEPT TABLES`.

    `BACKUP DATABASE db, ALL EXCEPT TABLES db.t` - the first element asks for `db.t` with its data,
    so both the DDL and the rows have to survive. This is the same rule as the test above with the
    data left in, and it is the case that shows the merged set is gone rather than merely narrowed.
    """
    instance.query("DROP DATABASE IF EXISTS except_tables_whole_db")
    instance.query("CREATE DATABASE except_tables_whole_db")
    instance.query(
        "CREATE TABLE except_tables_whole_db.t (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_whole_db.t VALUES (1), (2), (3)")

    backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_whole_db, "
        f"ALL EXCEPT DATABASE system EXCEPT TABLES except_tables_whole_db.t TO {backup}"
    )

    instance.query("DROP DATABASE except_tables_whole_db")
    instance.query(f"RESTORE DATABASE except_tables_whole_db FROM {backup}")
    assert instance.query("SELECT count() FROM except_tables_whole_db.t") == "3\n"

    instance.query("DROP DATABASE except_tables_whole_db")


def test_except_tables_excluded_by_every_element_is_still_excluded():
    """The fail-safe rule must not turn into "never exclude anything".

    When no element asks for the table it still has to be dropped, DDL included. Without this the
    two tests above would also be satisfied by an `EXCEPT TABLES` clause that excluded nothing.
    """
    instance.query("DROP DATABASE IF EXISTS except_tables_guard_db")
    instance.query("CREATE DATABASE except_tables_guard_db")
    instance.query(
        "CREATE TABLE except_tables_guard_db.t (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_guard_db.t VALUES (1), (2), (3)")
    instance.query(
        "CREATE TABLE except_tables_guard_db.other (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_guard_db.other VALUES (1), (2)")

    backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_guard_db EXCEPT TABLES except_tables_guard_db.t TO {backup}"
    )

    instance.query("DROP DATABASE except_tables_guard_db")
    instance.query(f"RESTORE DATABASE except_tables_guard_db FROM {backup}")
    assert (
        instance.query(
            "SELECT count() FROM system.tables WHERE database = 'except_tables_guard_db' AND name = 't'"
        )
        == "0\n"
    )
    # The sibling table proves the element itself worked rather than the whole backup being empty.
    assert instance.query("SELECT count() FROM except_tables_guard_db.other") == "2\n"

    instance.query("DROP DATABASE except_tables_guard_db")


def test_except_tables_three_overlapping_elements():
    """Three elements covering the same table, not two.

    The first two exclude only the data, the third excludes the table itself. The table is selected
    (twice) and no element asks for its data, so the DDL survives and the rows do not. A second arm
    swaps the middle element for one that asks for the table outright, which flips the data back on
    and shows the outcome tracks the elements rather than their number or order in the list.
    """
    instance.query("DROP DATABASE IF EXISTS except_tables_three_db")
    instance.query("CREATE DATABASE except_tables_three_db")
    instance.query(
        "CREATE TABLE except_tables_three_db.t (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_three_db.t VALUES (1), (2), (3)")

    data_excluded_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_three_db EXCEPT DATA FROM TABLE except_tables_three_db.t, "
        f"DATABASE except_tables_three_db EXCEPT DATA FROM TABLE except_tables_three_db.t, "
        f"ALL EXCEPT DATABASE system EXCEPT TABLES except_tables_three_db.t "
        f"TO {data_excluded_backup}"
    )

    data_wanted_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_three_db EXCEPT DATA FROM TABLE except_tables_three_db.t, "
        f"DATABASE except_tables_three_db, "
        f"ALL EXCEPT DATABASE system EXCEPT TABLES except_tables_three_db.t "
        f"TO {data_wanted_backup}"
    )

    instance.query("DROP DATABASE except_tables_three_db")
    instance.query(f"RESTORE DATABASE except_tables_three_db FROM {data_excluded_backup}")
    assert instance.query("SELECT count() FROM except_tables_three_db.t") == "0\n"

    instance.query("DROP DATABASE except_tables_three_db")
    instance.query(f"RESTORE DATABASE except_tables_three_db FROM {data_wanted_backup}")
    assert instance.query("SELECT count() FROM except_tables_three_db.t") == "3\n"

    instance.query("DROP DATABASE except_tables_three_db")


def test_except_tables_and_except_data_naming_the_same_table_on_one_element():
    """One element naming the same table in both `EXCEPT TABLES` and `EXCEPT DATA FROM TABLE`.

    The parser accepts this ordering (the reverse, `EXCEPT DATA FROM TABLE` before
    `EXCEPT TABLES`, is a syntax error), so it is a case a user can write and the two clauses
    disagree about the same table on the same element.

    `EXCEPT TABLES` wins, because it settles a question the other clause never reaches: the element
    does not select the table at all, so it has no data for the element to have an opinion about.
    That is not the "least exclusion wins" rule being broken - that rule arbitrates *between*
    elements, and here there is one element whose two clauses are about different things.

    The second arm is the same wide element with another element that does want the table. The
    wide element's clauses are scoped to itself, so its `EXCEPT DATA FROM TABLE` must not reach the
    table the other element asked for - only the first arm can be satisfied by a predicate that
    excludes too much, and only the second by one that excludes too little.
    """
    instance.query("DROP DATABASE IF EXISTS except_tables_mixed_db")
    instance.query("CREATE DATABASE except_tables_mixed_db")
    instance.query(
        "CREATE TABLE except_tables_mixed_db.t (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_mixed_db.t VALUES (1), (2), (3)")
    instance.query(
        "CREATE TABLE except_tables_mixed_db.other (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO except_tables_mixed_db.other VALUES (1), (2)")

    both_clauses_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_mixed_db "
        f"EXCEPT TABLES except_tables_mixed_db.t "
        f"EXCEPT DATA FROM TABLE except_tables_mixed_db.t TO {both_clauses_backup}"
    )

    other_element_wants_it_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_mixed_db, "
        f"ALL EXCEPT DATABASE system "
        f"EXCEPT TABLES except_tables_mixed_db.t "
        f"EXCEPT DATA FROM TABLE except_tables_mixed_db.t "
        f"TO {other_element_wants_it_backup}"
    )

    instance.query("DROP DATABASE except_tables_mixed_db")
    instance.query(f"RESTORE DATABASE except_tables_mixed_db FROM {both_clauses_backup}")
    # `EXCEPT TABLES` settled it: the table is not in the backup at all, DDL included.
    assert (
        instance.query(
            "SELECT count() FROM system.tables "
            "WHERE database = 'except_tables_mixed_db' AND name = 't'"
        )
        == "0\n"
    )
    # The sibling table pins that the element worked rather than the backup being empty.
    assert instance.query("SELECT count() FROM except_tables_mixed_db.other") == "2\n"

    instance.query("DROP DATABASE except_tables_mixed_db")
    instance.query(
        f"RESTORE DATABASE except_tables_mixed_db FROM {other_element_wants_it_backup}"
    )
    # Neither of the wide element's clauses reaches the table the first element asked for.
    assert instance.query("SELECT count() FROM except_tables_mixed_db.t") == "3\n"

    instance.query("DROP DATABASE except_tables_mixed_db")


def _create_partitioned_table(database):
    """A table with two partitions of different sizes, so a restore says which ones survived.

    `part = 1` holds two rows and `part = 2` holds one, so `SELECT part, count() ... GROUP BY part`
    distinguishes every combination: neither count can be mistaken for the other.
    """
    instance.query(f"DROP DATABASE IF EXISTS {database}")
    instance.query(f"CREATE DATABASE {database}")
    instance.query(
        f"CREATE TABLE {database}.t (part UInt8, id UInt64) "
        f"ENGINE = MergeTree PARTITION BY part ORDER BY id"
    )
    instance.query(f"INSERT INTO {database}.t VALUES (1, 1), (1, 2), (2, 3)")


def _restored_partitions(database, backup_name):
    """Drop the table, restore it alone, and report which partitions came back.

    `BACKUP TABLE` carries no database DDL, so the table is dropped and restored on its own while
    the database stays - `RESTORE DATABASE` would fail here with `UNKNOWN_DATABASE`.
    """
    instance.query(f"DROP TABLE {database}.t")
    instance.query(f"RESTORE TABLE {database}.t FROM {backup_name}")
    return instance.query(
        f"SELECT part, count() FROM {database}.t GROUP BY part ORDER BY part"
    )


def test_partition_scope_two_partitioned_elements_union_their_partitions():
    """Two elements each naming a partition must contribute both, not just the last one.

    Before the fix the partition scopes of the single-table elements were merged into one
    `std::optional<ASTs>` per table with `emplace()`, which destroys the contained value, so each
    partitioned element *discarded* what the previous ones had asked for. The result was last-wins:
    this backup silently dropped `part = 1` although the user named it explicitly.

    No exclusion clause is involved, so this reproduces on plain `BACKUP` and is independent of the
    `EXCEPT DATA` work - the defect is pre-existing in `master`.
    """
    _create_partitioned_table("partition_scope_union_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE partition_scope_union_db.t PARTITION '1', "
        f"TABLE partition_scope_union_db.t PARTITION '2' TO {backup_name}"
    )

    # Both partitions were asked for, so both must come back.
    assert (
        _restored_partitions("partition_scope_union_db", backup_name) == "1\t2\n2\t1\n"
    )

    instance.query("DROP DATABASE partition_scope_union_db")


def test_partition_scope_element_excluding_data_contributes_no_partitions():
    """An element carrying `EXCEPT DATA FROM TABLE` must contribute none of its partitions.

    Before the fix this failed in both directions at once: the partition `part = 1` that the first
    element asked for was erased by the second element's `emplace()`, and the `part = 2` data that
    the second element excluded was backed up anyway, because the exclusion had been reduced to a
    single boolean per table and was intersected across elements (`true && false`) rather than kept
    per element.

    So the assertion pins both halves - the surviving partition is the one that was asked for, and
    it is the only one.
    """
    _create_partitioned_table("partition_scope_excluded_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE partition_scope_excluded_db.t PARTITION '1', "
        f"TABLE partition_scope_excluded_db.t PARTITION '2' "
        f"EXCEPT DATA FROM TABLE partition_scope_excluded_db.t TO {backup_name}"
    )

    # Element 1 asked for `part = 1`'s data and keeps it; element 2 excluded its own data, so it
    # contributes nothing and `part = 2` must not appear.
    assert (
        _restored_partitions("partition_scope_excluded_db", backup_name) == "1\t2\n"
    )

    instance.query("DROP DATABASE partition_scope_excluded_db")


def test_partition_scope_unsupported_engine_rejected_even_when_data_excluded():
    """Naming a partition of an engine that has none must be refused however the data is left out.

    The partition-support check used to sit behind the `should_backup_data` early return, so every
    way of leaving a table's data out of the backup also skipped the check. `EXCEPT DATA FROM TABLE`
    on a single-object element is one such way - it is stored as the element's `except_data` flag,
    so one element can both name a partition and exclude the data - and `Log` does not support
    partitions, so the backup succeeded and silently ignored the `PARTITION` clause.

    `structure_only` is another such way, and is checked here too: the validation now fires there as
    well, which is a deliberate change to behaviour that predates `EXCEPT DATA`.
    """
    instance.query("DROP DATABASE IF EXISTS partition_scope_unsupported_db")
    instance.query("CREATE DATABASE partition_scope_unsupported_db")
    instance.query(
        "CREATE TABLE partition_scope_unsupported_db.log_t (id UInt64) ENGINE = Log"
    )
    instance.query("INSERT INTO partition_scope_unsupported_db.log_t VALUES (1)")

    # Control: without any exclusion the check has always fired, and must keep firing.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE partition_scope_unsupported_db.log_t PARTITION '1' "
            f"TO {new_backup_name()}"
        )
    assert "doesn't support partitions" in str(exc_info.value), str(exc_info.value)

    # Excluding the data must not buy the query a pass.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE partition_scope_unsupported_db.log_t PARTITION '1' "
            f"EXCEPT DATA FROM TABLE partition_scope_unsupported_db.log_t "
            f"TO {new_backup_name()}"
        )
    assert "doesn't support partitions" in str(exc_info.value), str(exc_info.value)

    # Neither must `structure_only`, which reaches the same early return by another route.
    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP TABLE partition_scope_unsupported_db.log_t PARTITION '1' "
            f"TO {new_backup_name()} SETTINGS structure_only = 1"
        )
    assert "doesn't support partitions" in str(exc_info.value), str(exc_info.value)

    instance.query("DROP DATABASE partition_scope_unsupported_db")


def test_partition_scope_whole_table_element_wins_over_partitioned_element():
    """An element asking for the whole table must not be narrowed by a later partitioned element.

    The most serious case of the family and the one nobody reported: an element naming the table
    with no `PARTITION` clause asks for all of it, and a later element naming one partition used to
    overwrite that request, so the backup silently held one partition of a table the user had asked
    for in full. Naming a partition is a request for *more* data, never a licence to drop the rest.

    Like the union case this needs no exclusion clause at all.
    """
    _create_partitioned_table("partition_scope_whole_table_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE partition_scope_whole_table_db.t, "
        f"TABLE partition_scope_whole_table_db.t PARTITION '1' TO {backup_name}"
    )

    # The unpartitioned element subsumes the partitioned one, so the whole table is backed up.
    assert (
        _restored_partitions("partition_scope_whole_table_db", backup_name)
        == "1\t2\n2\t1\n"
    )

    instance.query("DROP DATABASE partition_scope_whole_table_db")


def test_partition_scope_excluded_partitioned_element_leaves_only_the_other():
    """The over-shoot guard: excluding one partitioned element's data must not exclude the other's.

    This is the ordering where the old merge happened to produce the right answer - the second
    element's `emplace()` discarded `part = 1` which the first element had excluded anyway, and the
    intersected boolean came out false, so `part = 2` was backed up either way. It therefore passes
    both before and after the fix, which is exactly what makes it useful: a fix that over-shot into
    "an exclusion on any element excludes the table" would drop `part = 2` here and fail.
    """
    _create_partitioned_table("partition_scope_guard_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE partition_scope_guard_db.t PARTITION '1' "
        f"EXCEPT DATA FROM TABLE partition_scope_guard_db.t, "
        f"TABLE partition_scope_guard_db.t PARTITION '2' TO {backup_name}"
    )

    # Element 1 excluded its own `part = 1`; element 2 asked for `part = 2` and keeps it.
    assert _restored_partitions("partition_scope_guard_db", backup_name) == "2\t1\n"

    instance.query("DROP DATABASE partition_scope_guard_db")


def test_partition_scope_database_element_wants_the_whole_table():
    """A `DATABASE` element asking for the whole table must not be narrowed by a partitioned element.

    `partitionsWithData` consults only the single-table elements, but `isTableDataExcluded` consults the
    wide elements too, so the two disagreed about the same table: the `DATABASE` element made the data
    eligible for backup, and then the partition scope of the unrelated single-table element decided how
    much of it was written. The backup silently held one partition of a database the user asked for in
    full.

    This is the cross-element form of
    `test_partition_scope_whole_table_element_wins_over_partitioned_element` - the same "a request for
    more data is not a licence to drop the rest" rule, between a wide element and a single-table one.
    """
    _create_partitioned_table("partition_scope_database_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP DATABASE partition_scope_database_db, "
        f"TABLE partition_scope_database_db.t PARTITION '1' TO {backup_name}"
    )

    # The `DATABASE` element asked for the whole table, which subsumes the partitioned element.
    assert (
        _restored_partitions("partition_scope_database_db", backup_name)
        == "1\t2\n2\t1\n"
    )

    instance.query("DROP DATABASE partition_scope_database_db")


def test_partition_scope_partitioned_element_before_database_element():
    """The same as above with the elements the other way round, since neither order is privileged.

    Worth its own test because the single-table element is recorded in `tables` and the wide one in
    `all_tables_elements`, two separate containers consulted at different points, so an order
    dependence would not be visible in either one alone.
    """
    _create_partitioned_table("partition_scope_database_first_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE partition_scope_database_first_db.t PARTITION '1', "
        f"DATABASE partition_scope_database_first_db TO {backup_name}"
    )

    assert (
        _restored_partitions("partition_scope_database_first_db", backup_name)
        == "1\t2\n2\t1\n"
    )

    instance.query("DROP DATABASE partition_scope_database_first_db")


def test_partition_scope_all_element_wants_the_whole_table():
    """An `ALL` element asks for the whole table just as a `DATABASE` element does.

    `ALL` and `DATABASE` both arrive as one `all_tables_elements` entry, so this shares the defect and
    the fix, but it is pinned separately because that is the bot's second reported shape and because
    only `ALL` reaches tables in databases the query never names.

    Bare `ALL` is used deliberately: `ALL EXCEPT DATABASE system` cannot be combined with another
    element in either position - the name list swallows the comma before the next element, the same
    parser limitation `e7f7dc81914` recorded for `EXCEPT TABLES`.
    """
    _create_partitioned_table("partition_scope_all_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP ALL, TABLE partition_scope_all_db.t PARTITION '1' TO {backup_name}"
    )

    assert (
        _restored_partitions("partition_scope_all_db", backup_name) == "1\t2\n2\t1\n"
    )

    instance.query("DROP DATABASE partition_scope_all_db")


def test_partition_scope_wide_element_excluding_data_leaves_the_named_partition():
    """The over-shoot guard: a wide element that excludes the data must not widen the partition scope.

    Without this, a fix that simply forced the whole table whenever any wide element selected the table
    would pass the three tests above and still be wrong here: the `DATABASE` element excludes this
    table's data, so it asks for none of it, and the only element wanting data asked for `part = 1`
    alone. `part = 2` must not appear.

    It is the case that already behaved correctly, so it passes both before and after the fix - which
    is exactly what makes it useful.
    """
    _create_partitioned_table("partition_scope_wide_excluded_db")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE partition_scope_wide_excluded_db.t PARTITION '1', "
        f"DATABASE partition_scope_wide_excluded_db "
        f"EXCEPT DATA FROM TABLE partition_scope_wide_excluded_db.t TO {backup_name}"
    )

    # Only the single-table element wants data, and it named `part = 1`.
    assert (
        _restored_partitions("partition_scope_wide_excluded_db", backup_name) == "1\t2\n"
    )

    instance.query("DROP DATABASE partition_scope_wide_excluded_db")


def test_except_tables_on_the_outer_table_keeps_its_nested_table_internal():
    """`EXCEPT TABLES` must not be able to hide the table that makes a nested table recognisable.

    A `MaterializedPostgreSQL` nested table is classified through the outer table whose UUID its name
    carries, and that classification runs over the enumeration. `EXCEPT TABLES db.pg` removed the
    outer table from the enumeration *before* the classification, so nothing in it owned
    `<uuid>_nested` any more, the nested table was taken for an ordinary table, and the user who
    excluded the outer table got its hidden table in the backup instead - as a user-visible table
    that a restore brings back.

    Both arms exclude the outer table and neither may leak the nested one: through a `DATABASE`
    element and through an `ALL` element, because both carry their own `EXCEPT TABLES`.
    """
    pg_table = "mpg_except_tbls"
    # The name is not needed here - the assertions match the `_nested` marker, because a backup path
    # escapes the UUID in the name (`-` becomes `%2D`) - but the call is what creates the table.
    create_materialized_postgresql_table(pg_table, 30)

    # 1. A DATABASE element excluding the outer table.
    instance.query(
        f"BACKUP DATABASE default EXCEPT TABLES {pg_table} "
        "TO Disk('backups', 'mpg_except_tables_db/')"
    )
    from_database = backup_entries("mpg_except_tables_db")
    assert not any(
        "_nested" in path for path in from_database
    ), f"nested table leaked into the backup: {from_database}"
    assert not any(
        f"/{pg_table}" in path or path.endswith(f"{pg_table}.sql")
        for path in from_database
    ), f"the excluded outer table is in the backup: {from_database}"

    # 2. The same exclusion written on an ALL element.
    instance.query(
        f"BACKUP ALL EXCEPT DATABASE system EXCEPT TABLES default.{pg_table} "
        "TO Disk('backups', 'mpg_except_tables_all/')"
    )
    from_all = backup_entries("mpg_except_tables_all")
    assert not any(
        "_nested" in path for path in from_all
    ), f"nested table leaked into the backup: {from_all}"
    assert not any(
        f"/{pg_table}" in path or path.endswith(f"{pg_table}.sql")
        for path in from_all
    ), f"the excluded outer table is in the backup: {from_all}"

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")


def test_except_data_on_a_nested_table_is_rejected_when_the_outer_table_is_excluded():
    """The clause on a nested table stays refused when `EXCEPT TABLES` names the outer table.

    Both clauses in one query:

        BACKUP DATABASE db EXCEPT TABLES db.pg EXCEPT DATA FROM TABLE db.`<uuid>_nested`

    `EXCEPT TABLES` used to take the outer table out of the enumeration before the classification, so
    the nested table was not recognised as inner and the clause naming it was silently accepted -
    which is the same escape as the leak above, seen from the validation side. It has to be refused
    with `INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION`, as it is when the outer table is not excluded.
    """
    pg_table = "mpg_ex_tbl_rej"
    nested_table = create_materialized_postgresql_table(pg_table, 30)

    with pytest.raises(Exception) as exc_info:
        instance.query(
            f"BACKUP DATABASE default EXCEPT TABLES {pg_table} "
            f"EXCEPT DATA FROM TABLE `{nested_table}` TO {new_backup_name()}"
        )
    assert "INNER_TABLE_NOT_ALLOWED_IN_BACKUP_EXCLUSION" in str(exc_info.value), str(
        exc_info.value
    )

    # Negative control: an ordinary table of the same shape, whose UUID owns nothing, still takes the
    # clause in the very same query shape - the rejection comes from the outer table, not the name.
    nested_like = "aabbccdd-eeff-0011-2233-445566778899_nested"
    instance.query("DROP DATABASE IF EXISTS except_tables_shape_db")
    instance.query("CREATE DATABASE except_tables_shape_db")
    instance.query(
        f"CREATE TABLE except_tables_shape_db.`{nested_like}` (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(f"INSERT INTO except_tables_shape_db.`{nested_like}` VALUES (1), (2)")
    instance.query(
        "CREATE TABLE except_tables_shape_db.other (id UInt64) ENGINE = MergeTree ORDER BY id"
    )

    control_backup = new_backup_name()
    instance.query(
        f"BACKUP DATABASE except_tables_shape_db EXCEPT TABLES other "
        f"EXCEPT DATA FROM TABLE `{nested_like}` TO {control_backup}"
    )
    instance.query("DROP DATABASE except_tables_shape_db")
    instance.query(f"RESTORE DATABASE except_tables_shape_db FROM {control_backup}")
    assert (
        instance.query(f"SELECT count() FROM except_tables_shape_db.`{nested_like}`")
        == "0\n"
    )
    instance.query("DROP DATABASE except_tables_shape_db")

    instance.query(f"DROP TABLE IF EXISTS default.{pg_table} SYNC")
    pg_manager.execute(f"DROP TABLE IF EXISTS {pg_table}")
