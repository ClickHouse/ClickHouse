"""
Test database namespace isolation.

When a user has DATABASE NAMESPACE set and the server has database_namespace_separator
configured, all non-system database names are transparently prefixed with
"{namespace}{separator}".

This MUST be an integration test because database_namespace_separator is a startup-only
server setting that does NOT support SYSTEM RELOAD CONFIG.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/database_namespace.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def setup_cluster():
    try:
        cluster.start()

        # Create tenant users with DATABASE NAMESPACE
        node.query("CREATE USER tenant1_user DATABASE NAMESPACE tenant1")
        node.query("CREATE USER tenant2_user DATABASE NAMESPACE tenant2")
        node.query("GRANT ALL ON *.* TO tenant1_user")
        node.query("GRANT ALL ON *.* TO tenant2_user")

        yield cluster
    finally:
        # Cleanup
        for db in [
            "tenant1__testns",
            "tenant2__testns",
            "tenant1__otherdb",
            "tenant1__joindb",
            "tenant1__srcdb",
            "tenant3__altdb",
            "tenant1__sneakydb",
            "tenant1__renamedb",
            "tenant1__renamedb2",
            "shared_db",
            "shared_db2",
        ]:
            node.query(f"DROP DATABASE IF EXISTS {db}")
        for user in [
            "tenant1_user",
            "tenant2_user",
            "tenant3_user",
        ]:
            node.query(f"DROP USER IF EXISTS {user}")
        cluster.shutdown()


def q1(query, **kwargs):
    """Query as tenant1 user."""
    return node.query(query, user="tenant1_user", **kwargs)


def q2(query, **kwargs):
    """Query as tenant2 user."""
    return node.query(query, user="tenant2_user", **kwargs)


def q(query, **kwargs):
    """Query as default (admin) user."""
    return node.query(query, **kwargs)


# ============================================================
# Test 1: Server setting is active
# ============================================================
def test_server_setting_active():
    result = q(
        "SELECT value FROM system.server_settings WHERE name = 'database_namespace_separator'"
    )
    assert result.strip() == "__"


# ============================================================
# Test 2: CREATE DATABASE with namespace
# ============================================================
def test_create_database():
    q1("CREATE DATABASE testns")
    # Verify physical name exists in system.databases
    result = q1("SELECT name FROM system.databases WHERE name = 'tenant1__testns'")
    assert result.strip() == "tenant1__testns"


# ============================================================
# Test 3: CREATE TABLE and query with namespace
# ============================================================
def test_create_table_and_query():
    q1("CREATE TABLE testns.t1 (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO testns.t1 VALUES (1), (2), (3)")
    result = q1("SELECT * FROM testns.t1 ORDER BY x")
    assert result.strip() == "1\n2\n3"


# ============================================================
# Test 4: USE database works with namespace
# ============================================================
def test_use_database():
    result = q1("USE testns; SELECT currentDatabase()")
    # currentDatabase() should return the physical name
    assert "tenant1__testns" in result


# ============================================================
# Test 5: SHOW DATABASES filters by namespace and strips prefix
# ============================================================
def test_show_databases():
    result = q1("SHOW DATABASES LIKE 'testns'")
    assert result.strip() == "testns"


# ============================================================
# Test 6: Tenant isolation — different namespace sees different databases
# ============================================================
def test_tenant_isolation():
    q2("CREATE DATABASE testns")
    q2("CREATE TABLE testns.t1 (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q2("INSERT INTO testns.t1 VALUES (10), (20), (30)")
    result = q2("SELECT * FROM testns.t1 ORDER BY x")
    assert result.strip() == "10\n20\n30"
    # Verify physical name
    result = q2("SELECT name FROM system.databases WHERE name = 'tenant2__testns'")
    assert result.strip() == "tenant2__testns"


# ============================================================
# Test 7: Switching back to tenant1 sees tenant1's data
# ============================================================
def test_tenant1_sees_own_data():
    result = q1("SELECT * FROM testns.t1 ORDER BY x")
    assert result.strip() == "1\n2\n3"


# ============================================================
# Test 8: System databases are not prefixed
# ============================================================
def test_system_databases_not_prefixed():
    result = q1("SELECT count() > 0 FROM system.databases")
    assert result.strip() == "1"


# ============================================================
# Test 9: DROP database works with namespace
# ============================================================
def test_drop_database():
    q1("CREATE DATABASE otherdb")
    result = q1("SELECT name FROM system.databases WHERE name = 'tenant1__otherdb'")
    assert result.strip() == "tenant1__otherdb"
    q1("DROP DATABASE otherdb")
    result = q1("SELECT count() FROM system.databases WHERE name = 'tenant1__otherdb'")
    assert result.strip() == "0"


# ============================================================
# Test 10: Without namespace, physical names are visible
# ============================================================
def test_admin_sees_physical_names():
    result = q("SELECT count() FROM system.databases WHERE name = 'tenant1__testns'")
    assert result.strip() == "1"
    result = q("SELECT count() FROM system.databases WHERE name = 'tenant2__testns'")
    assert result.strip() == "1"


# ============================================================
# Test 11: SHOW CREATE DATABASE strips namespace prefix
# ============================================================
def test_show_create_database():
    result = q1("SHOW CREATE DATABASE testns")
    assert "testns" in result
    assert "tenant1__testns" not in result
    assert "Atomic" in result


# ============================================================
# Test 12: SHOW CREATE TABLE strips namespace prefix
# ============================================================
def test_show_create_table():
    result = q1("SHOW CREATE TABLE testns.t1")
    assert "testns" in result
    assert "tenant1__testns" not in result


# ============================================================
# Test 13: ALTER DATABASE with namespace
# ============================================================
def test_alter_database():
    q1("ALTER DATABASE testns MODIFY COMMENT 'tenant1 test database'")
    result = q1("SELECT comment FROM system.databases WHERE name = 'tenant1__testns'")
    assert result.strip() == "tenant1 test database"


# ============================================================
# Test 14: SHOW TABLES FROM with namespace
# ============================================================
def test_show_tables():
    result = q1("SHOW TABLES FROM testns")
    assert "t1" in result


# ============================================================
# Test 15: RENAME TABLE across databases within same namespace
# ============================================================
def test_rename_table():
    q1("CREATE DATABASE otherdb")
    q1("RENAME TABLE testns.t1 TO otherdb.t1_moved")
    result = q1("SELECT * FROM otherdb.t1_moved ORDER BY x")
    assert result.strip() == "1\n2\n3"
    q1("RENAME TABLE otherdb.t1_moved TO testns.t1")
    q1("DROP DATABASE otherdb")


# ============================================================
# Test 16: EXISTS TABLE with namespace
# ============================================================
def test_exists_table():
    result = q1("EXISTS TABLE testns.t1")
    assert result.strip() == "1"
    result = q1("EXISTS TABLE testns.nonexistent")
    assert result.strip() == "0"


# ============================================================
# Test 17: EXISTS DATABASE with namespace
# ============================================================
def test_exists_database():
    result = q1("EXISTS DATABASE testns")
    assert result.strip() == "1"
    result = q1("EXISTS DATABASE nonexistent_db")
    assert result.strip() == "0"


# ============================================================
# Test 18: TRUNCATE TABLE with namespace
# ============================================================
def test_truncate_table():
    q1("CREATE TABLE testns.t_trunc (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO testns.t_trunc VALUES (100), (200)")
    result = q1("SELECT count() FROM testns.t_trunc")
    assert result.strip() == "2"
    q1("TRUNCATE TABLE testns.t_trunc")
    result = q1("SELECT count() FROM testns.t_trunc")
    assert result.strip() == "0"
    q1("DROP TABLE testns.t_trunc")


# ============================================================
# Test 19: OPTIMIZE TABLE with namespace
# ============================================================
def test_optimize_table():
    q1("CREATE TABLE testns.t_opt (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO testns.t_opt VALUES (1)")
    q1("INSERT INTO testns.t_opt VALUES (2)")
    q1("OPTIMIZE TABLE testns.t_opt FINAL")
    result = q1("SELECT count() FROM testns.t_opt")
    assert result.strip() == "2"
    q1("DROP TABLE testns.t_opt")


# ============================================================
# Test 20: EXCHANGE TABLES with namespace
# ============================================================
def test_exchange_tables():
    q1("CREATE TABLE testns.t_ex1 (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("CREATE TABLE testns.t_ex2 (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO testns.t_ex1 VALUES (11)")
    q1("INSERT INTO testns.t_ex2 VALUES (22)")
    q1("EXCHANGE TABLES testns.t_ex1 AND testns.t_ex2")
    result = q1("SELECT * FROM testns.t_ex1")
    assert result.strip() == "22"
    result = q1("SELECT * FROM testns.t_ex2")
    assert result.strip() == "11"
    q1("DROP TABLE testns.t_ex1")
    q1("DROP TABLE testns.t_ex2")


# ============================================================
# Test 21: UNDROP TABLE with namespace
# ============================================================
def test_undrop_table():
    q1(
        "SET database_atomic_wait_for_drop_and_detach_synchronously = 0; "
        "CREATE TABLE testns.t_undrop (x UInt32) ENGINE = MergeTree() ORDER BY x"
    )
    q1("INSERT INTO testns.t_undrop VALUES (42)")
    q1(
        "SET database_atomic_wait_for_drop_and_detach_synchronously = 0; "
        "DROP TABLE testns.t_undrop"
    )
    result = q1(
        "SELECT table FROM system.dropped_tables "
        "WHERE database = 'tenant1__testns' AND table = 't_undrop' LIMIT 1"
    )
    assert result.strip() == "t_undrop"
    q1("UNDROP TABLE testns.t_undrop")
    result = q1("SELECT * FROM testns.t_undrop")
    assert result.strip() == "42"
    q1("DROP TABLE testns.t_undrop SYNC")


# ============================================================
# Test 22: SHOW CREATE USER shows DATABASE NAMESPACE
# ============================================================
def test_show_create_user():
    result = q("SHOW CREATE USER tenant1_user")
    assert "DATABASE NAMESPACE tenant1" in result


# ============================================================
# Test 23: Default database behavior — tenant connects without
# specifying a database and can use the default database
# ============================================================
def test_default_database():
    result = q1("SELECT currentDatabase()")
    assert result.strip() == "default"
    q1(
        "CREATE TABLE default.t_default_test (x UInt32) ENGINE = MergeTree() ORDER BY x"
    )
    q1("INSERT INTO default.t_default_test VALUES (999)")
    result = q1("SELECT * FROM default.t_default_test")
    assert result.strip() == "999"
    q1("DROP TABLE default.t_default_test")


# ============================================================
# Test 24: DESCRIBE TABLE with namespace
# ============================================================
def test_describe_table():
    result = q1("DESCRIBE TABLE testns.t1")
    assert "x" in result
    assert "UInt32" in result


# ============================================================
# Test 25: Cross-database JOIN within same namespace
# ============================================================
def test_cross_database_join():
    q1("CREATE DATABASE joindb")
    q1(
        "CREATE TABLE joindb.t2 (x UInt32, y String) ENGINE = MergeTree() ORDER BY x"
    )
    q1("INSERT INTO joindb.t2 VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    result = q1(
        "SELECT a.x, b.y FROM testns.t1 AS a JOIN joindb.t2 AS b ON a.x = b.x ORDER BY a.x"
    )
    assert result.strip() == "1\tone\n2\ttwo\n3\tthree"
    q1("DROP DATABASE joindb")


# ============================================================
# Test 26: INSERT ... SELECT across namespaced databases
# ============================================================
def test_insert_select():
    q1("CREATE DATABASE srcdb")
    q1("CREATE TABLE srcdb.src (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO srcdb.src VALUES (100), (200), (300)")
    q1("CREATE TABLE testns.t_dest (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO testns.t_dest SELECT * FROM srcdb.src")
    result = q1("SELECT * FROM testns.t_dest ORDER BY x")
    assert result.strip() == "100\n200\n300"
    q1("DROP TABLE testns.t_dest")
    q1("DROP DATABASE srcdb")


# ============================================================
# Test 27: CREATE VIEW on namespaced table
# ============================================================
def test_view():
    q1("CREATE VIEW testns.v1 AS SELECT x * 10 AS x10 FROM testns.t1")
    result = q1("SELECT * FROM testns.v1 ORDER BY x10")
    assert result.strip() == "10\n20\n30"
    # SHOW CREATE should show view definition with un-namespaced db name
    result = q1("SHOW CREATE TABLE testns.v1")
    assert "testns" in result
    assert "tenant1__testns" not in result
    q1("DROP VIEW testns.v1")


# ============================================================
# Test 28: ATTACH/DETACH with namespace
# ============================================================
def test_attach_detach():
    q1("CREATE TABLE testns.t_ad (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q1("INSERT INTO testns.t_ad VALUES (77)")
    q1("DETACH TABLE testns.t_ad")
    # Table should not be visible after detach
    result = q1("EXISTS TABLE testns.t_ad")
    assert result.strip() == "0"
    q1("ATTACH TABLE testns.t_ad")
    # Table should be back with data
    result = q1("SELECT * FROM testns.t_ad")
    assert result.strip() == "77"
    q1("DROP TABLE testns.t_ad")


# ============================================================
# Test 29: Cross-tenant isolation — tenant1 cannot see tenant2's
# databases even by using the physical name
# ============================================================
def test_cross_tenant_isolation():
    # tenant1 tries to access "tenant2__testns" — this gets namespaced
    # to "tenant1__tenant2__testns" which doesn't exist
    error = node.query_and_get_error(
        "SELECT 1 FROM tenant2__testns.t1", user="tenant1_user"
    )
    assert "UNKNOWN_DATABASE" in error
    # Verify tenant2's data is truly separate
    result = q2("SELECT * FROM testns.t1 ORDER BY x")
    assert result.strip() == "10\n20\n30"


# ============================================================
# Test 30: INFORMATION_SCHEMA access from tenant user
# ============================================================
def test_information_schema():
    result = q1(
        "SELECT count() > 0 FROM INFORMATION_SCHEMA.TABLES "
        "WHERE table_schema = 'tenant1__testns'"
    )
    assert result.strip() == "1"
    result = q1(
        "SELECT count() > 0 FROM information_schema.tables "
        "WHERE table_schema = 'tenant1__testns'"
    )
    assert result.strip() == "1"


# ============================================================
# Test 31: ALTER USER to change/remove namespace
# ============================================================
def test_alter_user_namespace():
    q("CREATE USER tenant3_user DATABASE NAMESPACE tenant3")
    q("GRANT ALL ON *.* TO tenant3_user")

    def q3(query, **kwargs):
        return node.query(query, user="tenant3_user", **kwargs)

    # Create a database under tenant3 namespace
    q3("CREATE DATABASE altdb")
    q3("CREATE TABLE altdb.t1 (x UInt32) ENGINE = MergeTree() ORDER BY x")
    q3("INSERT INTO altdb.t1 VALUES (333)")
    result = q3("SELECT * FROM altdb.t1")
    assert result.strip() == "333"
    # Verify physical name
    result = q("SELECT count() FROM system.databases WHERE name = 'tenant3__altdb'")
    assert result.strip() == "1"
    # Change namespace to tenant3b
    q("ALTER USER tenant3_user DATABASE NAMESPACE tenant3b")
    # Now the user sees tenant3b namespace — altdb is no longer visible
    result = q3("EXISTS DATABASE altdb")
    assert result.strip() == "0"
    # Remove namespace entirely
    q("ALTER USER tenant3_user DATABASE NAMESPACE NONE")
    # Now the user has no namespace — can see all databases by physical name
    result = q3("SELECT count() FROM system.databases WHERE name = 'tenant3__altdb'")
    assert result.strip() == "1"
    # Cleanup
    q("DROP DATABASE IF EXISTS tenant3__altdb")
    q("DROP USER IF EXISTS tenant3_user")


# ============================================================
# Test 32: Database name containing separator is rejected
# ============================================================
def test_separator_in_db_name():
    # Any database name with the separator "__" should be rejected
    error = node.query_and_get_error("CREATE DATABASE tenant1__sneaky")
    assert "BAD_ARGUMENTS" in error
    # Namespaced user also can't use separator in their logical db name
    error = node.query_and_get_error(
        "CREATE DATABASE bad__name", user="tenant1_user"
    )
    assert "BAD_ARGUMENTS" in error
    # But namespaced user CAN still create normal databases
    q1("CREATE DATABASE sneakydb")
    result = q1(
        "SELECT count() FROM system.databases WHERE name = 'tenant1__sneakydb'"
    )
    assert result.strip() == "1"
    result = q(
        "SELECT count() FROM system.databases WHERE name = 'tenant1__sneakydb'"
    )
    assert result.strip() == "1"
    q1("DROP DATABASE sneakydb")


# ============================================================
# Test 33: Namespace value cannot contain separator
# ============================================================
def test_namespace_with_separator():
    error = node.query_and_get_error(
        "CREATE USER bad_ns_user DATABASE NAMESPACE 'bad__ns'"
    )
    assert "BAD_ARGUMENTS" in error
    q("DROP USER IF EXISTS bad_ns_user")


# ============================================================
# Test 34: RENAME DATABASE target name cannot contain separator
# ============================================================
def test_rename_database_separator_rejected():
    # Create a database as tenant1 (physical: tenant1__renamedb)
    q1("CREATE DATABASE renamedb")
    # Admin tries to rename it to a name containing separator — must fail
    error = node.query_and_get_error(
        "RENAME DATABASE `tenant1__renamedb` TO `bad__name`"
    )
    assert "BAD_ARGUMENTS" in error
    # Tenant user tries to rename to a logical name containing separator — must fail
    error = node.query_and_get_error(
        "RENAME DATABASE renamedb TO `new__name`", user="tenant1_user"
    )
    assert "BAD_ARGUMENTS" in error
    # Tenant user CAN rename to a name without separator
    q1("RENAME DATABASE renamedb TO renamedb2")
    # Verify rename succeeded (physical: tenant1__renamedb2)
    assert q("SELECT count() FROM system.databases WHERE name = 'tenant1__renamedb2'").strip() == "1"
    assert q("SELECT count() FROM system.databases WHERE name = 'tenant1__renamedb'").strip() == "0"
    # Cleanup
    q1("DROP DATABASE renamedb2")


# ============================================================
# Test 35: Shared databases are visible to all tenants
# ============================================================
def test_shared_database_visible():
    # Admin creates a shared database (listed in shared_databases_across_namespaces)
    q("CREATE DATABASE shared_db")
    q("CREATE TABLE shared_db.shared_table (id UInt32) ENGINE = Memory")
    q("INSERT INTO shared_db.shared_table VALUES (42)")

    # Tenant1 can see it in SHOW DATABASES
    result = q1("SHOW DATABASES")
    assert "shared_db" in result

    # Tenant1 can query tables in it (not namespaced — accesses real shared_db)
    result = q1("SELECT id FROM shared_db.shared_table")
    assert result.strip() == "42"

    # Tenant2 can also see and query it
    result = q2("SHOW DATABASES")
    assert "shared_db" in result
    result = q2("SELECT id FROM shared_db.shared_table")
    assert result.strip() == "42"

    # Shared database is NOT namespaced — physical name is 'shared_db', not 'tenant1__shared_db'
    assert q("SELECT count() FROM system.databases WHERE name = 'shared_db'").strip() == "1"
    assert q("SELECT count() FROM system.databases WHERE name = 'tenant1__shared_db'").strip() == "0"

    # Cleanup
    q("DROP TABLE shared_db.shared_table")
    q("DROP DATABASE shared_db")


# ============================================================
# Test 36: Shared databases support dynamic reload
# ============================================================
def test_shared_database_reload():
    # Create two databases: shared_db (in initial config) and shared_db2 (not yet shared)
    q("CREATE DATABASE shared_db")
    q("CREATE DATABASE shared_db2")
    q("CREATE TABLE shared_db2.t (x UInt32) ENGINE = Memory")
    q("INSERT INTO shared_db2.t VALUES (99)")

    # Initially tenant can see shared_db but NOT shared_db2
    result = q1("SHOW DATABASES")
    assert "shared_db" in result
    assert "shared_db2" not in result

    # Add shared_db2 to the shared list via config reload
    with node.with_replace_config(
        "/etc/clickhouse-server/config.d/database_namespace.xml",
        "<clickhouse>"
        "<database_namespace_separator>__</database_namespace_separator>"
        "<shared_databases_across_namespaces>shared_db,shared_db2</shared_databases_across_namespaces>"
        "</clickhouse>",
    ):
        node.query("SYSTEM RELOAD CONFIG")

        # Now tenant can see both
        result = q1("SHOW DATABASES")
        assert "shared_db" in result
        assert "shared_db2" in result

        # Tenant can query shared_db2
        result = q1("SELECT x FROM shared_db2.t")
        assert result.strip() == "99"

    # After reverting config, reload again — shared_db2 disappears
    node.query("SYSTEM RELOAD CONFIG")
    result = q1("SHOW DATABASES")
    assert "shared_db" in result
    assert "shared_db2" not in result

    # Cleanup
    q("DROP DATABASE shared_db")
    q("DROP TABLE shared_db2.t")
    q("DROP DATABASE shared_db2")


# ============================================================
# Test 37: Tenant cannot shadow a shared database
# ============================================================
def test_shared_database_no_shadow():
    # Admin creates the shared database
    q("CREATE DATABASE shared_db")

    # Tenant tries CREATE DATABASE shared_db — should fail because
    # applyDatabaseNamespace('shared_db') returns 'shared_db' (not namespaced),
    # and shared_db already exists.
    error = node.query_and_get_error(
        "CREATE DATABASE shared_db", user="tenant1_user"
    )
    assert "ALREADY_EXISTS" in error or "DATABASE_ALREADY_EXISTS" in error

    # Cleanup
    q("DROP DATABASE shared_db")
