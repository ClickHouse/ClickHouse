import pytest
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import get_postgres_conn

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/named_collections.xml", "configs/backups.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
)

postgres_table_template = """
    CREATE TABLE {} (
    id Integer NOT NULL, value Integer, PRIMARY KEY (id))
    """

postgres_drop_table_template = """
    DROP TABLE {}
    """


def create_postgres_db(cursor, name):
    cursor.execute("CREATE DATABASE {}".format(name))


def create_postgres_table(cursor, table_name):
    # database was specified in connection string
    cursor.execute(postgres_table_template.format(table_name))


def drop_postgres_table(cursor, table_name):
    # database was specified in connection string
    cursor.execute(postgres_drop_table_template.format(table_name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn(cluster.postgres_ip, cluster.postgres_port)
        cursor = conn.cursor()
        create_postgres_db(cursor, "postgres_database")
        yield cluster

    finally:
        cluster.shutdown()


def test_postgres_database_engine_with_postgres_ddl(started_cluster):
    # connect to database as well
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )
    assert "postgres_database" in node1.query("SHOW DATABASES")

    create_postgres_table(cursor, "test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")

    cursor.execute("ALTER TABLE test_table ADD COLUMN data Text")
    assert "data" in node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'postgres_database'"
    )

    cursor.execute("ALTER TABLE test_table DROP COLUMN data")
    assert "data" not in node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'postgres_database'"
    )

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")

    drop_postgres_table(cursor, "test_table")


def test_postgresql_database_engine_with_clickhouse_ddl(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    create_postgres_table(cursor, "test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("DROP TABLE postgres_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("ATTACH TABLE postgres_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("DETACH TABLE postgres_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("ATTACH TABLE postgres_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")

    # Test DETACH PERMANENTLY for system.detached_tables (PR #107943)
    node1.query("DETACH TABLE postgres_database.test_table PERMANENTLY")
    is_perm = node1.query(
        "SELECT is_permanently FROM system.detached_tables WHERE database = 'postgres_database' AND table = 'test_table'"
    ).strip()
    assert is_perm == "1", f"Expected is_permanently=1, got {is_perm}"
    node1.query("ATTACH TABLE postgres_database.test_table")

    # Test ordinary DETACH TABLE (not PERMANENTLY) for system.detached_tables (PR #107943)
    node1.query("DETACH TABLE postgres_database.test_table")
    is_perm_ordinary = node1.query(
        "SELECT is_permanently FROM system.detached_tables WHERE database = 'postgres_database' AND table = 'test_table'"
    ).strip()
    assert is_perm_ordinary == "0", f"Expected is_permanently=0 for ordinary detach, got {is_perm_ordinary}"
    node1.query("ATTACH TABLE postgres_database.test_table")

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")

    drop_postgres_table(cursor, "test_table")


def test_postgresql_database_engine_queries(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    create_postgres_table(cursor, "test_table")
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip() == "0"
    )

    node1.query(
        "INSERT INTO postgres_database.test_table SELECT number, number from numbers(10000)"
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip()
        == "10000"
    )

    drop_postgres_table(cursor, "test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")


def test_get_create_table_query_with_multidim_arrays(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    cursor.execute(
        """
    CREATE TABLE array_columns (
        b Integer[][][] NOT NULL,
        c Integer[][][]
    )"""
    )

    node1.query("DETACH TABLE postgres_database.array_columns")
    node1.query("ATTACH TABLE postgres_database.array_columns")

    node1.query(
        "INSERT INTO postgres_database.array_columns "
        "VALUES ("
        "[[[1, 1], [1, 1]], [[3, 3], [3, 3]], [[4, 4], [5, 5]]], "
        "[[[1, NULL], [NULL, 1]], [[NULL, NULL], [NULL, NULL]], [[4, 4], [5, 5]]] "
        ")"
    )
    result = node1.query(
        """
        SELECT * FROM postgres_database.array_columns"""
    )
    expected = (
        "[[[1,1],[1,1]],[[3,3],[3,3]],[[4,4],[5,5]]]\t"
        "[[[1,NULL],[NULL,1]],[[NULL,NULL],[NULL,NULL]],[[4,4],[5,5]]]\n"
    )
    assert result == expected

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")
    drop_postgres_table(cursor, "array_columns")


def test_postgresql_database_engine_table_cache(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}', '', 1)"
    )

    create_postgres_table(cursor, "test_table")
    assert (
        node1.query("DESCRIBE TABLE postgres_database.test_table").rstrip()
        == "id\tInt32\t\t\t\t\t\nvalue\tNullable(Int32)"
    )

    cursor.execute("ALTER TABLE test_table ADD COLUMN data Text")
    assert (
        node1.query("DESCRIBE TABLE postgres_database.test_table").rstrip()
        == "id\tInt32\t\t\t\t\t\nvalue\tNullable(Int32)"
    )

    node1.query("DETACH TABLE postgres_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("ATTACH TABLE postgres_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")

    assert (
        node1.query("DESCRIBE TABLE postgres_database.test_table").rstrip()
        == "id\tInt32\t\t\t\t\t\nvalue\tNullable(Int32)\t\t\t\t\t\ndata\tNullable(String)"
    )

    node1.query("DROP TABLE postgres_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("ATTACH TABLE postgres_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")

    node1.query(
        "INSERT INTO postgres_database.test_table SELECT number, number, toString(number) from numbers(10000)"
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip()
        == "10000"
    )

    cursor.execute("DROP TABLE test_table;")
    assert "test_table" not in node1.query("SHOW TABLES FROM postgres_database")

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")


def test_postgresql_database_with_schema(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.table1 (a integer)")
    cursor.execute("CREATE TABLE test_schema.table2 (a integer)")
    cursor.execute("CREATE TABLE table3 (a integer)")

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}', 'test_schema')"
    )

    assert node1.query("SHOW TABLES FROM postgres_database") == "table1\ntable2\n"

    node1.query(
        "INSERT INTO postgres_database.table1 SELECT number from numbers(10000)"
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.table1").rstrip() == "10000"
    )
    node1.query("DETACH TABLE postgres_database.table1")
    node1.query("ATTACH TABLE postgres_database.table1")
    assert (
        node1.query("SELECT count() FROM postgres_database.table1").rstrip() == "10000"
    )
    node1.query("DROP DATABASE postgres_database")

    cursor.execute("DROP SCHEMA test_schema CASCADE")
    cursor.execute("DROP TABLE table3")


def test_predefined_connection_configuration(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_table")
    cursor.execute("CREATE TABLE test_table (a integer PRIMARY KEY, b integer)")

    node1.query("DROP DATABASE IF EXISTS postgres_database")
    node1.query("CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres1)")

    result = node1.query(
        "select create_table_query from system.tables where database ='postgres_database'"
    )
    print(f"kssenii: {result}")
    assert result.strip().endswith(
        "ENGINE = PostgreSQL(postgres1, `table` = \\'test_table\\')"
    )

    node1.query(
        "INSERT INTO postgres_database.test_table SELECT number, number from numbers(100)"
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip()
        == "100"
    )

    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.test_table (a integer)")

    node1.query("DROP DATABASE IF EXISTS postgres_database")
    node1.query(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres1, schema='test_schema')"
    )
    node1.query(
        "INSERT INTO postgres_database.test_table SELECT number from numbers(200)"
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip()
        == "200"
    )

    node1.query("DROP DATABASE IF EXISTS postgres_database")
    node1.query_and_get_error(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres1, 'test_schema')"
    )
    node1.query_and_get_error(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres2)"
    )
    node1.query_and_get_error(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL(unknown_collection)"
    )
    node1.query(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres3, port=5432)"
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip()
        == "100"
    )
    node1.query(
        """
        DROP DATABASE postgres_database;
        CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres1, use_table_cache=1);
        """
    )
    assert (
        node1.query("SELECT count() FROM postgres_database.test_table").rstrip()
        == "100"
    )
    assert node1.contains_in_log("Cached table `test_table`")

    node1.query("DROP DATABASE postgres_database")
    cursor.execute("DROP TABLE test_table ")
    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")


def test_postgres_database_old_syntax(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"""
        CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}', 1);
        """
    )
    create_postgres_table(cursor, "test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")
    cursor.execute("DROP TABLE test_table")
    node1.query("DROP DATABASE IF EXISTS postgres_database;")


def test_postgresql_fetch_tables(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")
    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.table1 (a integer)")
    cursor.execute("CREATE TABLE test_schema.table2 (a integer)")
    cursor.execute("CREATE TABLE table3 (a integer)")

    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    assert node1.query("SHOW TABLES FROM postgres_database") == "table3\n"
    assert not node1.contains_in_log("PostgreSQL table table1 does not exist")

    node1.query("DROP DATABASE postgres_database")
    cursor.execute("DROP TABLE table3")
    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")


def test_datetime(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("drop table if exists test")
    cursor.execute("create table test (u timestamp)")

    node1.query("drop database if exists pg")
    node1.query("create database pg engine = PostgreSQL(postgres1)")
    assert "DateTime64(6)" in node1.query("show create table pg.test")
    node1.query("detach table pg.test")
    node1.query("attach table pg.test")
    assert "DateTime64(6)" in node1.query("show create table pg.test")


def test_numeric_detach_attach(started_cluster):
    cursor = started_cluster.postgres_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_table")
    cursor.execute("""
        CREATE TABLE test_table (
            numeric_1 numeric NOT NULL,
            numeric_2 numeric(10) NOT NULL,
            numeric_3 numeric(10, 0) NOT NULL,
            numeric_4 numeric(5, 2) NOT NULL,
            numeric_5 numeric(10, 5) NOT NULL,
            numeric_6 numeric(20, 10) NOT NULL,
            numeric_7 numeric(50, 20) NOT NULL,
            decimal_1 decimal NOT NULL,
            decimal_2 decimal(10) NOT NULL,
            decimal_3 decimal(10, 0) NOT NULL,
            decimal_4 decimal(5, 2) NOT NULL,
            decimal_5 decimal(10, 5) NOT NULL,
            decimal_6 decimal(20, 10) NOT NULL,
            decimal_7 decimal(50, 20) NOT NULL
        )
    """)

    node1.query("DROP DATABASE IF EXISTS postgres_database")
    node1.query(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres1)"
    )

    expected_clickhouse_column_types = {
        "numeric_1": "Decimal(38, 19)",
        "numeric_2": "Decimal(10, 0)",
        "numeric_3": "Decimal(10, 0)",
        "numeric_4": "Decimal(5, 2)",
        "numeric_5": "Decimal(10, 5)",
        "numeric_6": "Decimal(20, 10)",
        "numeric_7": "Decimal(50, 20)",
        "decimal_1": "Decimal(38, 19)",
        "decimal_2": "Decimal(10, 0)",
        "decimal_3": "Decimal(10, 0)",
        "decimal_4": "Decimal(5, 2)",
        "decimal_5": "Decimal(10, 5)",
        "decimal_6": "Decimal(20, 10)",
        "decimal_7": "Decimal(50, 20)",
    }

    def get_actual_clickhouse_column_types():
        res = node1.query(
            "SELECT name, type FROM system.columns WHERE database = 'postgres_database' AND table = 'test_table'"
        )

        return dict(line.split('\t') for line in res.splitlines())

    assert get_actual_clickhouse_column_types() == expected_clickhouse_column_types

    create_ddl = node1.query("SHOW CREATE TABLE postgres_database.test_table")
    for column, expected_type in expected_clickhouse_column_types.items():
        assert f"`{column}` {expected_type}" in create_ddl

    node1.query("DETACH TABLE postgres_database.test_table")
    node1.query("ATTACH TABLE postgres_database.test_table")

    assert get_actual_clickhouse_column_types() == expected_clickhouse_column_types

    node1.query("DROP DATABASE postgres_database")
    cursor.execute("DROP TABLE test_table")

def test_postgresql_password_leak(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")
    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.table1 (a integer)")
    cursor.execute("CREATE TABLE table2 (a integer)")

    node1.query("DROP DATABASE IF EXISTS postgres_database")
    node1.query(
        f"CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}', 'test_schema')"
    )

    node1.query("DROP DATABASE IF EXISTS postgres_database2")
    node1.query(
        f"CREATE DATABASE postgres_database2 ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    assert f"{pg_pass}" not in node1.query("SHOW CREATE postgres_database.table1")
    assert f"{pg_pass}" not in node1.query(
        "SHOW CREATE postgres_database2.table2"
    )

    node1.query("DROP DATABASE postgres_database")
    node1.query("DROP DATABASE postgres_database2")

    cursor.execute("DROP SCHEMA test_schema CASCADE")
    cursor.execute("DROP TABLE table2")


# PostgreSQL database engine is created async in ClickHouse (first create the object then another thread
# do the connection), causing a created database object with an inaccessible URI, and access of system.tables
# timed out when touching the inaccessible database. We add the filter engine ability so we add a test here.
def test_inaccessible_postgresql_database_engine_filterable_on_system_tables(
    started_cluster,
):
    # This query takes some time depending on the trial times and conn timeout setting.
    node1.query(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('google.com:5432', 'dummy', 'dummy', 'dummy')"
    )
    assert "postgres_database" in node1.query("SHOW DATABASES")

    # Should quickly return result instead of wasting time in connection since it gets filtered.
    assert (
        node1.query(
            "SELECT DISTINCT(name) FROM system.tables WHERE engine!='PostgreSQL' AND name='COLUMNS'"
        )
        == "COLUMNS\n"
    )

    # Enigne of system.tables in fact means storage name, so View should not get filtered.
    assert (
        node1.query(
            "SELECT DISTINCT(name) FROM system.tables WHERE engine='View' and name='COLUMNS'"
        )
        == "COLUMNS\n"
    )

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")

def test_postgresql_database_engine_comment(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    conn.cursor()

    node1.query(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword') \
        comment 'test postgres database with comment'"
    )

    node1.query(
        "ALTER DATABASE postgres_database MODIFY COMMENT 'new comment on postgres database engine'"
    )

    assert (
        node1.query("SELECT comment FROM system.databases WHERE name = 'postgres_database'").rstrip()
        == "new comment on postgres database engine"
    )

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")


def test_backup_database(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    conn.cursor()

    node1.query(
        "CREATE DATABASE backup_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
    )

    backup_id = uuid.uuid4().hex
    backup_name = f"File('/backups/test_backup_{backup_id}/')"

    node1.query(f"BACKUP DATABASE backup_database TO {backup_name}")
    node1.query("DROP DATABASE backup_database SYNC")
    assert "backup_database" not in node1.query("SHOW DATABASES")

    node1.query(f"RESTORE DATABASE backup_database FROM {backup_name}")
    assert (
        node1.query("SHOW CREATE DATABASE backup_database")
        == "CREATE DATABASE backup_database\\nENGINE = PostgreSQL(\\'postgres1:5432\\', \\'postgres_database\\', \\'postgres\\', \\'[HIDDEN]\\')\n"
    )

    node1.query("DROP DATABASE backup_database")


def test_postgresql_detached_tables_permanent_marker(started_cluster):
    """
    Test that system.detached_tables correctly reports is_permanently for PostgreSQL databases.
    Tests both DETACH TABLE PERMANENTLY and ordinary DETACH TABLE.
    PR #107943
    """
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_test_detach ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    create_postgres_table(cursor, "test_permanent")
    create_postgres_table(cursor, "test_ordinary")
    assert "test_permanent" in node1.query("SHOW TABLES FROM postgres_test_detach")
    assert "test_ordinary" in node1.query("SHOW TABLES FROM postgres_test_detach")

    # Test DETACH TABLE PERMANENTLY
    node1.query("DETACH TABLE postgres_test_detach.test_permanent PERMANENTLY")
    is_perm = node1.query(
        "SELECT is_permanently FROM system.detached_tables "
        "WHERE database = 'postgres_test_detach' AND table = 'test_permanent'"
    ).strip()
    assert is_perm == "1", f"Expected is_permanently=1 for DETACH PERMANENTLY, got {is_perm}"

    # Test ordinary DETACH TABLE (not PERMANENTLY)
    node1.query("DETACH TABLE postgres_test_detach.test_ordinary")
    is_perm_ordinary = node1.query(
        "SELECT is_permanently FROM system.detached_tables "
        "WHERE database = 'postgres_test_detach' AND table = 'test_ordinary'"
    ).strip()
    assert is_perm_ordinary == "0", f"Expected is_permanently=0 for ordinary DETACH, got {is_perm_ordinary}"

    # Attach both back
    node1.query("ATTACH TABLE postgres_test_detach.test_permanent")
    node1.query("ATTACH TABLE postgres_test_detach.test_ordinary")

    # Clean up
    node1.query("DROP DATABASE postgres_test_detach")
    drop_postgres_table(cursor, "test_permanent")
    drop_postgres_table(cursor, "test_ordinary")


def test_postgresql_permanent_detach_marker_preserved_after_remote_drop(started_cluster):
    """
    Test that permanent detach markers (.removed) are preserved when the remote
    PostgreSQL table is dropped and later recreated, ensuring the table stays hidden
    in ClickHouse until explicit ATTACH TABLE.

    This tests the fix in DatabasePostgreSQL::removeOutdatedTables that mirrors
    the MySQL behavior from commit 1986888a213.
    PR #107943
    """
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_marker_test ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    # Create table in PostgreSQL
    create_postgres_table(cursor, "test_marker_persist")
    assert "test_marker_persist" in node1.query("SHOW TABLES FROM postgres_marker_test")

    # DROP TABLE in ClickHouse (creates .removed marker via detachTablePermanently)
    node1.query("DROP TABLE postgres_marker_test.test_marker_persist")

    # Verify table is in detached_tables with is_permanently=1
    detached_count = node1.query(
        "SELECT count() FROM system.detached_tables "
        "WHERE database = 'postgres_marker_test' AND table = 'test_marker_persist'"
    ).strip()
    assert detached_count == "1", f"Expected table in detached_tables, got count {detached_count}"

    is_perm = node1.query(
        "SELECT is_permanently FROM system.detached_tables "
        "WHERE database = 'postgres_marker_test' AND table = 'test_marker_persist'"
    ).strip()
    assert is_perm == "1", f"Expected is_permanently=1 after DROP TABLE, got {is_perm}"

    # Drop table in remote PostgreSQL
    drop_postgres_table(cursor, "test_marker_persist")

    # Trigger schema refresh in ClickHouse by querying the database
    node1.query("SHOW TABLES FROM postgres_marker_test")

    # Wait for background cleaner_task to run (removeOutdatedTables runs every 60 seconds)
    # Poll for up to 65 seconds to ensure at least one execution cycle completes
    import time
    max_wait = 65
    start_time = time.time()
    marker_preserved = False

    while time.time() - start_time < max_wait:
        detached_count = node1.query(
            "SELECT count() FROM system.detached_tables "
            "WHERE database = 'postgres_marker_test' AND table = 'test_marker_persist'"
        ).strip()

        is_perm = node1.query(
            "SELECT is_permanently FROM system.detached_tables "
            "WHERE database = 'postgres_marker_test' AND table = 'test_marker_persist'"
        ).strip()

        if detached_count == "1" and is_perm == "1":
            marker_preserved = True
            break

        time.sleep(5)  # Check every 5 seconds

    # Verify the permanent marker is still preserved even though remote table is gone
    assert marker_preserved, (
        f"Expected permanent marker preserved after remote drop, "
        f"but condition not met within {max_wait}s (count={detached_count}, is_perm={is_perm})"
    )

    # Recreate table in PostgreSQL with same name
    create_postgres_table(cursor, "test_marker_persist")

    # Verify table DOES NOT reappear in ClickHouse (marker kept it hidden)
    tables_after_recreate = node1.query("SHOW TABLES FROM postgres_marker_test")
    assert "test_marker_persist" not in tables_after_recreate, (
        "Table should stay hidden after remote recreation due to preserved .removed marker"
    )

    # Still in detached_tables
    still_detached = node1.query(
        "SELECT count() FROM system.detached_tables "
        "WHERE database = 'postgres_marker_test' AND table = 'test_marker_persist'"
    ).strip()
    assert still_detached == "1", "Expected table still detached after recreation"

    # Now ATTACH should work and remove the marker
    node1.query("ATTACH TABLE postgres_marker_test.test_marker_persist")
    assert "test_marker_persist" in node1.query("SHOW TABLES FROM postgres_marker_test")

    # Marker should be cleared
    after_attach = node1.query(
        "SELECT count() FROM system.detached_tables "
        "WHERE database = 'postgres_marker_test' AND table = 'test_marker_persist'"
    ).strip()
    assert after_attach == "0", f"Expected marker cleared after ATTACH, got count {after_attach}"

    # Clean up
    node1.query("DROP DATABASE postgres_marker_test")
    drop_postgres_table(cursor, "test_marker_persist")


def test_postgresql_ordinary_detach_pruned_after_remote_drop(started_cluster):
    """
    Test that ordinary DETACH TABLE entries (without .removed marker) are pruned
    from detached_or_dropped when the remote table is dropped.

    This ensures the reconciliation logic correctly distinguishes between:
    - Permanent detach (marker preserved)
    - Ordinary detach (entry pruned when remote table disappears)
    PR #107943
    """
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        f"CREATE DATABASE postgres_prune_test ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', '{pg_pass}')"
    )

    # Create table in PostgreSQL
    create_postgres_table(cursor, "test_prune")
    assert "test_prune" in node1.query("SHOW TABLES FROM postgres_prune_test")

    # Ordinary DETACH (not PERMANENTLY)
    node1.query("DETACH TABLE postgres_prune_test.test_prune")

    # Verify in detached_tables with is_permanently=0
    is_perm = node1.query(
        "SELECT is_permanently FROM system.detached_tables "
        "WHERE database = 'postgres_prune_test' AND table = 'test_prune'"
    ).strip()
    assert is_perm == "0", f"Expected is_permanently=0, got {is_perm}"

    # Drop table in remote PostgreSQL
    drop_postgres_table(cursor, "test_prune")

    # Trigger schema refresh
    node1.query("SHOW TABLES FROM postgres_prune_test")

    # Wait for background cleaner_task to run (removeOutdatedTables runs every 60 seconds)
    # Poll for up to 65 seconds to ensure at least one execution cycle completes
    import time
    max_wait = 65
    start_time = time.time()
    entry_pruned = False

    while time.time() - start_time < max_wait:
        pruned_count = node1.query(
            "SELECT count() FROM system.detached_tables "
            "WHERE database = 'postgres_prune_test' AND table = 'test_prune'"
        ).strip()

        if pruned_count == "0":
            entry_pruned = True
            break

        time.sleep(5)  # Check every 5 seconds

    # Verify the entry is PRUNED (not in detached_tables anymore)
    assert entry_pruned, (
        f"Expected ordinary detach entry pruned after remote drop within {max_wait}s, "
        f"but still in detached_tables (count={pruned_count})"
    )

    # Clean up
    node1.query("DROP DATABASE postgres_prune_test")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
