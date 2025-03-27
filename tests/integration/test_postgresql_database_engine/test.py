import psycopg2
import pytest
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from helpers.cluster import ClickHouseCluster
from helpers.postgres_utility import get_postgres_conn
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/named_collections.xml"],
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
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

    node1.query("DROP DATABASE postgres_database")
    assert "postgres_database" not in node1.query("SHOW DATABASES")

    drop_postgres_table(cursor, "test_table")


def test_postgresql_database_engine_queries(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword', '', 1)"
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword', 'test_schema')"
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
    cursor.execute(f"DROP TABLE IF EXISTS test_table")
    cursor.execute(f"CREATE TABLE test_table (a integer PRIMARY KEY, b integer)")

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
        node1.query(f"SELECT count() FROM postgres_database.test_table").rstrip()
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
        node1.query(f"SELECT count() FROM postgres_database.test_table").rstrip()
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
        node1.query(f"SELECT count() FROM postgres_database.test_table").rstrip()
        == "100"
    )
    node1.query(
        """
        DROP DATABASE postgres_database;
        CREATE DATABASE postgres_database ENGINE = PostgreSQL(postgres1, use_table_cache=1);
        """
    )
    assert (
        node1.query(f"SELECT count() FROM postgres_database.test_table").rstrip()
        == "100"
    )
    assert node1.contains_in_log("Cached table `test_table`")

    node1.query("DROP DATABASE postgres_database")
    cursor.execute(f"DROP TABLE test_table ")
    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")


def test_postgres_database_old_syntax(started_cluster):
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=True
    )
    cursor = conn.cursor()

    node1.query(
        """
        CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword', 1);
        """
    )
    create_postgres_table(cursor, "test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")
    cursor.execute(f"DROP TABLE test_table")
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
    )

    assert node1.query("SHOW TABLES FROM postgres_database") == "table3\n"
    assert not node1.contains_in_log("PostgreSQL table table1 does not exist")

    cursor.execute(f"DROP TABLE table3")
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
        "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword', 'test_schema')"
    )

    node1.query("DROP DATABASE IF EXISTS postgres_database2")
    node1.query(
        "CREATE DATABASE postgres_database2 ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
    )

    assert "mysecretpassword" not in node1.query("SHOW CREATE postgres_database.table1")
    assert "mysecretpassword" not in node1.query(
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


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
