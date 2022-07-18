import pytest
import psycopg2

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/named_collections.xml"], with_postgres=True
)

postgres_table_template = """
    CREATE TABLE {} (
    id Integer NOT NULL, value Integer, PRIMARY KEY (id))
    """

postgres_drop_table_template = """
    DROP TABLE {}
    """


def get_postgres_conn(cluster, database=False):
    if database == True:
        conn_string = f"host={cluster.postgres_ip} port={cluster.postgres_port} dbname='test_database' user='postgres' password='mysecretpassword'"
    else:
        conn_string = f"host={cluster.postgres_ip} port={cluster.postgres_port} user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn


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
        conn = get_postgres_conn(cluster)
        cursor = conn.cursor()
        create_postgres_db(cursor, "test_database")
        yield cluster

    finally:
        cluster.shutdown()


def test_postgres_database_engine_with_postgres_ddl(started_cluster):
    # connect to database as well
    conn = get_postgres_conn(started_cluster, True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')"
    )
    assert "test_database" in node1.query("SHOW DATABASES")

    create_postgres_table(cursor, "test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM test_database")

    cursor.execute("ALTER TABLE test_table ADD COLUMN data Text")
    assert "data" in node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'"
    )

    cursor.execute("ALTER TABLE test_table DROP COLUMN data")
    assert "data" not in node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'"
    )

    node1.query("DROP DATABASE test_database")
    assert "test_database" not in node1.query("SHOW DATABASES")

    drop_postgres_table(cursor, "test_table")


def test_postgresql_database_engine_with_clickhouse_ddl(started_cluster):
    conn = get_postgres_conn(started_cluster, True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')"
    )

    create_postgres_table(cursor, "test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM test_database")

    node1.query("DROP TABLE test_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM test_database")

    node1.query("ATTACH TABLE test_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM test_database")

    node1.query("DETACH TABLE test_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM test_database")

    node1.query("ATTACH TABLE test_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM test_database")

    node1.query("DROP DATABASE test_database")
    assert "test_database" not in node1.query("SHOW DATABASES")

    drop_postgres_table(cursor, "test_table")


def test_postgresql_database_engine_queries(started_cluster):
    conn = get_postgres_conn(started_cluster, True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')"
    )

    create_postgres_table(cursor, "test_table")
    assert node1.query("SELECT count() FROM test_database.test_table").rstrip() == "0"

    node1.query(
        "INSERT INTO test_database.test_table SELECT number, number from numbers(10000)"
    )
    assert (
        node1.query("SELECT count() FROM test_database.test_table").rstrip() == "10000"
    )

    drop_postgres_table(cursor, "test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM test_database")

    node1.query("DROP DATABASE test_database")
    assert "test_database" not in node1.query("SHOW DATABASES")


def test_get_create_table_query_with_multidim_arrays(started_cluster):
    conn = get_postgres_conn(started_cluster, True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')"
    )

    cursor.execute(
        """
    CREATE TABLE array_columns (
        b Integer[][][] NOT NULL,
        c Integer[][][]
    )"""
    )

    node1.query("DETACH TABLE test_database.array_columns")
    node1.query("ATTACH TABLE test_database.array_columns")

    node1.query(
        "INSERT INTO test_database.array_columns "
        "VALUES ("
        "[[[1, 1], [1, 1]], [[3, 3], [3, 3]], [[4, 4], [5, 5]]], "
        "[[[1, NULL], [NULL, 1]], [[NULL, NULL], [NULL, NULL]], [[4, 4], [5, 5]]] "
        ")"
    )
    result = node1.query(
        """
        SELECT * FROM test_database.array_columns"""
    )
    expected = (
        "[[[1,1],[1,1]],[[3,3],[3,3]],[[4,4],[5,5]]]\t"
        "[[[1,NULL],[NULL,1]],[[NULL,NULL],[NULL,NULL]],[[4,4],[5,5]]]\n"
    )
    assert result == expected

    node1.query("DROP DATABASE test_database")
    assert "test_database" not in node1.query("SHOW DATABASES")
    drop_postgres_table(cursor, "array_columns")


def test_postgresql_database_engine_table_cache(started_cluster):
    conn = get_postgres_conn(started_cluster, True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', '', 1)"
    )

    create_postgres_table(cursor, "test_table")
    assert (
        node1.query("DESCRIBE TABLE test_database.test_table").rstrip()
        == "id\tInt32\t\t\t\t\t\nvalue\tNullable(Int32)"
    )

    cursor.execute("ALTER TABLE test_table ADD COLUMN data Text")
    assert (
        node1.query("DESCRIBE TABLE test_database.test_table").rstrip()
        == "id\tInt32\t\t\t\t\t\nvalue\tNullable(Int32)"
    )

    node1.query("DETACH TABLE test_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM test_database")

    node1.query("ATTACH TABLE test_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM test_database")

    assert (
        node1.query("DESCRIBE TABLE test_database.test_table").rstrip()
        == "id\tInt32\t\t\t\t\t\nvalue\tNullable(Int32)\t\t\t\t\t\ndata\tNullable(String)"
    )

    node1.query("DROP TABLE test_database.test_table")
    assert "test_table" not in node1.query("SHOW TABLES FROM test_database")

    node1.query("ATTACH TABLE test_database.test_table")
    assert "test_table" in node1.query("SHOW TABLES FROM test_database")

    node1.query(
        "INSERT INTO test_database.test_table SELECT number, number, toString(number) from numbers(10000)"
    )
    assert (
        node1.query("SELECT count() FROM test_database.test_table").rstrip() == "10000"
    )

    cursor.execute("DROP TABLE test_table;")
    assert "test_table" not in node1.query("SHOW TABLES FROM test_database")

    node1.query("DROP DATABASE test_database")
    assert "test_database" not in node1.query("SHOW DATABASES")


def test_postgresql_database_with_schema(started_cluster):
    conn = get_postgres_conn(started_cluster, True)
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA test_schema")
    cursor.execute("CREATE TABLE test_schema.table1 (a integer)")
    cursor.execute("CREATE TABLE test_schema.table2 (a integer)")
    cursor.execute("CREATE TABLE table3 (a integer)")

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'test_schema')"
    )

    assert node1.query("SHOW TABLES FROM test_database") == "table1\ntable2\n"

    node1.query("INSERT INTO test_database.table1 SELECT number from numbers(10000)")
    assert node1.query("SELECT count() FROM test_database.table1").rstrip() == "10000"
    node1.query("DETACH TABLE test_database.table1")
    node1.query("ATTACH TABLE test_database.table1")
    assert node1.query("SELECT count() FROM test_database.table1").rstrip() == "10000"
    node1.query("DROP DATABASE test_database")

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
    assert result.strip().endswith(
        "ENGINE = PostgreSQL(postgres1, table = \\'test_table\\')"
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

    node1.query("DROP DATABASE postgres_database")
    cursor.execute(f"DROP TABLE test_table ")
    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE")

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

if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
