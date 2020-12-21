import pytest
import time
import psycopg2

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=[], with_postgres=True)

postgres_table_template = """
    CREATE TABLE IF NOT EXISTS {} (
    id Integer NOT NULL, value Integer, PRIMARY KEY (id))
    """

def get_postgres_conn(database=False):
    if database == True:
        conn_string = "host='localhost' dbname='test_database' user='postgres' password='mysecretpassword'"
    else:
        conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn

def create_postgres_db(cursor, name):
    cursor.execute("CREATE DATABASE {}".format(name))

def create_postgres_table(cursor, table_name):
    # database was specified in connection string
    cursor.execute(postgres_table_template.format(table_name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn()
        cursor = conn.cursor()
        create_postgres_db(cursor, 'test_database')
        yield cluster

    finally:
        cluster.shutdown()


def test_postgres_database_engine_with_postgres_ddl(started_cluster):
    # connect to database as well
    conn = get_postgres_conn(True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')")
    assert 'test_database' in node1.query('SHOW DATABASES')

    create_postgres_table(cursor, 'test_table')
    assert 'test_table' in node1.query('SHOW TABLES FROM test_database')

    cursor.execute('ALTER TABLE test_table ADD COLUMN data Text')
    assert 'data' in node1.query("SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")

    node1.query("INSERT INTO test_database.test_table SELECT 101, 101, toString(101)")
    assert node1.query("SELECT data FROM test_database.test_table WHERE id = 101").rstrip() == '101'

    cursor.execute('ALTER TABLE test_table DROP COLUMN data')
    assert 'data' not in node1.query("SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")

    cursor.execute('DROP TABLE test_table;')
    assert 'test_table' not in node1.query('SHOW TABLES FROM test_database')

    node1.query("DROP DATABASE test_database")
    assert 'test_database' not in node1.query('SHOW DATABASES')


def test_postgresql_database_engine_with_clickhouse_ddl(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')")

    create_postgres_table(cursor, 'test_table')
    assert 'test_table' in node1.query('SHOW TABLES FROM test_database')

    node1.query("DROP TABLE test_database.test_table")
    assert 'test_table' not in node1.query('SHOW TABLES FROM test_database')

    node1.query("ATTACH TABLE test_database.test_table")
    assert 'test_table' in node1.query('SHOW TABLES FROM test_database')

    node1.query("DETACH TABLE test_database.test_table")
    assert 'test_table' not in node1.query('SHOW TABLES FROM test_database')

    node1.query("ATTACH TABLE test_database.test_table")
    assert 'test_table' in node1.query('SHOW TABLES FROM test_database')

    node1.query("DROP DATABASE test_database")
    assert 'test_database' not in node1.query('SHOW DATABASES')


def test_postgresql_database_engine_queries(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()

    node1.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword')")

    create_postgres_table(cursor, 'test_table')
    assert node1.query("SELECT count() FROM test_database.test_table").rstrip() == '0'

    node1.query("INSERT INTO test_database.test_table SELECT number, number from numbers(10000)")
    assert node1.query("SELECT count() FROM test_database.test_table").rstrip() == '10000'

    node1.query("DROP DATABASE test_database")
    assert 'test_database' not in node1.query('SHOW DATABASES')


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
