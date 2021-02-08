import pytest
import time
import psycopg2
import os.path as p

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', main_configs=['configs/log_conf.xml'], with_postgres=True)

postgres_table_template = """
    CREATE TABLE IF NOT EXISTS {} (
    key Integer NOT NULL, value Integer)
    """

def get_postgres_conn(database=False):
    if database == True:
        conn_string = "host='localhost' dbname='postgres_database' user='postgres' password='mysecretpassword'"
    else:
        conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn


def create_postgres_db(cursor, name):
    cursor.execute("CREATE DATABASE {}".format(name))


def create_postgres_table(cursor, table_name):
    cursor.execute(postgres_table_template.format(table_name))
    cursor.execute('ALTER TABLE {} REPLICA IDENTITY FULL;'.format(table_name))


def check_tables_are_synchronized(table_name):
        expected = instance.query('select * from postgres_database.{} order by key;'.format(table_name))
        result = instance.query('select * from test_database.{} order by key;'.format(table_name))

        while result != expected:
            time.sleep(0.5)
            result = instance.query('select * from test_database.{} order by key;'.format(table_name))

        assert(result == expected)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn()
        cursor = conn.cursor()
        create_postgres_db(cursor, 'postgres_database')
        instance.query("DROP DATABASE IF EXISTS test_database")
        instance.query('''
                CREATE DATABASE postgres_database
                ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')''')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.postgresql_replica')


def test_load_and_sync_all_database(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i));
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, number from numbers(50)".format(i))

    instance.query("CREATE DATABASE test_database ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')")
    assert 'test_database' in instance.query('SHOW DATABASES')

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));
        cursor.execute('drop table postgresql_replica_{};'.format(i))

    result = instance.query('''SELECT count() FROM system.tables WHERE database = 'test_database';''')
    assert(int(result) == NUM_TABLES)

    instance.query("DROP DATABASE test_database")
    assert 'test_database' not in instance.query('SHOW DATABASES')


def test_replicating_dml(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i));
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {} from numbers(50)".format(i, i))

    instance.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')")

    for i in range(NUM_TABLES):
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {} from numbers(50, 50)".format(i, i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));

    for i in range(NUM_TABLES):
        cursor.execute('UPDATE postgresql_replica_{} SET value = {} * {} WHERE key < 50;'.format(i, i, i))
        cursor.execute('UPDATE postgresql_replica_{} SET value = {} * {} * {} WHERE key >= 50;'.format(i, i, i, i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));

    for i in range(NUM_TABLES):
        cursor.execute('DELETE FROM postgresql_replica_{} WHERE (value*value + {}) % 2 = 0;'.format(i, i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));

    for i in range(NUM_TABLES):
        cursor.execute('drop table postgresql_replica_{};'.format(i))

    instance.query("DROP DATABASE test_database")
    assert 'test_database' not in instance.query('SHOW DATABASES')


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
