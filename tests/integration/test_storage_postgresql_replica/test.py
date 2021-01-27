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
    key Integer NOT NULL, value Integer, PRIMARY KEY (key))
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

def postgresql_replica_check_result(result, check=False, ref_file='test_postgresql_replica.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn()
        cursor = conn.cursor()
        create_postgres_db(cursor, 'postgres_database')
        instance.query('''
                CREATE DATABASE postgres_database
                ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')''')

        instance.query('CREATE DATABASE test')
        yield cluster

    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.postgresql_replica')


def test_initial_load_from_snapshot(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    create_postgres_table(cursor, 'postgresql_replica');
    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT number, number from numbers(50)")

    instance.query('''
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')

    result = instance.query('SELECT * FROM test.postgresql_replica;')
    postgresql_replica_check_result(result, True)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
