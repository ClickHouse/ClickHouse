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
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')

    time.sleep(0.2)
    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    cursor.execute('DROP TABLE postgresql_replica;')
    postgresql_replica_check_result(result, True)


def test_no_connection_at_startup(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    create_postgres_table(cursor, 'postgresql_replica');
    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT number, number from numbers(50)")

    started_cluster.pause_container('postgres1')
    instance.query('''
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')
    time.sleep(3)
    started_cluster.unpause_container('postgres1')

    result = instance.query('SELECT count() FROM test.postgresql_replica;')
    while int(result) == 0:
        result = instance.query('SELECT count() FROM test.postgresql_replica;')
        time.sleep(1);

    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    cursor.execute('DROP TABLE postgresql_replica;')
    postgresql_replica_check_result(result, True)


def test_detach_attach_is_ok(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    create_postgres_table(cursor, 'postgresql_replica');
    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT number, number from numbers(50)")

    instance.query('''
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')

    time.sleep(0.2)
    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    postgresql_replica_check_result(result, True)

    instance.query('DETACH TABLE test.postgresql_replica')
    instance.query('ATTACH TABLE test.postgresql_replica')

    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    cursor.execute('DROP TABLE postgresql_replica;')
    postgresql_replica_check_result(result, True)


def test_replicating_insert_queries(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    create_postgres_table(cursor, 'postgresql_replica');

    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT number, number from numbers(10)")

    instance.query('''
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')

    time.sleep(0.2)
    result = instance.query('SELECT count() FROM test.postgresql_replica;')
    assert(int(result) == 10)

    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT 10 + number, 10 + number from numbers(10)")
    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT 20 + number, 20 + number from numbers(10)")

    time.sleep(2)

    result = instance.query('SELECT count() FROM test.postgresql_replica;')
    assert(int(result) == 30)

    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT 30 + number, 30 + number from numbers(10)")
    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT 40 + number, 40 + number from numbers(10)")

    time.sleep(2)

    result = instance.query('SELECT count() FROM test.postgresql_replica;')
    assert(int(result) == 50)

    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    cursor.execute('DROP TABLE postgresql_replica;')
    postgresql_replica_check_result(result, True)


def test_replicating_delete_queries(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    create_postgres_table(cursor, 'postgresql_replica');

    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT number, number from numbers(50)")

    instance.query('''
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')

    time.sleep(0.2)
    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    postgresql_replica_check_result(result, True)

    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT 50 + number, 50 + number from numbers(50)")
    time.sleep(2)

    result = instance.query('SELECT count() FROM test.postgresql_replica;')
    assert(int(result) == 100)

    cursor.execute('DELETE FROM postgresql_replica WHERE key > 49;')
    time.sleep(2);

    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    cursor.execute('DROP TABLE postgresql_replica;')
    postgresql_replica_check_result(result, True)


def test_replicating_update_queries(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    create_postgres_table(cursor, 'postgresql_replica');

    instance.query("INSERT INTO postgres_database.postgresql_replica SELECT number, number + 10 from numbers(50)")

    instance.query('''
        CREATE TABLE test.postgresql_replica (key UInt64, value UInt64, _sign Int8 MATERIALIZED 1, _version UInt64 MATERIALIZED 1)
            ENGINE = PostgreSQLReplica(
            'postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres', 'mysecretpassword')
            PRIMARY KEY key;
        ''')

    time.sleep(0.2)
    result = instance.query('SELECT count() FROM test.postgresql_replica;')
    assert(int(result) == 50)

    cursor.execute('UPDATE postgresql_replica SET value = value - 10;')
    time.sleep(2);

    result = instance.query('SELECT * FROM test.postgresql_replica ORDER BY key;')
    print(result)
    cursor.execute('DROP TABLE postgresql_replica;')
    postgresql_replica_check_result(result, True)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
