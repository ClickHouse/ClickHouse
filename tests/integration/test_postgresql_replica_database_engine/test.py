import pytest
import time
import psycopg2
import os.path as p
import random

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', main_configs=['configs/log_conf.xml'], with_postgres=True)

postgres_table_template = """
    CREATE TABLE IF NOT EXISTS {} (
    key Integer NOT NULL, value Integer, PRIMARY KEY(key))
    """
postgres_table_template_2 = """
    CREATE TABLE IF NOT EXISTS {} (
    key Integer NOT NULL, value1 Integer, value2 Integer, value3 Integer, PRIMARY KEY(key))
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


def create_postgres_table(cursor, table_name, replica_identity_full=False, template=postgres_table_template):
    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))
    cursor.execute(template.format(table_name))
    if replica_identity_full:
        cursor.execute('ALTER TABLE {} REPLICA IDENTITY FULL;'.format(table_name))


def assert_nested_table_is_created(table_name):
    database_tables = instance.query('SHOW TABLES FROM test_database')
    while table_name not in database_tables:
        time.sleep(0.2)
        database_tables = instance.query('SHOW TABLES FROM test_database')
    assert(table_name in database_tables)


def check_tables_are_synchronized(table_name, order_by='key'):
        assert_nested_table_is_created(table_name)

        expected = instance.query('select * from postgres_database.{} order by {};'.format(table_name, order_by))
        result = instance.query('select * from test_database.{} order by {};'.format(table_name, order_by))

        while result != expected:
            time.sleep(0.5)
            result = instance.query('select * from test_database.{} order by {};'.format(table_name, order_by))

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
def postgresql_setup_teardown():
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.postgresql_replica')


@pytest.mark.timeout(120)
def test_load_and_sync_all_database_tables(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test_database")
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        create_postgres_table(cursor, table_name);
        instance.query("INSERT INTO postgres_database.{} SELECT number, number from numbers(50)".format(table_name))

    instance.query("CREATE DATABASE test_database ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')")
    assert 'test_database' in instance.query('SHOW DATABASES')

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        check_tables_are_synchronized(table_name);
        cursor.execute('drop table {};'.format(table_name))

    result = instance.query('''SELECT count() FROM system.tables WHERE database = 'test_database';''')
    assert(int(result) == NUM_TABLES)

    instance.query("DROP DATABASE test_database")
    assert 'test_database' not in instance.query('SHOW DATABASES')


@pytest.mark.timeout(120)
def test_replicating_dml(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test_database")
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i));
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {} from numbers(50)".format(i, i))

    instance.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')")

    for i in range(NUM_TABLES):
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT 50 + number, {} from numbers(1000)".format(i, i))

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        check_tables_are_synchronized(table_name);

    for i in range(NUM_TABLES):
        cursor.execute('UPDATE postgresql_replica_{} SET value = {} * {} WHERE key < 50;'.format(i, i, i))
        cursor.execute('UPDATE postgresql_replica_{} SET value = {} * {} * {} WHERE key >= 50;'.format(i, i, i, i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));

    for i in range(NUM_TABLES):
        cursor.execute('DELETE FROM postgresql_replica_{} WHERE (value*value + {}) % 2 = 0;'.format(i, i))
        cursor.execute('UPDATE postgresql_replica_{} SET value = value - (value % 7) WHERE key > 128 AND key < 512;'.format(i))
        cursor.execute('DELETE FROM postgresql_replica_{} WHERE key % 7 = 1;'.format(i, i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));

    for i in range(NUM_TABLES):
        cursor.execute('drop table postgresql_replica_{};'.format(i))

    instance.query("DROP DATABASE test_database")
    assert 'test_database' not in instance.query('SHOW DATABASES')


@pytest.mark.timeout(120)
def test_different_data_types(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test_database")
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    cursor.execute('drop table if exists test_data_types;')
    cursor.execute('drop table if exists test_array_data_type;')

    cursor.execute(
        '''CREATE TABLE test_data_types (
        id integer PRIMARY KEY, a smallint, b integer, c bigint, d real, e double precision, f serial, g bigserial,
        h timestamp, i date, j decimal(5, 5), k numeric(5, 5))''')

    cursor.execute(
        '''CREATE TABLE test_array_data_type
           (
                key Integer NOT NULL PRIMARY KEY,
                a Date[] NOT NULL,                          -- Date
                b Timestamp[] NOT NULL,                     -- DateTime
                c real[][] NOT NULL,                        -- Float32
                d double precision[][] NOT NULL,            -- Float64
                e decimal(5, 5)[][][] NOT NULL,             -- Decimal32
                f integer[][][] NOT NULL,                   -- Int32
                g Text[][][][][] NOT NULL,                  -- String
                h Integer[][][],                            -- Nullable(Int32)
                i Char(2)[][][][],                          -- Nullable(String)
                k Char(2)[]                                 -- Nullable(String)
           )''')

    instance.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')")

    for i in range(10):
        instance.query('''
            INSERT INTO postgres_database.test_data_types VALUES
            ({}, -32768, -2147483648, -9223372036854775808, 1.12345, 1.1234567890, 2147483647, 9223372036854775807, '2000-05-12 12:12:12', '2000-05-12', 0.2, 0.2)'''.format(i))

    check_tables_are_synchronized('test_data_types', 'id');
    result = instance.query('SELECT * FROM test_database.test_data_types ORDER BY id LIMIT 1;')
    assert(result == '0\t-32768\t-2147483648\t-9223372036854775808\t1.12345\t1.123456789\t2147483647\t9223372036854775807\t2000-05-12 12:12:12\t2000-05-12\t0.20000\t0.20000\n')

    for i in range(10):
        col = random.choice(['a', 'b', 'c'])
        cursor.execute('UPDATE test_data_types SET {} = {};'.format(col, i))
        cursor.execute('''UPDATE test_data_types SET i = '2020-12-12';'''.format(col, i))

    check_tables_are_synchronized('test_data_types', 'id');
    cursor.execute('drop table test_data_types;')

    instance.query("INSERT INTO postgres_database.test_array_data_type "
        "VALUES ("
        "0, "
        "['2000-05-12', '2000-05-12'], "
        "['2000-05-12 12:12:12', '2000-05-12 12:12:12'], "
        "[[1.12345], [1.12345], [1.12345]], "
        "[[1.1234567891], [1.1234567891], [1.1234567891]], "
        "[[[0.11111, 0.11111]], [[0.22222, 0.22222]], [[0.33333, 0.33333]]], "
        "[[[1, 1], [1, 1]], [[3, 3], [3, 3]], [[4, 4], [5, 5]]], "
        "[[[[['winx', 'winx', 'winx']]]]], "
        "[[[1, NULL], [NULL, 1]], [[NULL, NULL], [NULL, NULL]], [[4, 4], [5, 5]]], "
        "[[[[NULL]]]], "
        "[]"
        ")")

    expected = (
        "0\t" +
        "['2000-05-12','2000-05-12']\t" +
        "['2000-05-12 12:12:12','2000-05-12 12:12:12']\t" +
        "[[1.12345],[1.12345],[1.12345]]\t" +
        "[[1.1234567891],[1.1234567891],[1.1234567891]]\t" +
        "[[[0.11111,0.11111]],[[0.22222,0.22222]],[[0.33333,0.33333]]]\t"
        "[[[1,1],[1,1]],[[3,3],[3,3]],[[4,4],[5,5]]]\t"
        "[[[[['winx','winx','winx']]]]]\t"
        "[[[1,NULL],[NULL,1]],[[NULL,NULL],[NULL,NULL]],[[4,4],[5,5]]]\t"
        "[[[[NULL]]]]\t"
        "[]\n"
        )

    check_tables_are_synchronized('test_array_data_type');
    result = instance.query('SELECT * FROM test_database.test_array_data_type ORDER BY key;')
    instance.query("DROP DATABASE test_database")
    assert(result == expected)


@pytest.mark.timeout(120)
def test_load_and_sync_subset_of_database_tables(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test_database")
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    NUM_TABLES = 10

    publication_tables = ''
    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i));
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, number from numbers(50)".format(i))

        if i < int(NUM_TABLES/2):
            if publication_tables != '':
                publication_tables += ', '
            publication_tables += table_name

    instance.query('''
            CREATE DATABASE test_database
            ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')
            SETTINGS postgresql_tables_list = '{}';
    '''.format(publication_tables))
    assert 'test_database' in instance.query('SHOW DATABASES')

    time.sleep(1)

    for i in range(int(NUM_TABLES/2)):
        table_name = 'postgresql_replica_{}'.format(i)
        assert_nested_table_is_created(table_name)

    result = instance.query('''SELECT count() FROM system.tables WHERE database = 'test_database';''')
    assert(int(result) == int(NUM_TABLES/2))

    database_tables = instance.query('SHOW TABLES FROM test_database')
    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        if i < int(NUM_TABLES/2):
            assert table_name in database_tables
        else:
            assert table_name not in database_tables
        instance.query("INSERT INTO postgres_database.{} SELECT 50 + number, {} from numbers(100)".format(table_name, i))

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        if i < int(NUM_TABLES/2):
            check_tables_are_synchronized(table_name);
        cursor.execute('drop table {};'.format(table_name))

    instance.query("DROP DATABASE test_database")
    assert 'test_database' not in instance.query('SHOW DATABASES')


@pytest.mark.timeout(120)
def test_table_schema_changes(started_cluster):
    instance.query("DROP DATABASE IF EXISTS test_database")
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i), template=postgres_table_template_2);
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {}, {}, {} from numbers(25)".format(i, i, i, i))

    instance.query(
        "CREATE DATABASE test_database ENGINE = PostgreSQLReplica('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')")

    for i in range(NUM_TABLES):
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT 25 + number, {}, {}, {} from numbers(25)".format(i, i, i, i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));

    expected = instance.query("SELECT key, value1, value3 FROM test_database.postgresql_replica_3 ORDER BY key");
    cursor.execute("ALTER TABLE postgresql_replica_{} DROP COLUMN value2".format(random.randint(0, 4)))

    for i in range(NUM_TABLES):
        cursor.execute("INSERT INTO postgresql_replica_{} VALUES (50, {}, {})".format(i, i, i))
        cursor.execute("UPDATE postgresql_replica_{} SET value3 = 12 WHERE key%2=0".format(i))

    for i in range(NUM_TABLES):
        check_tables_are_synchronized('postgresql_replica_{}'.format(i));


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
