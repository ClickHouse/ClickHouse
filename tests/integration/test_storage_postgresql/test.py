import time

import pytest
import psycopg2
from multiprocessing.dummy import Pool

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=["configs/log_conf.xml"], with_postgres=True)

def get_postgres_conn(database=False):
    if database == True:
        conn_string = "host='localhost' dbname='clickhouse' user='postgres' password='mysecretpassword'"
    else:
        conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn

def create_postgres_db(conn, name):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE {}".format(name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        postgres_conn = get_postgres_conn()
        print("postgres connected")
        create_postgres_db(postgres_conn, 'clickhouse')
        yield cluster

    finally:
        cluster.shutdown()


def test_postgres_select_insert(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    table_name = 'test_many'
    table = '''postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name)
    cursor.execute('CREATE TABLE IF NOT EXISTS {} (a integer, b text, c integer)'.format(table_name))

    result = node1.query('''
        INSERT INTO TABLE FUNCTION {}
        SELECT number, concat('name_', toString(number)), 3 from numbers(10000)'''.format(table))
    check1 = "SELECT count() FROM {}".format(table)
    check2 = "SELECT Sum(c) FROM {}".format(table)
    check3 = "SELECT count(c) FROM {} WHERE a % 2 == 0".format(table)
    check4 = "SELECT count() FROM {} WHERE b LIKE concat('name_', toString(1))".format(table)
    assert (node1.query(check1)).rstrip() == '10000'
    assert (node1.query(check2)).rstrip() == '30000'
    assert (node1.query(check3)).rstrip() == '5000'
    assert (node1.query(check4)).rstrip() == '1'


def test_postgres_conversions(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS test_types (
        a smallint, b integer, c bigint, d real, e double precision, f serial, g bigserial,
        h timestamp, i date, j decimal(5, 3), k numeric)''')
    node1.query('''
        INSERT INTO TABLE FUNCTION postgresql('postgres1:5432', 'clickhouse', 'test_types', 'postgres', 'mysecretpassword') VALUES
        (-32768, -2147483648, -9223372036854775808, 1.12345, 1.1234567890, 2147483647, 9223372036854775807, '2000-05-12 12:12:12', '2000-05-12', 22.222, 22.222)''')
    result = node1.query('''
        SELECT a, b, c, d, e, f, g, h, i, j, toDecimal128(k, 3) FROM postgresql('postgres1:5432', 'clickhouse', 'test_types', 'postgres', 'mysecretpassword')''')
    assert(result == '-32768\t-2147483648\t-9223372036854775808\t1.12345\t1.123456789\t2147483647\t9223372036854775807\t2000-05-12 12:12:12\t2000-05-12\t22.222\t22.222\n')

    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS test_array_dimensions
           (
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

    result = node1.query('''
        DESCRIBE TABLE postgresql('postgres1:5432', 'clickhouse', 'test_array_dimensions', 'postgres', 'mysecretpassword')''')
    expected = ('a\tArray(Date)\t\t\t\t\t\n' +
               'b\tArray(DateTime)\t\t\t\t\t\n' +
               'c\tArray(Array(Float32))\t\t\t\t\t\n' +
               'd\tArray(Array(Float64))\t\t\t\t\t\n' +
               'e\tArray(Array(Array(Decimal(5, 5))))\t\t\t\t\t\n' +
               'f\tArray(Array(Array(Int32)))\t\t\t\t\t\n' +
               'g\tArray(Array(Array(Array(Array(String)))))\t\t\t\t\t\n' +
               'h\tArray(Array(Array(Nullable(Int32))))\t\t\t\t\t\n' +
               'i\tArray(Array(Array(Array(Nullable(String)))))\t\t\t\t\t\n' +
               'k\tArray(Nullable(String))'
               )
    assert(result.rstrip() == expected)

    node1.query("INSERT INTO TABLE FUNCTION postgresql('postgres1:5432', 'clickhouse', 'test_array_dimensions', 'postgres', 'mysecretpassword') "
        "VALUES ("
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

    result = node1.query('''
        SELECT * FROM postgresql('postgres1:5432', 'clickhouse', 'test_array_dimensions', 'postgres', 'mysecretpassword')''')
    expected = (
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
    assert(result == expected)


def test_non_default_scema(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    cursor.execute('CREATE SCHEMA test_schema')
    cursor.execute('CREATE TABLE test_schema.test_table (a integer)')
    cursor.execute('INSERT INTO test_schema.test_table SELECT i FROM generate_series(0, 99) as t(i)')

    node1.query('''
        CREATE TABLE test_pg_table_schema (a UInt32)
        ENGINE PostgreSQL('postgres1:5432', 'clickhouse', 'test_table', 'postgres', 'mysecretpassword', 'test_schema');
    ''')

    result = node1.query('SELECT * FROM test_pg_table_schema')
    expected = node1.query('SELECT number FROM numbers(100)')
    assert(result == expected)

    table_function = '''postgresql('postgres1:5432', 'clickhouse', 'test_table', 'postgres', 'mysecretpassword', 'test_schema')'''
    result = node1.query('SELECT * FROM {}'.format(table_function))
    assert(result == expected)

    cursor.execute('''CREATE SCHEMA "test.nice.schema"''')
    cursor.execute('''CREATE TABLE "test.nice.schema"."test.nice.table" (a integer)''')
    cursor.execute('INSERT INTO "test.nice.schema"."test.nice.table" SELECT i FROM generate_series(0, 99) as t(i)')

    node1.query('''
        CREATE TABLE test_pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('postgres1:5432', 'clickhouse', 'test.nice.table', 'postgres', 'mysecretpassword', 'test.nice.schema');
    ''')
    result = node1.query('SELECT * FROM test_pg_table_schema_with_dots')
    assert(result == expected)


def test_concurrent_queries(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()

    node1.query('''
        CREATE TABLE test_table (key UInt32, value UInt32)
        ENGINE = PostgreSQL('postgres1:5432', 'clickhouse', 'test_table', 'postgres', 'mysecretpassword')''')

    cursor.execute('CREATE TABLE test_table (key integer, value integer)')

    prev_count =  node1.count_in_log('New connection to postgres1:5432')
    def node_select(_):
        for i in range(20):
            result = node1.query("SELECT * FROM test_table", user='default')
    busy_pool = Pool(20)
    p = busy_pool.map_async(node_select, range(20))
    p.wait()
    count =  node1.count_in_log('New connection to postgres1:5432')
    print(count, prev_count)
    # 16 is default size for connection pool
    assert(int(count) == int(prev_count) + 16)

    def node_insert(_):
        for i in range(5):
            result = node1.query("INSERT INTO test_table SELECT number, number FROM numbers(1000)", user='default')

    busy_pool = Pool(5)
    p = busy_pool.map_async(node_insert, range(5))
    p.wait()
    result = node1.query("SELECT count() FROM test_table", user='default')
    print(result)
    assert(int(result) == 5 * 5 * 1000)

    def node_insert_select(_):
        for i in range(5):
            result = node1.query("INSERT INTO test_table SELECT number, number FROM numbers(1000)", user='default')
            result = node1.query("SELECT * FROM test_table LIMIT 100", user='default')

    busy_pool = Pool(5)
    p = busy_pool.map_async(node_insert_select, range(5))
    p.wait()
    result = node1.query("SELECT count() FROM test_table", user='default')
    print(result)
    assert(int(result) == 5 * 5 * 1000  * 2)

    node1.query('DROP TABLE test_table;')
    cursor.execute('DROP TABLE test_table;')

    count =  node1.count_in_log('New connection to postgres1:5432')
    print(count, prev_count)
    assert(int(count) == int(prev_count) + 16)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
