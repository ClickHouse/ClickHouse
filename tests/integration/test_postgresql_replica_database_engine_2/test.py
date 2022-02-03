import pytest
import time
import psycopg2
import os.path as p
import random

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from helpers.test_tools import TSV

from random import randrange
import threading

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
        main_configs = ['configs/log_conf.xml'],
        user_configs = ['configs/users.xml'],
        with_postgres=True, stay_alive=True)

postgres_table_template = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key Integer NOT NULL, value Integer, PRIMARY KEY(key))
    """
postgres_table_template_2 = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key Integer NOT NULL, value1 Integer, value2 Integer, value3 Integer, PRIMARY KEY(key))
    """
postgres_table_template_3 = """
    CREATE TABLE IF NOT EXISTS "{}" (
    key1 Integer NOT NULL, value1 Integer, key2 Integer NOT NULL, value2 Integer NOT NULL)
    """
postgres_table_template_4 = """
    CREATE TABLE IF NOT EXISTS "{}"."{}" (
    key Integer NOT NULL, value Integer, PRIMARY KEY(key))
    """

def get_postgres_conn(ip, port, database=False, auto_commit=True, database_name='postgres_database', replication=False):
    if database == True:
        conn_string = "host={} port={} dbname='{}' user='postgres' password='mysecretpassword'".format(ip, port, database_name)
    else:
        conn_string = "host={} port={} user='postgres' password='mysecretpassword'".format(ip, port)

    if replication:
        conn_string += " replication='database'"

    conn = psycopg2.connect(conn_string)
    if auto_commit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        conn.autocommit = True
    return conn

def create_replication_slot(conn, slot_name='user_slot'):
    cursor = conn.cursor()
    cursor.execute('CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT'.format(slot_name))
    result = cursor.fetchall()
    print(result[0][0]) # slot name
    print(result[0][1]) # start lsn
    print(result[0][2]) # snapshot
    return result[0][2]

def drop_replication_slot(conn, slot_name='user_slot'):
    cursor = conn.cursor()
    cursor.execute("select pg_drop_replication_slot('{}')".format(slot_name))

def create_postgres_db(cursor, name='postgres_database'):
    cursor.execute("CREATE DATABASE {}".format(name))

def drop_postgres_db(cursor, name='postgres_database'):
    cursor.execute("DROP DATABASE IF EXISTS {}".format(name))

def drop_postgres_schema(cursor, schema_name):
    cursor.execute('DROP SCHEMA IF EXISTS {} CASCADE'.format(schema_name))

def create_postgres_schema(cursor, schema_name):
    drop_postgres_schema(cursor, schema_name)
    cursor.execute('CREATE SCHEMA {}'.format(schema_name))

def create_clickhouse_postgres_db(ip, port, name='postgres_database', database_name='postgres_database', schema_name=''):
    drop_clickhouse_postgres_db(name)
    if len(schema_name) == 0:
        instance.query('''
                CREATE DATABASE {}
                ENGINE = PostgreSQL('{}:{}', '{}', 'postgres', 'mysecretpassword')'''.format(name, ip, port, database_name))
    else:
        instance.query('''
                CREATE DATABASE {}
                ENGINE = PostgreSQL('{}:{}', '{}', 'postgres', 'mysecretpassword', '{}')'''.format(name, ip, port, database_name, schema_name))

def drop_clickhouse_postgres_db(name='postgres_database'):
    instance.query('DROP DATABASE IF EXISTS {}'.format(name))

def create_materialized_db(ip, port,
                           materialized_database='test_database',
                           postgres_database='postgres_database',
                           settings=[]):
    instance.query(f"DROP DATABASE IF EXISTS {materialized_database}")
    create_query = f"CREATE DATABASE {materialized_database} ENGINE = MaterializedPostgreSQL('{ip}:{port}', '{postgres_database}', 'postgres', 'mysecretpassword')"
    if len(settings) > 0:
        create_query += " SETTINGS "
        for i in range(len(settings)):
            if i != 0:
                create_query += ', '
            create_query += settings[i]
    instance.query(create_query)
    assert materialized_database in instance.query('SHOW DATABASES')

def drop_materialized_db(materialized_database='test_database'):
    instance.query('DROP DATABASE IF EXISTS {}'.format(materialized_database))
    assert materialized_database not in instance.query('SHOW DATABASES')

def drop_postgres_table(cursor, table_name):
    cursor.execute("""DROP TABLE IF EXISTS "{}" """.format(table_name))

def drop_postgres_table_with_schema(cursor, schema_name, table_name):
    cursor.execute("""DROP TABLE IF EXISTS "{}"."{}" """.format(schema_name, table_name))

def create_postgres_table(cursor, table_name, replica_identity_full=False, template=postgres_table_template):
    drop_postgres_table(cursor, table_name)
    cursor.execute(template.format(table_name))
    if replica_identity_full:
        cursor.execute('ALTER TABLE {} REPLICA IDENTITY FULL;'.format(table_name))

def create_postgres_table_with_schema(cursor, schema_name, table_name):
    drop_postgres_table_with_schema(cursor, schema_name, table_name)
    cursor.execute(postgres_table_template_4.format(schema_name, table_name))

queries = [
    'INSERT INTO postgresql_replica_{} select i, i from generate_series(0, 10000) as t(i);',
    'DELETE FROM postgresql_replica_{} WHERE (value*value) % 3 = 0;',
    'UPDATE postgresql_replica_{} SET value = value - 125 WHERE key % 2 = 0;',
    "UPDATE postgresql_replica_{} SET key=key+20000 WHERE key%2=0",
    'INSERT INTO postgresql_replica_{} select i, i from generate_series(40000, 50000) as t(i);',
    'DELETE FROM postgresql_replica_{} WHERE key % 10 = 0;',
    'UPDATE postgresql_replica_{} SET value = value + 101 WHERE key % 2 = 1;',
    "UPDATE postgresql_replica_{} SET key=key+80000 WHERE key%2=1",
    'DELETE FROM postgresql_replica_{} WHERE value % 2 = 0;',
    'UPDATE postgresql_replica_{} SET value = value + 2000 WHERE key % 5 = 0;',
    'INSERT INTO postgresql_replica_{} select i, i from generate_series(200000, 250000) as t(i);',
    'DELETE FROM postgresql_replica_{} WHERE value % 3 = 0;',
    'UPDATE postgresql_replica_{} SET value = value * 2 WHERE key % 3 = 0;',
    "UPDATE postgresql_replica_{} SET key=key+500000 WHERE key%2=1",
    'INSERT INTO postgresql_replica_{} select i, i from generate_series(1000000, 1050000) as t(i);',
    'DELETE FROM postgresql_replica_{} WHERE value % 9 = 2;',
    "UPDATE postgresql_replica_{} SET key=key+10000000",
    'UPDATE postgresql_replica_{} SET value = value + 2  WHERE key % 3 = 1;',
    'DELETE FROM postgresql_replica_{} WHERE value%5 = 0;'
    ]


def assert_nested_table_is_created(table_name, materialized_database='test_database', schema_name=''):
    if len(schema_name) == 0:
        table = table_name
    else:
        table = schema_name + "." + table_name
    print(f'Checking table {table} exists in {materialized_database}')
    database_tables = instance.query('SHOW TABLES FROM {}'.format(materialized_database))
    while table not in database_tables:
        time.sleep(0.2)
        database_tables = instance.query('SHOW TABLES FROM {}'.format(materialized_database))
    assert(table in database_tables)


def assert_number_of_columns(expected, table_name, database_name='test_database'):
    result = instance.query(f"select count() from system.columns where table = '{table_name}' and database = '{database_name}' and not startsWith(name, '_')")
    while (int(result) != expected):
        time.sleep(1)
        result = instance.query(f"select count() from system.columns where table = '{table_name}' and database = '{database_name}' and not startsWith(name, '_')")
    print('Number of columns ok')


@pytest.mark.timeout(320)
def check_tables_are_synchronized(table_name, order_by='key', postgres_database='postgres_database', materialized_database='test_database', schema_name=''):
    assert_nested_table_is_created(table_name, materialized_database, schema_name)

    print("Checking table is synchronized:", table_name)
    expected = instance.query('select * from {}.{} order by {};'.format(postgres_database, table_name, order_by))
    if len(schema_name) == 0:
        result = instance.query('select * from {}.{} order by {};'.format(materialized_database, table_name, order_by))
    else:
        result = instance.query('select * from {}.`{}.{}` order by {};'.format(materialized_database, schema_name, table_name, order_by))

    try_num = 0
    while result != expected:
        time.sleep(0.5)
        if len(schema_name) == 0:
            result = instance.query('select * from {}.{} order by {};'.format(materialized_database, table_name, order_by))
        else:
            result = instance.query('select * from {}.`{}.{}` order by {};'.format(materialized_database, schema_name, table_name, order_by))
        try_num += 1
        if try_num > 30:
            break

    assert(result == expected)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        conn = get_postgres_conn(ip=cluster.postgres_ip, port=cluster.postgres_port)
        cursor = conn.cursor()
        create_postgres_db(cursor, 'postgres_database')
        create_clickhouse_postgres_db(ip=cluster.postgres_ip, port=cluster.postgres_port)

        instance.query("DROP DATABASE IF EXISTS test_database")
        yield cluster

    finally:
        cluster.shutdown()


def test_add_new_table_to_replication(started_cluster):
    drop_materialized_db()
    conn = get_postgres_conn(ip=started_cluster.postgres_ip,
                             port=started_cluster.postgres_port,
                             database=True)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS test_table')
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i));
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {} from numbers(10000)".format(i, i))

    create_materialized_db(ip=started_cluster.postgres_ip,
                           port=started_cluster.postgres_port)

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        check_tables_are_synchronized(table_name);

    result = instance.query("SHOW TABLES FROM test_database")
    assert(result == "postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n")

    table_name = 'postgresql_replica_5'
    create_postgres_table(cursor, table_name)
    instance.query("INSERT INTO postgres_database.{} SELECT number, number from numbers(10000)".format(table_name))

    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(") # Check without ip
    assert(result[-59:] == "\\'postgres_database\\', \\'postgres\\', \\'mysecretpassword\\')\n")

    result = instance.query_and_get_error("ALTER DATABASE test_database MODIFY SETTING materialized_postgresql_tables_list='tabl1'")
    assert('Changing setting `materialized_postgresql_tables_list` is not allowed' in result)

    result = instance.query_and_get_error("ALTER DATABASE test_database MODIFY SETTING materialized_postgresql_tables='tabl1'")
    assert('Database engine MaterializedPostgreSQL does not support setting' in result)

    instance.query("ATTACH TABLE test_database.{}".format(table_name));

    result = instance.query("SHOW TABLES FROM test_database")
    assert(result == "postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\npostgresql_replica_5\n")

    check_tables_are_synchronized(table_name);
    instance.query("INSERT INTO postgres_database.{} SELECT number, number from numbers(10000, 10000)".format(table_name))
    check_tables_are_synchronized(table_name);

    result = instance.query_and_get_error("ATTACH TABLE test_database.{}".format(table_name));
    assert('Table test_database.postgresql_replica_5 already exists' in result)

    result = instance.query_and_get_error("ATTACH TABLE test_database.unknown_table");
    assert('PostgreSQL table unknown_table does not exist' in result)

    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(")
    assert(result[-180:] == ")\\nSETTINGS materialized_postgresql_tables_list = \\'postgresql_replica_0,postgresql_replica_1,postgresql_replica_2,postgresql_replica_3,postgresql_replica_4,postgresql_replica_5\\'\n")

    table_name = 'postgresql_replica_6'
    create_postgres_table(cursor, table_name)
    instance.query("INSERT INTO postgres_database.{} SELECT number, number from numbers(10000)".format(table_name))
    instance.query("ATTACH TABLE test_database.{}".format(table_name));

    instance.restart_clickhouse()

    table_name = 'postgresql_replica_7'
    create_postgres_table(cursor, table_name)
    instance.query("INSERT INTO postgres_database.{} SELECT number, number from numbers(10000)".format(table_name))
    instance.query("ATTACH TABLE test_database.{}".format(table_name));

    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(")
    assert(result[-222:] == ")\\nSETTINGS materialized_postgresql_tables_list = \\'postgresql_replica_0,postgresql_replica_1,postgresql_replica_2,postgresql_replica_3,postgresql_replica_4,postgresql_replica_5,postgresql_replica_6,postgresql_replica_7\\'\n")

    result = instance.query("SHOW TABLES FROM test_database")
    assert(result == "postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\npostgresql_replica_5\npostgresql_replica_6\npostgresql_replica_7\n")

    for i in range(NUM_TABLES + 3):
        table_name = 'postgresql_replica_{}'.format(i)
        check_tables_are_synchronized(table_name);

    for i in range(NUM_TABLES + 3):
        cursor.execute('drop table if exists postgresql_replica_{};'.format(i))

def test_remove_table_from_replication(started_cluster):
    drop_materialized_db()
    conn = get_postgres_conn(ip=started_cluster.postgres_ip,
                             port=started_cluster.postgres_port,
                             database=True)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS test_table')
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, 'postgresql_replica_{}'.format(i));
        instance.query("INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {} from numbers(10000)".format(i, i))

    create_materialized_db(ip=started_cluster.postgres_ip,
                           port=started_cluster.postgres_port)

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        check_tables_are_synchronized(table_name);

    result = instance.query("SHOW TABLES FROM test_database")
    assert(result == "postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n")

    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(")
    assert(result[-59:] == "\\'postgres_database\\', \\'postgres\\', \\'mysecretpassword\\')\n")

    table_name = 'postgresql_replica_4'
    instance.query('DETACH TABLE test_database.{}'.format(table_name));
    result = instance.query_and_get_error('SELECT * FROM test_database.{}'.format(table_name))
    assert("doesn't exist" in result)

    result = instance.query("SHOW TABLES FROM test_database")
    assert(result == "postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\n")

    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(")
    assert(result[-138:] == ")\\nSETTINGS materialized_postgresql_tables_list = \\'postgresql_replica_0,postgresql_replica_1,postgresql_replica_2,postgresql_replica_3\\'\n")

    instance.query('ATTACH TABLE test_database.{}'.format(table_name));
    check_tables_are_synchronized(table_name);

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        check_tables_are_synchronized(table_name);

    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(")
    assert(result[-159:] == ")\\nSETTINGS materialized_postgresql_tables_list = \\'postgresql_replica_0,postgresql_replica_1,postgresql_replica_2,postgresql_replica_3,postgresql_replica_4\\'\n")

    table_name = 'postgresql_replica_1'
    instance.query('DETACH TABLE test_database.{}'.format(table_name));
    result = instance.query('SHOW CREATE DATABASE test_database')
    assert(result[:63] == "CREATE DATABASE test_database\\nENGINE = MaterializedPostgreSQL(")
    assert(result[-138:] == ")\\nSETTINGS materialized_postgresql_tables_list = \\'postgresql_replica_0,postgresql_replica_2,postgresql_replica_3,postgresql_replica_4\\'\n")

    for i in range(NUM_TABLES):
        cursor.execute('drop table if exists postgresql_replica_{};'.format(i))


def test_predefined_connection_configuration(started_cluster):
    drop_materialized_db()
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port, database=True)
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS test_table')
    cursor.execute(f'CREATE TABLE test_table (key integer PRIMARY KEY, value integer)')
    cursor.execute(f'INSERT INTO test_table SELECT 1, 2')

    instance.query("CREATE DATABASE test_database ENGINE = MaterializedPostgreSQL(postgres1) SETTINGS materialized_postgresql_tables_list='test_table'")
    check_tables_are_synchronized("test_table");
    drop_materialized_db()
    cursor.execute('DROP TABLE IF EXISTS test_table')


insert_counter = 0

def test_database_with_single_non_default_schema(started_cluster):
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port, database=True)
    cursor = conn.cursor()

    NUM_TABLES=5
    schema_name = 'test_schema'
    clickhouse_postgres_db = 'postgres_database_with_schema'
    global insert_counter
    insert_counter = 0

    def insert_into_tables():
        global insert_counter
        clickhouse_postgres_db = 'postgres_database_with_schema'
        for i in range(NUM_TABLES):
            table_name = f'postgresql_replica_{i}'
            instance.query(f"INSERT INTO {clickhouse_postgres_db}.{table_name} SELECT number, number from numbers(1000 * {insert_counter}, 1000)")
        insert_counter += 1

    def assert_show_tables(expected):
        result = instance.query('SHOW TABLES FROM test_database')
        assert(result == expected)
        print('assert show tables Ok')

    def check_all_tables_are_synchronized():
        for i in range(NUM_TABLES):
            print('checking table', i)
            check_tables_are_synchronized("postgresql_replica_{}".format(i), postgres_database=clickhouse_postgres_db);
        print('synchronization Ok')

    create_postgres_schema(cursor, schema_name)
    create_clickhouse_postgres_db(ip=cluster.postgres_ip, port=cluster.postgres_port, name=clickhouse_postgres_db, schema_name=schema_name)

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        create_postgres_table_with_schema(cursor, schema_name, table_name);

    insert_into_tables()
    create_materialized_db(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port,
                           settings=[f"materialized_postgresql_schema = '{schema_name}'", "materialized_postgresql_allow_automatic_update = 1"])

    insert_into_tables()
    check_all_tables_are_synchronized()
    assert_show_tables("postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n")

    instance.restart_clickhouse()
    check_all_tables_are_synchronized()
    assert_show_tables("postgresql_replica_0\npostgresql_replica_1\npostgresql_replica_2\npostgresql_replica_3\npostgresql_replica_4\n")
    insert_into_tables()
    check_all_tables_are_synchronized()

    print('ALTER')
    altered_table = random.randint(0, NUM_TABLES-1)
    cursor.execute("ALTER TABLE test_schema.postgresql_replica_{} ADD COLUMN value2 integer".format(altered_table))

    instance.query(f"INSERT INTO {clickhouse_postgres_db}.postgresql_replica_{altered_table} SELECT number, number, number from numbers(5000, 1000)")
    assert_number_of_columns(3, f'postgresql_replica_{altered_table}')
    check_tables_are_synchronized(f"postgresql_replica_{altered_table}", postgres_database=clickhouse_postgres_db);
    drop_materialized_db()


def test_database_with_multiple_non_default_schemas_1(started_cluster):
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port, database=True)
    cursor = conn.cursor()

    NUM_TABLES = 5
    schema_name = 'test_schema'
    clickhouse_postgres_db = 'postgres_database_with_schema'
    publication_tables = ''
    global insert_counter
    insert_counter = 0

    def insert_into_tables():
        global insert_counter
        clickhouse_postgres_db = 'postgres_database_with_schema'
        for i in range(NUM_TABLES):
            table_name = f'postgresql_replica_{i}'
            instance.query(f"INSERT INTO {clickhouse_postgres_db}.{table_name} SELECT number, number from numbers(1000 * {insert_counter}, 1000)")
        insert_counter += 1

    def assert_show_tables(expected):
        result = instance.query('SHOW TABLES FROM test_database')
        assert(result == expected)
        print('assert show tables Ok')

    def check_all_tables_are_synchronized():
        for i in range(NUM_TABLES):
            print('checking table', i)
            check_tables_are_synchronized("postgresql_replica_{}".format(i), schema_name=schema_name, postgres_database=clickhouse_postgres_db);
        print('synchronization Ok')

    create_postgres_schema(cursor, schema_name)
    create_clickhouse_postgres_db(ip=cluster.postgres_ip, port=cluster.postgres_port, name=clickhouse_postgres_db, schema_name=schema_name)

    for i in range(NUM_TABLES):
        table_name = 'postgresql_replica_{}'.format(i)
        create_postgres_table_with_schema(cursor, schema_name, table_name);
        if publication_tables != '':
            publication_tables += ', '
        publication_tables += schema_name + '.' + table_name

    insert_into_tables()
    create_materialized_db(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port,
                           settings=[f"materialized_postgresql_tables_list = '{publication_tables}'", "materialized_postgresql_tables_list_with_schema=1", "materialized_postgresql_allow_automatic_update = 1"])

    check_all_tables_are_synchronized()
    assert_show_tables("test_schema.postgresql_replica_0\ntest_schema.postgresql_replica_1\ntest_schema.postgresql_replica_2\ntest_schema.postgresql_replica_3\ntest_schema.postgresql_replica_4\n")

    instance.restart_clickhouse()
    check_all_tables_are_synchronized()
    assert_show_tables("test_schema.postgresql_replica_0\ntest_schema.postgresql_replica_1\ntest_schema.postgresql_replica_2\ntest_schema.postgresql_replica_3\ntest_schema.postgresql_replica_4\n")

    insert_into_tables()
    check_all_tables_are_synchronized()

    print('ALTER')
    altered_table = random.randint(0, NUM_TABLES-1)
    cursor.execute("ALTER TABLE test_schema.postgresql_replica_{} ADD COLUMN value2 integer".format(altered_table))

    instance.query(f"INSERT INTO {clickhouse_postgres_db}.postgresql_replica_{altered_table} SELECT number, number, number from numbers(5000, 1000)")
    assert_number_of_columns(3, f'{schema_name}.postgresql_replica_{altered_table}')
    check_tables_are_synchronized(f"postgresql_replica_{altered_table}", schema_name=schema_name, postgres_database=clickhouse_postgres_db);
    drop_materialized_db()


def test_database_with_multiple_non_default_schemas_2(started_cluster):
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port, database=True)
    cursor = conn.cursor()

    NUM_TABLES = 2
    schemas_num = 2
    schema_list = 'schema0, schema1'
    global insert_counter
    insert_counter = 0

    def check_all_tables_are_synchronized():
        for i in range(schemas_num):
            schema_name = f'schema{i}'
            clickhouse_postgres_db = f'clickhouse_postgres_db{i}'
            for ti in range(NUM_TABLES):
                table_name = f'postgresql_replica_{ti}'
                print(f'checking table {schema_name}.{table_name}')
                check_tables_are_synchronized(f'{table_name}', schema_name=schema_name, postgres_database=clickhouse_postgres_db);
        print('synchronized Ok')

    def insert_into_tables():
        global insert_counter
        for i in range(schemas_num):
            clickhouse_postgres_db = f'clickhouse_postgres_db{i}'
            for ti in range(NUM_TABLES):
                table_name = f'postgresql_replica_{ti}'
                instance.query(f'INSERT INTO {clickhouse_postgres_db}.{table_name} SELECT number, number from numbers(1000 * {insert_counter}, 1000)')
        insert_counter += 1

    def assert_show_tables(expected):
        result = instance.query('SHOW TABLES FROM test_database')
        assert(result == expected)
        print('assert show tables Ok')

    for i in range(schemas_num):
        schema_name = f'schema{i}'
        clickhouse_postgres_db = f'clickhouse_postgres_db{i}'
        create_postgres_schema(cursor, schema_name)
        create_clickhouse_postgres_db(ip=cluster.postgres_ip, port=cluster.postgres_port, name=clickhouse_postgres_db, schema_name=schema_name)
        for ti in range(NUM_TABLES):
            table_name = f'postgresql_replica_{ti}'
            create_postgres_table_with_schema(cursor, schema_name, table_name);

    insert_into_tables()
    create_materialized_db(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port,
                           settings=[f"materialized_postgresql_schema_list = '{schema_list}'", "materialized_postgresql_allow_automatic_update = 1"])

    check_all_tables_are_synchronized()
    insert_into_tables()
    assert_show_tables("schema0.postgresql_replica_0\nschema0.postgresql_replica_1\nschema1.postgresql_replica_0\nschema1.postgresql_replica_1\n")

    instance.restart_clickhouse()
    assert_show_tables("schema0.postgresql_replica_0\nschema0.postgresql_replica_1\nschema1.postgresql_replica_0\nschema1.postgresql_replica_1\n")
    check_all_tables_are_synchronized()
    insert_into_tables()
    check_all_tables_are_synchronized()

    print('ALTER')
    altered_schema = random.randint(0, schemas_num-1)
    altered_table = random.randint(0, NUM_TABLES-1)
    cursor.execute(f"ALTER TABLE schema{altered_schema}.postgresql_replica_{altered_table} ADD COLUMN value2 integer")

    instance.query(f"INSERT INTO clickhouse_postgres_db{altered_schema}.postgresql_replica_{altered_table} SELECT number, number, number from numbers(1000 * {insert_counter}, 1000)")
    assert_number_of_columns(3, f'schema{altered_schema}.postgresql_replica_{altered_table}')
    check_tables_are_synchronized(f"postgresql_replica_{altered_table}", schema_name=schema_name, postgres_database=clickhouse_postgres_db);
    drop_materialized_db()


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
