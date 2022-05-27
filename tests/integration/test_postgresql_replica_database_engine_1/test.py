import pytest
import time
import os.path as p
import random

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.test_tools import TSV

from random import randrange
import threading

from helpers.postgres_utility import get_postgres_conn
from helpers.postgres_utility import PostgresManager

from helpers.postgres_utility import create_replication_slot, drop_replication_slot
from helpers.postgres_utility import create_postgres_schema, drop_postgres_schema
from helpers.postgres_utility import create_postgres_table, drop_postgres_table
from helpers.postgres_utility import check_tables_are_synchronized
from helpers.postgres_utility import check_several_tables_are_synchronized
from helpers.postgres_utility import assert_nested_table_is_created
from helpers.postgres_utility import assert_number_of_columns
from helpers.postgres_utility import (
    postgres_table_template,
    postgres_table_template_2,
    postgres_table_template_3,
    postgres_table_template_4,
)
from helpers.postgres_utility import queries


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    stay_alive=True,
)

pg_manager = PostgresManager()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        pg_manager.init(instance, cluster.postgres_ip, cluster.postgres_port)
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    print("PostgreSQL is available - running test")
    yield  # run test
    pg_manager.restart()


def test_load_and_sync_all_database_tables(started_cluster):
    NUM_TABLES = 5
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_several_tables_are_synchronized(instance, NUM_TABLES)
    result = instance.query(
        "SELECT count() FROM system.tables WHERE database = 'test_database';"
    )
    assert int(result) == NUM_TABLES


def test_replicating_dml(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(cursor, "postgresql_replica_{}".format(i))
        instance.query(
            "INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {} from numbers(50)".format(
                i, i
            )
        )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    for i in range(NUM_TABLES):
        instance.query(
            "INSERT INTO postgres_database.postgresql_replica_{} SELECT 50 + number, {} from numbers(1000)".format(
                i, i
            )
        )
    check_several_tables_are_synchronized(instance, NUM_TABLES)

    for i in range(NUM_TABLES):
        cursor.execute(
            "UPDATE postgresql_replica_{} SET value = {} * {} WHERE key < 50;".format(
                i, i, i
            )
        )
        cursor.execute(
            "UPDATE postgresql_replica_{} SET value = {} * {} * {} WHERE key >= 50;".format(
                i, i, i, i
            )
        )
    check_several_tables_are_synchronized(instance, NUM_TABLES)

    for i in range(NUM_TABLES):
        cursor.execute(
            "DELETE FROM postgresql_replica_{} WHERE (value*value + {}) % 2 = 0;".format(
                i, i
            )
        )
        cursor.execute(
            "UPDATE postgresql_replica_{} SET value = value - (value % 7) WHERE key > 128 AND key < 512;".format(
                i
            )
        )
        cursor.execute(
            "DELETE FROM postgresql_replica_{} WHERE key % 7 = 1;".format(i, i)
        )
    check_several_tables_are_synchronized(instance, NUM_TABLES)


def test_different_data_types(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()
    cursor.execute("drop table if exists test_data_types;")
    cursor.execute("drop table if exists test_array_data_type;")

    cursor.execute(
        """CREATE TABLE test_data_types (
        id integer PRIMARY KEY, a smallint, b integer, c bigint, d real, e double precision, f serial, g bigserial,
        h timestamp, i date, j decimal(5, 5), k numeric(5, 5))"""
    )

    cursor.execute(
        """CREATE TABLE test_array_data_type
           (
                key Integer NOT NULL PRIMARY KEY,
                a Date[] NOT NULL,                          -- Date
                b Timestamp[] NOT NULL,                     -- DateTime64(6)
                c real[][] NOT NULL,                        -- Float32
                d double precision[][] NOT NULL,            -- Float64
                e decimal(5, 5)[][][] NOT NULL,             -- Decimal32
                f integer[][][] NOT NULL,                   -- Int32
                g Text[][][][][] NOT NULL,                  -- String
                h Integer[][][],                            -- Nullable(Int32)
                i Char(2)[][][][],                          -- Nullable(String)
                k Char(2)[]                                 -- Nullable(String)
           )"""
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    for i in range(10):
        instance.query(
            """
            INSERT INTO postgres_database.test_data_types VALUES
            ({}, -32768, -2147483648, -9223372036854775808, 1.12345, 1.1234567890, 2147483647, 9223372036854775807, '2000-05-12 12:12:12.012345', '2000-05-12', 0.2, 0.2)""".format(
                i
            )
        )

    check_tables_are_synchronized(instance, "test_data_types", "id")
    result = instance.query(
        "SELECT * FROM test_database.test_data_types ORDER BY id LIMIT 1;"
    )
    assert (
        result
        == "0\t-32768\t-2147483648\t-9223372036854775808\t1.12345\t1.123456789\t2147483647\t9223372036854775807\t2000-05-12 12:12:12.012345\t2000-05-12\t0.2\t0.2\n"
    )

    for i in range(10):
        col = random.choice(["a", "b", "c"])
        cursor.execute("UPDATE test_data_types SET {} = {};".format(col, i))
        cursor.execute(
            """UPDATE test_data_types SET i = '2020-12-12';""".format(col, i)
        )

    check_tables_are_synchronized(instance, "test_data_types", "id")

    instance.query(
        "INSERT INTO postgres_database.test_array_data_type "
        "VALUES ("
        "0, "
        "['2000-05-12', '2000-05-12'], "
        "['2000-05-12 12:12:12.012345', '2000-05-12 12:12:12.012345'], "
        "[[1.12345], [1.12345], [1.12345]], "
        "[[1.1234567891], [1.1234567891], [1.1234567891]], "
        "[[[0.11111, 0.11111]], [[0.22222, 0.22222]], [[0.33333, 0.33333]]], "
        "[[[1, 1], [1, 1]], [[3, 3], [3, 3]], [[4, 4], [5, 5]]], "
        "[[[[['winx', 'winx', 'winx']]]]], "
        "[[[1, NULL], [NULL, 1]], [[NULL, NULL], [NULL, NULL]], [[4, 4], [5, 5]]], "
        "[[[[NULL]]]], "
        "[]"
        ")"
    )

    expected = (
        "0\t"
        + "['2000-05-12','2000-05-12']\t"
        + "['2000-05-12 12:12:12.012345','2000-05-12 12:12:12.012345']\t"
        + "[[1.12345],[1.12345],[1.12345]]\t"
        + "[[1.1234567891],[1.1234567891],[1.1234567891]]\t"
        + "[[[0.11111,0.11111]],[[0.22222,0.22222]],[[0.33333,0.33333]]]\t"
        "[[[1,1],[1,1]],[[3,3],[3,3]],[[4,4],[5,5]]]\t"
        "[[[[['winx','winx','winx']]]]]\t"
        "[[[1,NULL],[NULL,1]],[[NULL,NULL],[NULL,NULL]],[[4,4],[5,5]]]\t"
        "[[[[NULL]]]]\t"
        "[]\n"
    )

    check_tables_are_synchronized(instance, "test_array_data_type")
    result = instance.query(
        "SELECT * FROM test_database.test_array_data_type ORDER BY key;"
    )
    assert result == expected

    pg_manager.drop_materialized_db()
    cursor.execute("drop table if exists test_data_types;")
    cursor.execute("drop table if exists test_array_data_type;")


def test_load_and_sync_subset_of_database_tables(started_cluster):
    NUM_TABLES = 10
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES)

    publication_tables = ""
    for i in range(NUM_TABLES):
        if i < int(NUM_TABLES / 2):
            if publication_tables != "":
                publication_tables += ", "
            publication_tables += f"postgresql_replica_{i}"

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = '{}'".format(publication_tables)
        ],
    )

    time.sleep(1)

    for i in range(int(NUM_TABLES / 2)):
        table_name = f"postgresql_replica_{i}"
        assert_nested_table_is_created(instance, table_name)

    result = instance.query(
        """SELECT count() FROM system.tables WHERE database = 'test_database';"""
    )
    assert int(result) == int(NUM_TABLES / 2)

    database_tables = instance.query("SHOW TABLES FROM test_database")
    for i in range(NUM_TABLES):
        table_name = "postgresql_replica_{}".format(i)
        if i < int(NUM_TABLES / 2):
            assert table_name in database_tables
        else:
            assert table_name not in database_tables
        instance.query(
            "INSERT INTO postgres_database.{} SELECT 50 + number, {} from numbers(100)".format(
                table_name, i
            )
        )

    for i in range(NUM_TABLES):
        table_name = f"postgresql_replica_{i}"
        if i < int(NUM_TABLES / 2):
            check_tables_are_synchronized(instance, table_name)


def test_changing_replica_identity_value(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()
    create_postgres_table(cursor, "postgresql_replica")
    instance.query(
        "INSERT INTO postgres_database.postgresql_replica SELECT 50 + number, number from numbers(50)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    instance.query(
        "INSERT INTO postgres_database.postgresql_replica SELECT 100 + number, number from numbers(50)"
    )
    check_tables_are_synchronized(instance, "postgresql_replica")
    cursor.execute("UPDATE postgresql_replica SET key=key-25 WHERE key<100 ")
    check_tables_are_synchronized(instance, "postgresql_replica")


def test_clickhouse_restart(started_cluster):
    NUM_TABLES = 5
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_several_tables_are_synchronized(instance, NUM_TABLES)

    for i in range(NUM_TABLES):
        instance.query(
            "INSERT INTO postgres_database.postgresql_replica_{} SELECT 50 + number, {} from numbers(50000)".format(
                i, i
            )
        )

    instance.restart_clickhouse()
    check_several_tables_are_synchronized(instance, NUM_TABLES)


def test_replica_identity_index(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()

    create_postgres_table(
        cursor, "postgresql_replica", template=postgres_table_template_3
    )
    cursor.execute("CREATE unique INDEX idx on postgresql_replica(key1, key2);")
    cursor.execute("ALTER TABLE postgresql_replica REPLICA IDENTITY USING INDEX idx")
    instance.query(
        "INSERT INTO postgres_database.postgresql_replica SELECT number, number, number, number from numbers(50, 10)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    instance.query(
        "INSERT INTO postgres_database.postgresql_replica SELECT number, number, number, number from numbers(100, 10)"
    )
    check_tables_are_synchronized(instance, "postgresql_replica", order_by="key1")

    cursor.execute("UPDATE postgresql_replica SET key1=key1-25 WHERE key1<100 ")
    cursor.execute("UPDATE postgresql_replica SET key2=key2-25 WHERE key2>100 ")
    cursor.execute("UPDATE postgresql_replica SET value1=value1+100 WHERE key1<100 ")
    cursor.execute("UPDATE postgresql_replica SET value2=value2+200 WHERE key2>100 ")
    check_tables_are_synchronized(instance, "postgresql_replica", order_by="key1")

    cursor.execute("DELETE FROM postgresql_replica WHERE key2<75;")
    check_tables_are_synchronized(instance, "postgresql_replica", order_by="key1")


def test_table_schema_changes(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()
    NUM_TABLES = 5

    for i in range(NUM_TABLES):
        create_postgres_table(
            cursor,
            "postgresql_replica_{}".format(i),
            template=postgres_table_template_2,
        )
        instance.query(
            "INSERT INTO postgres_database.postgresql_replica_{} SELECT number, {}, {}, {} from numbers(25)".format(
                i, i, i, i
            )
        )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=["materialized_postgresql_allow_automatic_update = 1"],
    )

    for i in range(NUM_TABLES):
        instance.query(
            "INSERT INTO postgres_database.postgresql_replica_{} SELECT 25 + number, {}, {}, {} from numbers(25)".format(
                i, i, i, i
            )
        )

    check_several_tables_are_synchronized(instance, NUM_TABLES)

    expected = instance.query(
        "SELECT key, value1, value3 FROM test_database.postgresql_replica_3 ORDER BY key"
    )

    altered_idx = random.randint(0, 4)
    altered_table = f"postgresql_replica_{altered_idx}"
    cursor.execute(f"ALTER TABLE {altered_table} DROP COLUMN value2")

    for i in range(NUM_TABLES):
        cursor.execute(f"INSERT INTO postgresql_replica_{i} VALUES (50, {i}, {i})")
        cursor.execute(f"UPDATE {altered_table} SET value3 = 12 WHERE key%2=0")

    time.sleep(2)
    assert_nested_table_is_created(instance, altered_table)
    assert_number_of_columns(instance, 3, altered_table)
    check_tables_are_synchronized(instance, altered_table)
    print("check1 OK")

    check_several_tables_are_synchronized(instance, NUM_TABLES)

    for i in range(NUM_TABLES):
        if i != altered_idx:
            instance.query(
                "INSERT INTO postgres_database.postgresql_replica_{} SELECT 51 + number, {}, {}, {} from numbers(49)".format(
                    i, i, i, i
                )
            )
        else:
            instance.query(
                "INSERT INTO postgres_database.postgresql_replica_{} SELECT 51 + number, {}, {} from numbers(49)".format(
                    i, i, i
                )
            )

    check_tables_are_synchronized(instance, altered_table)
    print("check2 OK")
    check_several_tables_are_synchronized(instance, NUM_TABLES)


def test_many_concurrent_queries(started_cluster):
    query_pool = [
        "DELETE FROM postgresql_replica_{} WHERE (value*value) % 3 = 0;",
        "UPDATE postgresql_replica_{} SET value = value - 125 WHERE key % 2 = 0;",
        "DELETE FROM postgresql_replica_{} WHERE key % 10 = 0;",
        "UPDATE postgresql_replica_{} SET value = value*5 WHERE key % 2 = 1;",
        "DELETE FROM postgresql_replica_{} WHERE value % 2 = 0;",
        "UPDATE postgresql_replica_{} SET value = value + 2000 WHERE key % 5 = 0;",
        "DELETE FROM postgresql_replica_{} WHERE value % 3 = 0;",
        "UPDATE postgresql_replica_{} SET value = value * 2 WHERE key % 3 = 0;",
        "DELETE FROM postgresql_replica_{} WHERE value % 9 = 2;",
        "UPDATE postgresql_replica_{} SET value = value + 2  WHERE key % 3 = 1;",
        "DELETE FROM postgresql_replica_{} WHERE value%5 = 0;",
    ]

    NUM_TABLES = 5

    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()
    pg_manager.create_and_fill_postgres_tables_from_cursor(
        cursor, NUM_TABLES, numbers=10000
    )

    def attack(thread_id):
        print("thread {}".format(thread_id))
        k = 10000
        for i in range(20):
            query_id = random.randrange(0, len(query_pool) - 1)
            table_id = random.randrange(0, 5)  # num tables

            # random update / delete query
            cursor.execute(query_pool[query_id].format(table_id))
            print("table {} query {} ok".format(table_id, query_id))

            # allow some thread to do inserts (not to violate key constraints)
            if thread_id < 5:
                print("try insert table {}".format(thread_id))
                instance.query(
                    "INSERT INTO postgres_database.postgresql_replica_{} SELECT {}*10000*({} +  number), number from numbers(1000)".format(
                        i, thread_id, k
                    )
                )
                k += 1
                print("insert table {} ok".format(thread_id))

                if i == 5:
                    # also change primary key value
                    print("try update primary key {}".format(thread_id))
                    cursor.execute(
                        "UPDATE postgresql_replica_{} SET key=key%100000+100000*{} WHERE key%{}=0".format(
                            thread_id, i + 1, i + 1
                        )
                    )
                    print("update primary key {} ok".format(thread_id))

    n = [10000]

    threads = []
    threads_num = 16
    for i in range(threads_num):
        threads.append(threading.Thread(target=attack, args=(i,)))

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    n[0] = 50000
    for table_id in range(NUM_TABLES):
        n[0] += 1
        instance.query(
            "INSERT INTO postgres_database.postgresql_replica_{} SELECT {} +  number, number from numbers(5000)".format(
                table_id, n[0]
            )
        )
        # cursor.execute("UPDATE postgresql_replica_{} SET key=key%100000+100000*{} WHERE key%{}=0".format(table_id, table_id+1, table_id+1))

    for thread in threads:
        thread.join()

    for i in range(NUM_TABLES):
        check_tables_are_synchronized(instance, "postgresql_replica_{}".format(i))
        count1 = instance.query(
            "SELECT count() FROM postgres_database.postgresql_replica_{}".format(i)
        )
        count2 = instance.query(
            "SELECT count() FROM (SELECT * FROM test_database.postgresql_replica_{})".format(
                i
            )
        )
        assert int(count1) == int(count2)
        print(count1, count2)


def test_single_transaction(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
        auto_commit=False,
    )
    cursor = conn.cursor()

    table_name = "postgresql_replica_0"
    create_postgres_table(cursor, table_name)
    conn.commit()

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    assert_nested_table_is_created(instance, table_name)

    for query in queries:
        print("query {}".format(query))
        cursor.execute(query.format(0))

    time.sleep(5)
    result = instance.query(f"select count() from test_database.{table_name}")
    # no commit yet
    assert int(result) == 0

    conn.commit()
    check_tables_are_synchronized(instance, table_name)


def test_virtual_columns(started_cluster):
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
    )
    cursor = conn.cursor()
    table_name = "postgresql_replica_0"
    create_postgres_table(cursor, table_name)

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=["materialized_postgresql_allow_automatic_update = 1"],
    )

    assert_nested_table_is_created(instance, table_name)
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number from numbers(10)"
    )
    check_tables_are_synchronized(instance, table_name)

    # just check that it works, no check with `expected` because _version is taken as LSN, which will be different each time.
    result = instance.query(
        f"SELECT key, value, _sign, _version FROM test_database.{table_name};"
    )
    print(result)

    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN value2 integer")
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number, number from numbers(10, 10)"
    )
    assert_number_of_columns(instance, 3, table_name)
    check_tables_are_synchronized(instance, table_name)

    result = instance.query(
        "SELECT key, value, value2,  _sign, _version FROM test_database.postgresql_replica_0;"
    )
    print(result)

    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number, number from numbers(20, 10)"
    )
    check_tables_are_synchronized(instance, table_name)

    result = instance.query(
        f"SELECT key, value, value2,  _sign, _version FROM test_database.{table_name};"
    )
    print(result)


def test_multiple_databases(started_cluster):
    NUM_TABLES = 5
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=False,
    )
    cursor = conn.cursor()
    pg_manager.create_postgres_db(cursor, "postgres_database_1")
    pg_manager.create_postgres_db(cursor, "postgres_database_2")

    conn1 = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
        database_name="postgres_database_1",
    )
    conn2 = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
        database_name="postgres_database_2",
    )

    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()

    pg_manager.create_clickhouse_postgres_db(
        cluster.postgres_ip,
        cluster.postgres_port,
        "postgres_database_1",
        "postgres_database_1",
    )
    pg_manager.create_clickhouse_postgres_db(
        cluster.postgres_ip,
        cluster.postgres_port,
        "postgres_database_2",
        "postgres_database_2",
    )

    cursors = [cursor1, cursor2]
    for cursor_id in range(len(cursors)):
        for i in range(NUM_TABLES):
            table_name = "postgresql_replica_{}".format(i)
            create_postgres_table(cursors[cursor_id], table_name)
            instance.query(
                "INSERT INTO postgres_database_{}.{} SELECT number, number from numbers(50)".format(
                    cursor_id + 1, table_name
                )
            )
    print(
        "database 1 tables: ",
        instance.query(
            """SELECT name FROM system.tables WHERE database = 'postgres_database_1';"""
        ),
    )
    print(
        "database 2 tables: ",
        instance.query(
            """SELECT name FROM system.tables WHERE database = 'postgres_database_2';"""
        ),
    )

    pg_manager.create_materialized_db(
        started_cluster.postgres_ip,
        started_cluster.postgres_port,
        "test_database_1",
        "postgres_database_1",
    )
    pg_manager.create_materialized_db(
        started_cluster.postgres_ip,
        started_cluster.postgres_port,
        "test_database_2",
        "postgres_database_2",
    )

    cursors = [cursor1, cursor2]
    for cursor_id in range(len(cursors)):
        for i in range(NUM_TABLES):
            table_name = "postgresql_replica_{}".format(i)
            instance.query(
                "INSERT INTO postgres_database_{}.{} SELECT 50 + number, number from numbers(50)".format(
                    cursor_id + 1, table_name
                )
            )

    for cursor_id in range(len(cursors)):
        for i in range(NUM_TABLES):
            table_name = "postgresql_replica_{}".format(i)
            check_tables_are_synchronized(
                instance,
                table_name,
                "key",
                "postgres_database_{}".format(cursor_id + 1),
                "test_database_{}".format(cursor_id + 1),
            )


def test_concurrent_transactions(started_cluster):
    def transaction(thread_id):
        conn = get_postgres_conn(
            ip=started_cluster.postgres_ip,
            port=started_cluster.postgres_port,
            database=True,
            auto_commit=False,
        )
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query.format(thread_id))
            print("thread {}, query {}".format(thread_id, query))
        conn.commit()

    NUM_TABLES = 6
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES, numbers=0)

    threads = []
    threads_num = 6
    for i in range(threads_num):
        threads.append(threading.Thread(target=transaction, args=(i,)))

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    for thread in threads:
        time.sleep(random.uniform(0, 0.5))
        thread.start()

    for thread in threads:
        thread.join()

    for i in range(NUM_TABLES):
        check_tables_are_synchronized(instance, f"postgresql_replica_{i}")
        count1 = instance.query(
            f"SELECT count() FROM postgres_database.postgresql_replica_{i}"
        )
        count2 = instance.query(
            f"SELECT count() FROM (SELECT * FROM test_database.postgresql_replica_{i})"
        )
        print(int(count1), int(count2), sep=" ")
        assert int(count1) == int(count2)


def test_abrupt_connection_loss_while_heavy_replication(started_cluster):
    def transaction(thread_id):
        if thread_id % 2:
            conn = get_postgres_conn(
                ip=started_cluster.postgres_ip,
                port=started_cluster.postgres_port,
                database=True,
                auto_commit=True,
            )
        else:
            conn = get_postgres_conn(
                ip=started_cluster.postgres_ip,
                port=started_cluster.postgres_port,
                database=True,
                auto_commit=False,
            )
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query.format(thread_id))
            print("thread {}, query {}".format(thread_id, query))
        if thread_id % 2 == 0:
            conn.commit()

    NUM_TABLES = 6
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES, numbers=0)

    threads_num = 6
    threads = []
    for i in range(threads_num):
        threads.append(threading.Thread(target=transaction, args=(i,)))

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    for thread in threads:
        time.sleep(random.uniform(0, 0.5))
        thread.start()

    for thread in threads:
        thread.join()  # Join here because it takes time for data to reach wal

    time.sleep(2)
    started_cluster.pause_container("postgres1")

    # for i in range(NUM_TABLES):
    #     result = instance.query(f"SELECT count() FROM test_database.postgresql_replica_{i}")
    #     print(result) # Just debug

    started_cluster.unpause_container("postgres1")
    check_several_tables_are_synchronized(instance, NUM_TABLES)


def test_drop_database_while_replication_startup_not_finished(started_cluster):
    NUM_TABLES = 5
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES, 100000)
    for i in range(6):
        pg_manager.create_materialized_db(
            ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
        )
        time.sleep(0.5 * i)
        pg_manager.drop_materialized_db()


def test_restart_server_while_replication_startup_not_finished(started_cluster):
    NUM_TABLES = 5
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES, 100000)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    time.sleep(1)
    instance.restart_clickhouse()
    check_several_tables_are_synchronized(instance, NUM_TABLES)


def test_abrupt_server_restart_while_heavy_replication(started_cluster):
    def transaction(thread_id):
        if thread_id % 2:
            conn = get_postgres_conn(
                ip=started_cluster.postgres_ip,
                port=started_cluster.postgres_port,
                database=True,
                auto_commit=True,
            )
        else:
            conn = get_postgres_conn(
                ip=started_cluster.postgres_ip,
                port=started_cluster.postgres_port,
                database=True,
                auto_commit=False,
            )
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query.format(thread_id))
            print("thread {}, query {}".format(thread_id, query))
        if thread_id % 2 == 0:
            conn.commit()

    NUM_TABLES = 6
    pg_manager.create_and_fill_postgres_tables(tables_num=NUM_TABLES, numbers=0)

    threads = []
    threads_num = 6
    for i in range(threads_num):
        threads.append(threading.Thread(target=transaction, args=(i,)))

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )

    for thread in threads:
        time.sleep(random.uniform(0, 0.5))
        thread.start()

    for thread in threads:
        thread.join()  # Join here because it takes time for data to reach wal

    instance.restart_clickhouse()
    check_several_tables_are_synchronized(instance, NUM_TABLES)


def test_quoting_1(started_cluster):
    table_name = "user"
    pg_manager.create_and_fill_postgres_table(table_name)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_tables_are_synchronized(instance, table_name)


def test_quoting_2(started_cluster):
    table_name = "user"
    pg_manager.create_and_fill_postgres_table(table_name)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[f"materialized_postgresql_tables_list = '{table_name}'"],
    )
    check_tables_are_synchronized(instance, table_name)


def test_user_managed_slots(started_cluster):
    slot_name = "user_slot"
    table_name = "test_table"
    pg_manager.create_and_fill_postgres_table(table_name)

    replication_connection = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=True,
        replication=True,
        auto_commit=True,
    )
    snapshot = create_replication_slot(replication_connection, slot_name=slot_name)

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            f"materialized_postgresql_replication_slot = '{slot_name}'",
            f"materialized_postgresql_snapshot = '{snapshot}'",
        ],
    )
    check_tables_are_synchronized(instance, table_name)

    instance.query(
        "INSERT INTO postgres_database.{} SELECT number, number from numbers(10000, 10000)".format(
            table_name
        )
    )
    check_tables_are_synchronized(instance, table_name)

    instance.restart_clickhouse()

    instance.query(
        "INSERT INTO postgres_database.{} SELECT number, number from numbers(20000, 10000)".format(
            table_name
        )
    )
    check_tables_are_synchronized(instance, table_name)

    pg_manager.drop_materialized_db()
    drop_replication_slot(replication_connection, slot_name)
    replication_connection.close()


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
