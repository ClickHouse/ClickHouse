import os.path as p
import random
import threading
import time
from random import randrange

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.postgres_utility import (
    PostgresManager,
    assert_nested_table_is_created,
    assert_number_of_columns,
    check_several_tables_are_synchronized,
    check_tables_are_synchronized,
    create_postgres_schema,
    create_postgres_table,
    create_replication_slot,
    drop_postgres_schema,
    drop_postgres_table,
    drop_replication_slot,
    get_postgres_conn,
    postgres_table_template,
    postgres_table_template_2,
    postgres_table_template_3,
    postgres_table_template_4,
    queries,
)
from helpers.test_tools import TSV, assert_eq_with_retry

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
        pg_manager.init(
            instance,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    print("PostgreSQL is available - running test")
    yield  # run test
    pg_manager.restart()


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


def test_multiple_databases(started_cluster):
    NUM_TABLES = 5
    conn = get_postgres_conn(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        database=False,
    )
    pg_manager.create_postgres_db("postgres_database_1")
    pg_manager.create_postgres_db("postgres_database_2")

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
        "postgres_database_1",
        "",
        "postgres_database_1",
    )
    pg_manager.create_clickhouse_postgres_db(
        "postgres_database_2",
        "",
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


    with started_cluster.pause_container("postgres1"):
        # for i in range(NUM_TABLES):
        #     result = instance.query(f"SELECT count() FROM test_database.postgresql_replica_{i}")
        #     print(result) # Just debug
        pass

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
