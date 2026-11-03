import random
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass
from helpers.postgres_utility import (
    PostgresManager,
    assert_nested_table_is_created,
    check_several_tables_are_synchronized,
    check_tables_are_synchronized,
    create_postgres_table,
    create_replication_slot,
    drop_replication_slot,
    get_postgres_conn,
    queries,
)

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
    get_postgres_conn(
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

    with started_cluster.pause_container_using_signal("postgres1"):
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


def test_drop_database_cancels_startup_retries(started_cluster):
    NUM_TABLES = 2
    pg_manager.create_and_fill_postgres_tables(NUM_TABLES, 100)
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_several_tables_are_synchronized(instance, NUM_TABLES)

    retry_message = "Failed to start replication from PostgreSQL"
    failpoint = "database_materialized_postgresql_pause_before_table_drop"

    count_before_restart = int(instance.count_in_log(retry_message))

    with started_cluster.pause_container_using_signal("postgres1"):
        # Restart while PostgreSQL is unreachable: the startup task fails and keeps
        # re-scheduling itself every 5 seconds, while the nested tables already exist.
        instance.restart_clickhouse()
        deadline = time.monotonic() + 120
        while int(instance.count_in_log(retry_message)) == count_before_restart:
            assert time.monotonic() < deadline, "replication startup did not fail"
            time.sleep(0.5)

        instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        drop_thread = threading.Thread(
            target=instance.query, args=("DROP DATABASE test_database SYNC",)
        )
        drop_thread.start()
        try:
            # The drop pauses at the failpoint on the first table, which is reached
            # after `stopReplication` has already run.
            instance.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")

            # `stopReplication` must have cancelled the startup retry task. Hold the
            # drop open across several retry periods (the task re-schedules itself
            # every 5 seconds) and check that no retry fires while the tables are
            # being dropped.
            baseline = int(instance.count_in_log(retry_message))
            time.sleep(20)
            assert int(instance.count_in_log(retry_message)) == baseline
        finally:
            instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            drop_thread.join()


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


def test_bool_and_bool_array(started_cluster):
    """Test for https://github.com/ClickHouse/ClickHouse/issues/62544
    A PostgreSQL `boolean[]` column failed to replicate through
    MaterializedPostgreSQL because the array parser did not understand
    PostgreSQL's 't'/'f' boolean text format. This checks both the initial
    snapshot and the streaming replication path (INSERT and UPDATE), including
    NULL scalars, NULL array elements and NULL arrays. Rows are written directly
    in PostgreSQL so the values arrive over the wire exactly as '{t,f,...}'.
    """
    table_name = "test_bool_array"
    cursor = pg_manager.get_db_cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    cursor.execute(
        f'CREATE TABLE "{table_name}" '
        "(key integer PRIMARY KEY, b boolean, arr boolean[])"
    )
    cursor.execute(
        f'INSERT INTO "{table_name}" VALUES '
        "(1, 't', '{t,t,t,f,f,f,t,t}'), "
        "(2, 'f', '{f,f}'), "
        "(3, NULL, '{t,NULL,f}'), "
        "(4, 't', NULL)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[f"materialized_postgresql_tables_list = '{table_name}'"],
    )

    # Initial snapshot.
    check_tables_are_synchronized(instance, table_name)
    assert (
        instance.query(f"SELECT * FROM test_database.{table_name} ORDER BY key")
        == "1\t1\t[1,1,1,0,0,0,1,1]\n"
        "2\t0\t[0,0]\n"
        "3\t\\N\t[1,NULL,0]\n"
        "4\t1\t[]\n"
    )

    # Streaming replication: INSERT and UPDATE performed directly in PostgreSQL.
    cursor.execute(f"""INSERT INTO "{table_name}" VALUES (5, 'f', '{{f,NULL,t,f}}')""")
    cursor.execute(
        f"""UPDATE "{table_name}" SET b = 'f', arr = '{{t,t,t}}' WHERE key = 1"""
    )

    check_tables_are_synchronized(instance, table_name)
    assert (
        instance.query(f"SELECT * FROM test_database.{table_name} ORDER BY key")
        == "1\t0\t[1,1,1]\n"
        "2\t0\t[0,0]\n"
        "3\t\\N\t[1,NULL,0]\n"
        "4\t1\t[]\n"
        "5\t0\t[0,NULL,1,0]\n"
    )

    pg_manager.drop_materialized_db()


def test_merge_table_over_materialized_postgresql(started_cluster):
    """
    Reading a MaterializedPostgreSQL table through Merge forces FINAL on the child read
    """
    table_name = "postgresql_replica_final"
    pg_manager.create_postgres_table(table_name)
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number FROM numbers(3)"
    )

    instance.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    instance.query(
        f"""
        CREATE TABLE {table_name} (key Int32, value Int32)
        ENGINE=MaterializedPostgreSQL('{started_cluster.postgres_ip}:{started_cluster.postgres_port}', 'postgres_database', '{table_name}', 'postgres', '{pg_pass}') ORDER BY key
        """
    )

    try:
        check_tables_are_synchronized(
            instance, table_name, materialized_database="default"
        )

        # Stop merges so the nested ReplacingMergeTree keeps both versions of the
        # updated row and the read has to deduplicate them with FINAL.
        instance.query(f"SYSTEM STOP MERGES {table_name}")
        pg_manager.execute(f"UPDATE {table_name} SET value = 42 WHERE key = 1")

        check_tables_are_synchronized(
            instance, table_name, materialized_database="default"
        )

        expected = "0\t0\n1\t42\n2\t2\n"
        direct_query = f"SELECT key, value FROM {table_name} ORDER BY key, value"
        merge_query = (
            f"SELECT key, value FROM merge('default', '^{table_name}$')"
            " ORDER BY key, value"
        )

        for query in [direct_query, merge_query]:
            explain = instance.query(f"EXPLAIN actions=1 {query}")
            assert "FINAL: 1" in explain, explain

        assert instance.query(direct_query) == expected
        assert instance.query(merge_query) == expected
    finally:
        instance.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_merge_table_over_materialized_postgresql_database(started_cluster):
    """
    Reading a MaterializedPostgreSQL database through Merge forces FINAL on the child read:
    Merge maps every table it enumerates through getTableForRead, which returns the
    StorageMaterializedPostgreSQL wrapper instead of the nested ReplacingMergeTree table, so
    that Merge over the database reads with the forced FINAL and the `_sign = 1` filter
    (otherwise stale and deleted row versions would be exposed).
    """
    table_name = "postgresql_replica_final_db"
    pg_manager.create_postgres_table(table_name)
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number FROM numbers(3)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_tables_are_synchronized(instance, table_name)

    pg_manager.execute(f"UPDATE {table_name} SET value = 42 WHERE key = 1")
    pg_manager.execute(f"DELETE FROM {table_name} WHERE key = 2")
    check_tables_are_synchronized(instance, table_name)

    expected = "0\t0\n1\t42\n"
    direct_query = (
        f"SELECT key, value FROM test_database.{table_name} ORDER BY key, value"
    )
    merge_query = (
        f"SELECT key, value FROM merge('test_database', '^{table_name}$')"
        " ORDER BY key, value"
    )

    for query in [direct_query, merge_query]:
        explain = instance.query(f"EXPLAIN actions=1 {query}")
        assert "FINAL: 1" in explain, explain

    assert instance.query(direct_query) == expected
    assert instance.query(merge_query) == expected


def test_reads_during_startup_window_use_final(started_cluster):
    """
    Right after a server restart (and right after CREATE / ATTACH DATABASE) the map of
    StorageMaterializedPostgreSQL wrappers is empty until startSynchronization has fetched the
    tables list from PostgreSQL and published the wrappers. In that window tryGetTable and
    getTableForRead used to fall back to the nested ReplacingMergeTree tables, so user-facing
    reads bypassed the forced FINAL and the `_sign = 1` filter and exposed stale and deleted row
    versions. Now the nested tables are wrapped on the fly.

    The window is held open deterministically: PostgreSQL is paused, so after the restart the
    synchronization keeps failing and retrying and can never publish the wrappers.
    """
    table_name = "postgresql_replica_startup_window"
    pg_manager.create_postgres_table(table_name)
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number FROM numbers(3)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_tables_are_synchronized(instance, table_name)

    pg_manager.execute(f"UPDATE {table_name} SET value = 42 WHERE key = 1")
    pg_manager.execute(f"DELETE FROM {table_name} WHERE key = 2")
    check_tables_are_synchronized(instance, table_name)

    expected = "0\t0\n1\t42\n"
    direct_query = (
        f"SELECT key, value FROM test_database.{table_name} ORDER BY key, value"
    )
    merge_query = (
        f"SELECT key, value FROM merge('test_database', '^{table_name}$')"
        " ORDER BY key, value"
    )

    with started_cluster.pause_container_using_signal("postgres1"):
        instance.restart_clickhouse()

        # The table is visible while synchronization has not finished (it cannot finish -
        # PostgreSQL is paused), and reads go through the wrapper: FINAL is forced and the
        # deleted row (key = 2) and the stale version (key = 1, value = 1) are filtered out.
        assert (
            table_name
            in instance.query("SHOW TABLES FROM test_database").strip().split("\n")
        )

        for query in [direct_query, merge_query]:
            explain = instance.query(f"EXPLAIN actions=1 {query}")
            assert "FINAL: 1" in explain, explain

        assert instance.query(direct_query) == expected
        assert instance.query(merge_query) == expected

    # Once PostgreSQL is reachable again, synchronization catches up and reads keep working.
    check_tables_are_synchronized(instance, table_name)
    assert instance.query(direct_query) == expected


def test_database_introspection_sees_nested_tables(started_cluster):
    """
    `Merge` needs the `StorageMaterializedPostgreSQL` wrappers, but generic enumeration must keep
    exposing the physical nested `ReplacingMergeTree` tables: a wrapper has no Atomic UUID and is
    not `MergeTreeData`, so returning wrappers from `getTablesIterator` would make
    `system.tables.uuid` empty and would drop these tables out of `system.parts` and out of the
    asynchronous table metrics. Only the reading path goes through `getTableForRead`.
    """
    table_name = "postgresql_replica_introspection"
    pg_manager.create_postgres_table(table_name)
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number FROM numbers(10)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_tables_are_synchronized(instance, table_name)

    # `system.tables` keeps reporting a real Atomic UUID for the table.
    uuid = instance.query(
        f"SELECT uuid FROM system.tables WHERE database = 'test_database' AND name = '{table_name}'"
    ).strip()
    assert uuid != "00000000-0000-0000-0000-000000000000", uuid

    # `system.parts` enumerates databases through `getTablesIterator` and keeps only the storages
    # that are `MergeTreeData`, so the nested table has to stay visible there. The very same
    # `dynamic_cast<MergeTreeData *>` over the iterator is what `ServerAsynchronousMetrics` uses to
    # account for the parts, rows and bytes of every table.
    assert (
        int(
            instance.query(
                "SELECT sum(rows) FROM system.parts "
                f"WHERE database = 'test_database' AND table = '{table_name}' AND active"
            )
        )
        == 10
    )

    # Reading still goes through the wrapper.
    explain = instance.query(
        f"EXPLAIN actions=1 SELECT key, value FROM test_database.{table_name}"
    )
    assert "FINAL: 1" in explain, explain


def test_merge_keeps_child_table_capabilities(started_cluster):
    """
    `Merge` discovers the capabilities and the statistics of its children through the very same
    enumeration that it reads through, so every one of those checks now sees the
    `StorageMaterializedPostgreSQL` wrapper. The wrapper hands the query over to the nested
    `ReplacingMergeTree` untouched, so it has to report what the nested table supports - otherwise
    a `Merge` over a `MaterializedPostgreSQL` database silently loses `PREWHERE` and the size
    estimates.
    """
    table_name = "postgresql_replica_capabilities"
    pg_manager.create_postgres_table(table_name)
    instance.query(
        f"INSERT INTO postgres_database.{table_name} SELECT number, number FROM numbers(10)"
    )

    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
    )
    check_tables_are_synchronized(instance, table_name)

    instance.query("DROP TABLE IF EXISTS merge_over_matpg")
    # The columns are inferred from the source table, so that they match its types exactly:
    # `Merge` drops a column out of `supportedPrewhereColumns` when its type differs.
    instance.query(
        f"CREATE TABLE merge_over_matpg ENGINE = Merge('test_database', '^{table_name}$')"
    )

    # An explicit PREWHERE is rejected outright when the storage does not support it.
    assert (
        instance.query("SELECT key FROM merge_over_matpg PREWHERE value = 5") == "5\n"
    )

    # A subcolumn is admitted into PREWHERE through its origin column only when every child
    # reports `supportedPrewhereColumnsIncludeSubcolumns` - `Merge` ANDs that bit across its
    # children, and it is rejected with ILLEGAL_PREWHERE otherwise. The pg `value` column is
    # `Nullable(Int32)`, so `value.null` is its subcolumn.
    assert (
        instance.query(
            "SELECT count() FROM merge_over_matpg PREWHERE value.null = 0"
        ).strip()
        == "10"
    )

    # Subcolumns are readable through the wrapper directly as well.
    assert (
        instance.query(
            f"SELECT count() FROM test_database.{table_name} WHERE value.null = 0"
        ).strip()
        == "10"
    )

    # `totalRows` and `totalBytes` of the nested table are what fills these in.
    total_rows, total_bytes = (
        instance.query(
            "SELECT total_rows, total_bytes FROM system.tables WHERE name = 'merge_over_matpg'"
        )
        .strip()
        .split("\t")
    )
    assert int(total_rows) == 10, total_rows
    assert int(total_bytes) > 0, total_bytes

    instance.query("DROP TABLE merge_over_matpg")


def test_drop_database_while_enumerating_tables(started_cluster):
    """
    `DROP DATABASE` clears the map of `StorageMaterializedPostgreSQL` wrappers that
    `tryGetTable` and `getTableForRead` look into, so both sides have to hold the same mutex.
    Otherwise the drop destroys a wrapper while a reader is still dereferencing it, which is a
    use-after-free that the sanitizer builds turn into an aborted server.

    `ServerAsynchronousMetrics` enumerates the tables of every database once per second, so this
    used to be hit by the metrics thread rather than by a query. Here `system.tables` is read in a
    loop instead, to drive the same code path from the test itself.
    """
    table_names = [f"postgresql_replica_drop_race_{i}" for i in range(2)]
    for table_name in table_names:
        pg_manager.create_postgres_table(table_name)
        instance.query(
            f"INSERT INTO postgres_database.{table_name} SELECT number, number FROM numbers(10)"
        )

    stop_reading = threading.Event()

    def keep_enumerating_tables():
        while not stop_reading.is_set():
            for query in [
                "SELECT count() FROM system.tables WHERE database = 'test_database'",
                # This one goes through the wrapper map: `Merge` maps every enumerated table
                # through `getTableForRead`, which looks the wrappers up under `tables_mutex`.
                "SELECT count() FROM merge('test_database', '^postgresql_replica_drop_race_')",
            ]:
                try:
                    instance.query(query)
                except Exception:
                    # The database being dropped from under the query is expected. What must
                    # not happen is the server going away, which the assertions below check
                    # for.
                    pass

    readers = [threading.Thread(target=keep_enumerating_tables) for _ in range(4)]
    for reader in readers:
        reader.start()

    try:
        for _ in range(10):
            pg_manager.create_materialized_db(
                ip=started_cluster.postgres_ip, port=started_cluster.postgres_port
            )
            for table_name in table_names:
                check_tables_are_synchronized(instance, table_name)
            pg_manager.drop_materialized_db()
    finally:
        stop_reading.set()
        for reader in readers:
            reader.join()

    # The server is still up - it did not abort on a sanitizer report.
    assert instance.query("SELECT 1") == "1\n"


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
