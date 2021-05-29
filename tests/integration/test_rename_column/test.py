from __future__ import print_function

import time
import random
import pytest

from multiprocessing.dummy import Pool
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager
from helpers.test_tools import TSV

node_options = dict(
    with_zookeeper=True,
    main_configs=['configs/remote_servers.xml'],
    config_dir='configs',
    tmpfs=['/external:size=200M', '/internal:size=1M'])

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', macros={"shard": 0, "replica": 1}, **node_options)
node2 = cluster.add_instance('node2', macros={"shard": 0, "replica": 2}, **node_options)
node3 = cluster.add_instance('node3', macros={"shard": 1, "replica": 1}, **node_options)
node4 = cluster.add_instance('node4', macros={"shard": 1, "replica": 2}, **node_options)
nodes = [node1, node2, node3, node4]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))
    time.sleep(1)


def create_table(nodes, table_name, with_storage_policy=False, with_time_column=False,
                 with_ttl_move=False, with_ttl_delete=False):
    extra_columns = ""
    settings = []

    for node in nodes:
        sql = """
            CREATE TABLE {table_name}
                (
                    num UInt32,
                    num2 UInt32 DEFAULT num + 1{extra_columns}
                )
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table_name}', '{replica}')
                ORDER BY num PARTITION BY num % 100
            """
        if with_ttl_move:
            sql += """
                TTL time + INTERVAL (num2 % 1) SECOND TO DISK 'external'
            """
        if with_ttl_delete:
            sql += """
                TTL time + INTERVAL (num2 % 1) SECOND DELETE
            """
            settings.append("merge_with_ttl_timeout = 1")

        if with_storage_policy:
            settings.append("storage_policy='default_with_external'")

        if settings:
            sql += """
                SETTINGS {}
            """.format(", ".join(settings))

        if with_time_column:
            extra_columns = """,
                    time DateTime
                """
        node.query(sql.format(table_name=table_name, replica=node.name, extra_columns=extra_columns))


def create_distributed_table(node, table_name):
        sql = """
            CREATE TABLE %(table_name)s_replicated ON CLUSTER test_cluster
            (
                num UInt32,
                num2 UInt32 DEFAULT num + 1
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/%(table_name)s_replicated', '{replica}')
            ORDER BY num PARTITION BY num %% 100;
        """ % dict(table_name=table_name)
        node.query(sql)
        sql = """
            CREATE TABLE %(table_name)s ON CLUSTER test_cluster AS %(table_name)s_replicated
            ENGINE = Distributed(test_cluster, default, %(table_name)s_replicated, rand())
        """ % dict(table_name=table_name)
        node.query(sql)


def drop_distributed_table(node, table_name):
    node.query("DROP TABLE IF EXISTS {} ON CLUSTER test_cluster".format(table_name))
    node.query("DROP TABLE IF EXISTS {}_replicated ON CLUSTER test_cluster".format(table_name))
    time.sleep(1)


def insert(node, table_name, chunk=1000, col_names=None, iterations=1, ignore_exception=False,
        slow=False, with_many_parts=False, offset=0, with_time_column=False):
    if col_names is None:
        col_names = ['num', 'num2']
    for i in range(iterations):
        try:
            query = ["SET max_partitions_per_insert_block = 10000000"]
            if with_many_parts:
                query.append("SET max_insert_block_size = 64")
            if with_time_column:
                query.append(
                    "INSERT INTO {table_name} ({col0}, {col1}, time) SELECT number AS {col0}, number + 1 AS {col1}, now() + 10 AS time FROM numbers_mt({chunk})"
                .format(table_name=table_name, chunk=chunk, col0=col_names[0], col1=col_names[1]))
            elif slow:
                query.append(
                    "INSERT INTO {table_name} ({col0}, {col1}) SELECT number + sleepEachRow(0.001) AS {col0}, number + 1 AS {col1} FROM numbers_mt({chunk})"
                .format(table_name=table_name, chunk=chunk, col0=col_names[0], col1=col_names[1]))
            else:
                query.append(
                    "INSERT INTO {table_name} ({col0},{col1}) SELECT number + {offset} AS {col0}, number + 1 + {offset} AS {col1} FROM numbers_mt({chunk})"
                .format(table_name=table_name, chunk=chunk, col0=col_names[0], col1=col_names[1], offset=str(offset)))
            node.query(";\n".join(query))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise


def select(node, table_name, col_name="num", expected_result=None, iterations=1, ignore_exception=False, slow=False, poll=None):
    for i in range(iterations):
        start_time = time.time()
        while True:
            try:
                if slow:
                    r = node.query("SELECT count() FROM (SELECT num2, sleepEachRow(0.5) FROM {} WHERE {} % 1000 > 0)".format(table_name, col_name))
                else:
                    r = node.query("SELECT count() FROM {} WHERE {} % 1000 > 0".format(table_name, col_name))
                if expected_result:
                    if r != expected_result and poll and time.time() - start_time < poll:
                        continue
                    assert r == expected_result
            except QueryRuntimeException as ex:
                if not ignore_exception:
                    raise
            break


def rename_column(node, table_name, name, new_name, iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            node.query("ALTER TABLE {table_name} RENAME COLUMN {name} to {new_name}".format(
                table_name=table_name, name=name, new_name=new_name
            ))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise


def rename_column_on_cluster(node, table_name, name, new_name, iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            node.query("ALTER TABLE {table_name} ON CLUSTER test_cluster RENAME COLUMN {name} to {new_name}".format(
                table_name=table_name, name=name, new_name=new_name
            ))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise


def alter_move(node, table_name, iterations=1, ignore_exception=False):
    for i in range(iterations):
        move_part = random.randint(0, 99)
        move_volume = 'external'
        try:
            node.query("ALTER TABLE {table_name} MOVE PARTITION '{move_part}' TO VOLUME '{move_volume}'"
                .format(table_name=table_name, move_part=move_part, move_volume=move_volume))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise


def test_rename_parallel_same_node(started_cluster):
    table_name = "test_rename_parallel_same_node"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node1, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node1, table_name, "foo3", "num2", 5, True)))
        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "999\n")
    finally:
        drop_table(nodes, table_name)


def test_rename_parallel(started_cluster):
    table_name = "test_rename_parallel"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 5, True)))
        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "999\n")
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_select(started_cluster):
    table_name = "test_rename_with_parallel_select"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)

        select(node1, table_name, "num2", "999\n", poll=30)
        select(node2, table_name, "num2", "999\n", poll=30)
        select(node3, table_name, "num2", "999\n", poll=30)

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 5, True)))
            tasks.append(p.apply_async(select, (node1, table_name, "foo3", "999\n", 5, True)))
            tasks.append(p.apply_async(select, (node2, table_name, "num2", "999\n", 5, True)))
            tasks.append(p.apply_async(select, (node3, table_name, "foo2", "999\n", 5, True)))
        for task in tasks:
            task.get(timeout=240)

        # rename column back to original name
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "999\n")
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_insert(started_cluster):
    table_name = "test_rename_with_parallel_insert"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 5, True)))
            tasks.append(p.apply_async(insert, (node1, table_name, 100, ["num", "foo3"], 5, True)))
            tasks.append(p.apply_async(insert, (node2, table_name, 100, ["num", "num2"], 5, True)))
            tasks.append(p.apply_async(insert, (node3, table_name, 100, ["num", "foo2"], 5, True)))
        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2")
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_merges(started_cluster):
    table_name = "test_rename_with_parallel_merges"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        for i in range(20):
            insert(node1, table_name, 100, ["num","num2"], 1, False, False, True, offset=i*100)

        def merge_parts(node, table_name, iterations=1):
            for i in range(iterations):
                node.query("OPTIMIZE TABLE %s FINAL" % table_name)

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 5, True)))
            tasks.append(p.apply_async(merge_parts, (node1, table_name, 5)))
            tasks.append(p.apply_async(merge_parts, (node2, table_name, 5)))
            tasks.append(p.apply_async(merge_parts, (node3, table_name, 5)))

        for task in tasks:
            task.get(timeout=240)

        # rename column back to the original name
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "1998\n")
        select(node2, table_name, "num2", "1998\n")
        select(node3, table_name, "num2", "1998\n")
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_slow_insert(started_cluster):
    table_name = "test_rename_with_parallel_slow_insert"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []
        tasks.append(p.apply_async(insert, (node1, table_name, 10000, ["num", "num2"], 1, False, True)))
        tasks.append(p.apply_async(insert, (node1, table_name, 10000, ["num", "num2"], 1, True, True))) # deduplicated
        time.sleep(0.5)
        tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2")))

        for task in tasks:
            task.get(timeout=240)

        insert(node1, table_name, 100, ["num", "foo2"])

        # rename column back to original
        rename_column(node1, table_name, "foo2", "num2")

        # check that select still works
        select(node1, table_name, "num2", "11089\n")
        select(node2, table_name, "num2", "11089\n", poll=30)
        select(node3, table_name, "num2", "11089\n", poll=30)
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_slow_select(started_cluster):
    table_name = "test_rename_with_parallel_slow_select"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []

        tasks.append(p.apply_async(select, (node1, table_name, "num2", "999\n", 1, True, True)))
        time.sleep(0.5)
        tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2")))

        for task in tasks:
            task.get(timeout=240)

        insert(node1, table_name, 100, ["num", "foo2"])

        # rename column back to original
        rename_column(node1, table_name, "foo2", "num2")

        # check that select still works
        select(node1, table_name, "num2", "1099\n")
        select(node2, table_name, "num2", "1099\n", poll=30)
        select(node3, table_name, "num2", "1099\n", poll=30)
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_moves(started_cluster):
    table_name = "test_rename_with_parallel_moves"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name, with_storage_policy=True)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []

        tasks.append(p.apply_async(alter_move, (node1, table_name, 20, True)))
        tasks.append(p.apply_async(alter_move, (node2, table_name, 20, True)))
        tasks.append(p.apply_async(alter_move, (node3, table_name, 20, True)))
        tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 20, True)))
        tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 20, True)))
        tasks.append(p.apply_async(rename_column, (node3, table_name, "num3", "num2", 20, True)))

        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo2", "num2", 1, True)
        rename_column(node1, table_name, "foo3", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "999\n")
        select(node2, table_name, "num2", "999\n", poll=30)
        select(node3, table_name, "num2", "999\n", poll=30)
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_ttl_move(started_cluster):
    table_name = 'test_rename_with_parallel_ttl_move'
    try:
        create_table(nodes, table_name, with_storage_policy=True, with_time_column=True, with_ttl_move=True)
        rename_column(node1, table_name, "time", "time2", 1, False)
        rename_column(node1, table_name, "time2", "time", 1, False)

        p = Pool(15)
        tasks = []

        tasks.append(p.apply_async(insert, (node1, table_name, 10000, ["num", "num2"], 1, False, False, True, 0, True)))
        time.sleep(5)
        rename_column(node1, table_name, "time", "time2", 1, False)
        time.sleep(4)
        tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 20, True)))
        tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 20, True)))
        tasks.append(p.apply_async(rename_column, (node3, table_name, "num3", "num2", 20, True)))

        for task in tasks:
            task.get(timeout=240)

        # check some parts got moved
        assert "external" in set(node1.query("SELECT disk_name FROM system.parts WHERE table == '{}' AND active=1 ORDER BY modification_time".format(table_name)).strip().splitlines())

        # rename column back to original
        rename_column(node1, table_name, "foo2", "num2", 1, True)
        rename_column(node1, table_name, "foo3", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "9990\n")
    finally:
        drop_table(nodes, table_name)


def test_rename_with_parallel_ttl_delete(started_cluster):
    table_name = 'test_rename_with_parallel_ttl_delete'
    try:
        create_table(nodes, table_name, with_time_column=True, with_ttl_delete=True)
        rename_column(node1, table_name, "time", "time2", 1, False)
        rename_column(node1, table_name, "time2", "time", 1, False)

        def merge_parts(node, table_name, iterations=1):
            for i in range(iterations):
                node.query("OPTIMIZE TABLE {}".format(table_name))

        p = Pool(15)
        tasks = []

        tasks.append(p.apply_async(insert, (node1, table_name, 10000, ["num", "num2"], 1, False, False, True, 0, True)))
        time.sleep(15)
        tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 20, True)))
        tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 20, True)))
        tasks.append(p.apply_async(rename_column, (node3, table_name, "num3", "num2", 20, True)))
        tasks.append(p.apply_async(merge_parts, (node1, table_name, 20)))
        tasks.append(p.apply_async(merge_parts, (node2, table_name, 20)))
        tasks.append(p.apply_async(merge_parts, (node3, table_name, 20)))

        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo2", "num2", 1, True)
        rename_column(node1, table_name, "foo3", "num2", 1, True)

        assert int(node1.query("SELECT count() FROM {}".format(table_name)).strip()) < 10000
    finally:
        drop_table(nodes, table_name)


def test_rename_distributed(started_cluster):
    table_name = 'test_rename_distributed'
    try:
        create_distributed_table(node1, table_name)
        insert(node1, table_name, 1000)

        rename_column_on_cluster(node1, table_name, 'num2', 'foo2')
        rename_column_on_cluster(node1, '%s_replicated' % table_name, 'num2', 'foo2')

        insert(node1, table_name, 1000, col_names=['num','foo2'])

        select(node1, table_name, "foo2", '1998\n', poll=30)
    finally:
        drop_distributed_table(node1, table_name)


def test_rename_distributed_parallel_insert_and_select(started_cluster):
    table_name = 'test_rename_distributed_parallel_insert_and_select'
    try:
        create_distributed_table(node1, table_name)
        insert(node1, table_name, 1000)

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, table_name, 'num2', 'foo2', 3, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, '%s_replicated' % table_name, 'num2', 'foo2', 3, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, table_name, 'foo2', 'foo3', 3, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, '%s_replicated' % table_name, 'foo2', 'foo3', 3, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, table_name, 'foo3', 'num2', 3, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, '%s_replicated' % table_name, 'foo3', 'num2', 3, True)))
            tasks.append(p.apply_async(insert, (node1, table_name, 10, ["num", "foo3"], 5, True)))
            tasks.append(p.apply_async(insert, (node2, table_name, 10, ["num", "num2"], 5, True)))
            tasks.append(p.apply_async(insert, (node3, table_name, 10, ["num", "foo2"], 5, True)))
            tasks.append(p.apply_async(select, (node1, table_name, "foo2", None, 5, True)))
            tasks.append(p.apply_async(select, (node2, table_name, "foo3", None, 5, True)))
            tasks.append(p.apply_async(select, (node3, table_name, "num2", None, 5, True)))
        for task in tasks:
            task.get(timeout=240)

        rename_column_on_cluster(node1, table_name, 'foo2', 'num2', 1, True)
        rename_column_on_cluster(node1, '%s_replicated' % table_name, 'foo2', 'num2', 1, True)
        rename_column_on_cluster(node1, table_name, 'foo3', 'num2', 1, True)
        rename_column_on_cluster(node1, '%s_replicated' % table_name, 'foo3', 'num2', 1, True)

        insert(node1, table_name, 1000, col_names=['num','num2'])
        select(node1, table_name, "num2")
        select(node2, table_name, "num2")
        select(node3, table_name, "num2")
        select(node4, table_name, "num2")
    finally:
        drop_distributed_table(node1, table_name)
