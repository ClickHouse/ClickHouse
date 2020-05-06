from __future__ import print_function

import time
import pytest

from multiprocessing.dummy import Pool
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager
from helpers.test_tools import TSV

main_configs=['configs/remote_servers.xml']

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True, macros={"shard": 0, "replica": 1}, main_configs=main_configs )
node2 = cluster.add_instance('node2', with_zookeeper=True, macros={"shard": 0, "replica": 2}, main_configs=main_configs )
node3 = cluster.add_instance('node3', with_zookeeper=True, macros={"shard": 1, "replica": 1}, main_configs=main_configs )
node4 = cluster.add_instance('node4', with_zookeeper=True, macros={"shard": 1, "replica": 2}, main_configs=main_configs )
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

def create_table(nodes, table_name):
    for node in nodes:
        sql = """
            CREATE TABLE {table_name}
            (
                num UInt32,
                num2 UInt32 DEFAULT num + 1
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table_name}', '{replica}')
            ORDER BY num PARTITION BY num % 100;
        """.format(table_name=table_name, replica=node.name)
        node.query(sql)

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

def insert(node, table_name, chunk=1000, col_names=None, iterations=1, ignore_exception=False, slow=False, with_many_parts=False, offset=0):
    if col_names is None:
        col_names = ['num', 'num2']
    for i in range(iterations):
        try:
            query = ["SET max_partitions_per_insert_block = 10000000"]
            if with_many_parts:
                query.append("SET max_insert_block_size = 64")
            if slow:
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
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 25, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 25, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 25, True)))
            tasks.append(p.apply_async(merge_parts, (node1, table_name, 15)))
            tasks.append(p.apply_async(merge_parts, (node2, table_name, 15)))
            tasks.append(p.apply_async(merge_parts, (node3, table_name, 15)))

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
        time.sleep(0.25)
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
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, table_name, 'num2', 'foo2', 5, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, '%s_replicated' % table_name, 'num2', 'foo2', 5, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, table_name, 'foo2', 'foo3', 5, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, '%s_replicated' % table_name, 'foo2', 'foo3', 5, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, table_name, 'foo3', 'num2', 5, True)))
            tasks.append(p.apply_async(rename_column_on_cluster, (node1, '%s_replicated' % table_name, 'foo3', 'num2', 5, True)))
            tasks.append(p.apply_async(insert, (node1, table_name, 100, ["num", "foo3"], 5, True)))
            tasks.append(p.apply_async(insert, (node2, table_name, 100, ["num", "num2"], 5, True)))
            tasks.append(p.apply_async(insert, (node3, table_name, 100, ["num", "foo2"], 5, True)))
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

