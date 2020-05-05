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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Adding more tests.
=======
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

>>>>>>> Adding test to check rename column when a replicated table has distributed table over it.
def insert(node, table_name, chunk=1000, col_name="num", iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            node.query((
                "SET max_partitions_per_insert_block = 10000000;\n"
                "INSERT INTO {table_name} ({col_name}) SELECT number AS {col_name} FROM numbers_mt({chunk})"
            ).format(table_name=table_name, chunk=chunk, col_name=col_name))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise
<<<<<<< HEAD
=======
def insert(node, table_name, num_inserts):
    node1.query((
        "SET max_partitions_per_insert_block = 10000000;\n"
        "INSERT INTO {table_name} (num) SELECT number AS num FROM numbers_mt({num_inserts})"
    ).format(table_name=table_name, num_inserts=num_inserts))
>>>>>>> Starting to add ALTER RENAME COLUMN integration tests.
=======
>>>>>>> Adding more tests.

def select(node, table_name, col_name="num", expected_result=None, iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            r = node.query("SELECT count() FROM {} WHERE {} % 1000 > 0".format(table_name, col_name))
            if expected_result:
                assert r == expected_result
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise
<<<<<<< HEAD
<<<<<<< HEAD
=======
            print(ex)
>>>>>>> Starting to add ALTER RENAME COLUMN integration tests.
=======
>>>>>>> Adding more tests.

def rename_column(node, table_name, name, new_name, iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            node.query("ALTER TABLE {table_name} RENAME COLUMN {name} to {new_name}".format(
                table_name=table_name, name=name, new_name=new_name
            ))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Adding more tests.

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

        # rename column back to original name
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2", "999\n")
    finally:
        drop_table(nodes, table_name)
<<<<<<< HEAD
=======
            print(ex)
>>>>>>> Starting to add ALTER RENAME COLUMN integration tests.
=======
>>>>>>> Adding more tests.

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

<<<<<<< HEAD
<<<<<<< HEAD
        # rename column back to original name
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
=======
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select works
>>>>>>> Starting to add ALTER RENAME COLUMN integration tests.
=======
        # rename column back to original
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
>>>>>>> Adding more tests.
        select(node1, table_name, "num2", "999\n")
    finally:
        drop_table(nodes, table_name)

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Adding more tests.
def test_rename_with_parallel_insert(started_cluster):
    table_name = "test_rename_with_parallel_insert"
    drop_table(nodes, table_name)
    try:
        create_table(nodes, table_name)
        insert(node1, table_name, 1000)
<<<<<<< HEAD

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 5, True)))
            tasks.append(p.apply_async(insert, (node1, table_name, 100, "foo3", 5, True)))
            tasks.append(p.apply_async(insert, (node2, table_name, 100, "num2", 5, True)))
            tasks.append(p.apply_async(insert, (node3, table_name, 100, "foo2", 5, True)))
        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2")
    finally:
        drop_table(nodes, table_name)
=======

>>>>>>> Starting to add ALTER RENAME COLUMN integration tests.
=======

        p = Pool(15)
        tasks = []
        for i in range(1):
            tasks.append(p.apply_async(rename_column, (node1, table_name, "num2", "foo2", 5, True)))
            tasks.append(p.apply_async(rename_column, (node2, table_name, "foo2", "foo3", 5, True)))
            tasks.append(p.apply_async(rename_column, (node3, table_name, "foo3", "num2", 5, True)))
            tasks.append(p.apply_async(insert, (node1, table_name, 100, "foo3", 5, True)))
            tasks.append(p.apply_async(insert, (node2, table_name, 100, "num2", 5, True)))
            tasks.append(p.apply_async(insert, (node3, table_name, 100, "foo2", 5, True)))
        for task in tasks:
            task.get(timeout=240)

        # rename column back to original
        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select still works
        select(node1, table_name, "num2")
    finally:
        drop_table(nodes, table_name)
<<<<<<< HEAD
>>>>>>> Adding more tests.
=======

def test_rename_distributed(started_cluster):
    table_name = 'test_rename_distributed'
    try:
        create_distributed_table(node1, table_name)
        insert(node1, table_name, 1000)

        rename_column_on_cluster(node1, table_name, 'num2', 'foo2')
        rename_column_on_cluster(node1, '%s_replicated' % table_name, 'num2', 'foo2')

        insert(node1, table_name, 1000)

        select(node1, table_name, "foo2", '1998\n')
    finally:
        drop_distributed_table(node1, table_name)
>>>>>>> Adding test to check rename column when a replicated table has distributed table over it.
