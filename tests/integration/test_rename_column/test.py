from __future__ import print_function

import time
import pytest

from multiprocessing.dummy import Pool
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)
node3 = cluster.add_instance('node3', with_zookeeper=True)
nodes = [node1, node2, node3]

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

def insert(node, table_name, num_inserts):
    node1.query((
        "SET max_partitions_per_insert_block = 10000000;\n"
        "INSERT INTO {table_name} (num) SELECT number AS num FROM numbers_mt({num_inserts})"
    ).format(table_name=table_name, num_inserts=num_inserts))

def select(node, table_name, col_name="num", expected_result=None, iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            r = node.query("SELECT count() FROM {} WHERE {} % 1000 > 0".format(table_name, col_name))
            if expected_result:
                assert r == expected_result
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise
            print(ex)

def rename_column(node, table_name, name, new_name, iterations=1, ignore_exception=False):
    for i in range(iterations):
        try:
            node.query("ALTER TABLE {table_name} RENAME COLUMN {name} to {new_name}".format(
                table_name=table_name, name=name, new_name=new_name
            ))
        except QueryRuntimeException as ex:
            if not ignore_exception:
                raise
            print(ex)

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

        rename_column(node1, table_name, "foo3", "num2", 1, True)
        rename_column(node1, table_name, "foo2", "num2", 1, True)

        # check that select works
        select(node1, table_name, "num2", "999\n")
    finally:
        drop_table(nodes, table_name)


