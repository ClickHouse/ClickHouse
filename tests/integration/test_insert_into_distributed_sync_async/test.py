#!/usr/bin/env python2
import sys
import os
from contextlib import contextmanager
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.network import PartitionManager
from helpers.test_tools import TSV
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException, QueryTimeoutExceedException

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query('''
CREATE TABLE local_table(date Date, val UInt64) ENGINE = MergeTree(date, (date, val), 8192);
''')


        node1.query('''
CREATE TABLE distributed_table(date Date, val UInt64) ENGINE = Distributed(test_cluster, default, local_table)
''')

        yield cluster

    finally:
        cluster.shutdown()


def test_insertion_sync(started_cluster):

    node1.query('''SET insert_distributed_sync = 1, insert_distributed_timeout = 0;
    INSERT INTO distributed_table SELECT today() as date, number as val FROM system.numbers LIMIT 10000''')

    assert node2.query("SELECT count() FROM local_table").rstrip() == '10000'

    node1.query('''
    SET insert_distributed_sync = 1, insert_distributed_timeout = 1;
    INSERT INTO distributed_table SELECT today() - 1 as date, number as val FROM system.numbers LIMIT 10000''')

    assert node2.query("SELECT count() FROM local_table").rstrip() == '20000'

    # Insert with explicitly specified columns.
    node1.query('''
    SET insert_distributed_sync = 1, insert_distributed_timeout = 1;
    INSERT INTO distributed_table(date, val) VALUES ('2000-01-01', 100500)''')

    # Insert with columns specified in different order.
    node1.query('''
    SET insert_distributed_sync = 1, insert_distributed_timeout = 1;
    INSERT INTO distributed_table(val, date) VALUES (100500, '2000-01-01')''')

    # Insert with an incomplete list of columns.
    node1.query('''
    SET insert_distributed_sync = 1, insert_distributed_timeout = 1;
    INSERT INTO distributed_table(val) VALUES (100500)''')

    expected = TSV('''
1970-01-01	100500
2000-01-01	100500
2000-01-01	100500''')
    assert TSV(node2.query('SELECT date, val FROM local_table WHERE val = 100500 ORDER BY date')) == expected


"""
def test_insertion_sync_fails_on_error(started_cluster):
    with PartitionManager() as pm:
        pm.partition_instances(node2, node1, action='REJECT --reject-with tcp-reset')
        with pytest.raises(QueryRuntimeException):
            node1.query('''
            SET insert_distributed_sync = 1, insert_distributed_timeout = 0;
            INSERT INTO distributed_table SELECT today() as date, number as val FROM system.numbers''', timeout=2)
"""


def test_insertion_sync_fails_with_timeout(started_cluster):
    with pytest.raises(QueryRuntimeException):
        node1.query('''
        SET insert_distributed_sync = 1, insert_distributed_timeout = 1;
        INSERT INTO distributed_table SELECT today() as date, number as val FROM system.numbers''')


def test_insertion_without_sync_ignores_timeout(started_cluster):
    with pytest.raises(QueryTimeoutExceedException):
        node1.query('''
        SET insert_distributed_sync = 0, insert_distributed_timeout = 1;
        INSERT INTO distributed_table SELECT today() as date, number as val FROM system.numbers''', timeout=1.5)


def test_insertion_sync_with_disabled_timeout(started_cluster):
    with pytest.raises(QueryTimeoutExceedException):
        node1.query('''
        SET insert_distributed_sync = 1, insert_distributed_timeout = 0;
        INSERT INTO distributed_table SELECT today() as date, number as val FROM system.numbers''', timeout=1)


def test_async_inserts_into_local_shard(started_cluster):
    node1.query('''CREATE TABLE shard_local (i Int64) ENGINE = Memory''')
    node1.query('''CREATE TABLE shard_distributed (i Int64) ENGINE = Distributed(local_shard_with_internal_replication, default, shard_local)''')
    node1.query('''INSERT INTO shard_distributed VALUES (1)''', settings={ "insert_distributed_sync" : 0 })

    assert TSV(node1.query('''SELECT count() FROM shard_distributed''')) == TSV("1\n")
    node1.query('''DETACH TABLE shard_distributed''')
    node1.query('''ATTACH TABLE shard_distributed''')
    assert TSV(node1.query('''SELECT count() FROM shard_distributed''')) == TSV("1\n")

    node1.query('''DROP TABLE shard_distributed''')
    node1.query('''DROP TABLE shard_local''')


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in cluster.instances.items():
            print name, instance.ip_address
        raw_input("Cluster created, press any key to destroy...")
