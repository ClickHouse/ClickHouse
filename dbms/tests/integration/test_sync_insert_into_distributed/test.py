from contextlib import contextmanager
from helpers.network import PartitionManager

import pytest

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
        INSERT INTO distributed_table SELECT today() as date, number as val FROM system.numbers''', timeout=5)


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


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in cluster.instances.items():
            print name, instance.ip_address
        raw_input("Cluster created, press any key to destroy...")
