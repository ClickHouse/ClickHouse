import time
from contextlib import contextmanager

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node_to_shards = [
            (node1, [0, 2]),
            (node2, [0, 1]),
            (node3, [1, 2]),
        ]

        for node, shards in node_to_shards:
            for shard in shards:
                node.query('''
CREATE DATABASE shard_{shard};

CREATE TABLE shard_{shard}.replicated(date Date, id UInt32, shard_id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated', '{replica}', date, id, 8192);
                '''.format(shard=shard, replica=node.name))

            node.query('''
CREATE TABLE distributed(date Date, id UInt32, shard_id UInt32)
    ENGINE = Distributed(test_cluster, '', replicated, shard_id);
''')

        # Insert some data onto different shards using the Distributed table
        to_insert = '''\
2017-06-16	111	0
2017-06-16	222	1
2017-06-16	333	2
'''
        node1.query("INSERT INTO distributed FORMAT TSV", stdin=to_insert)
        time.sleep(0.5)

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    # Check that the data has been inserted into correct tables.
    assert_eq_with_retry(node1, "SELECT id FROM shard_0.replicated", '111')
    assert_eq_with_retry(node1, "SELECT id FROM shard_2.replicated", '333')

    assert_eq_with_retry(node2, "SELECT id FROM shard_0.replicated", '111')
    assert_eq_with_retry(node2, "SELECT id FROM shard_1.replicated", '222')

    assert_eq_with_retry(node3, "SELECT id FROM shard_1.replicated", '222')
    assert_eq_with_retry(node3, "SELECT id FROM shard_2.replicated", '333')

    # Check that SELECT from the Distributed table works.
    expected_from_distributed = '''\
2017-06-16	111	0
2017-06-16	222	1
2017-06-16	333	2
'''
    assert_eq_with_retry(node1, "SELECT * FROM distributed ORDER BY id", expected_from_distributed)
    assert_eq_with_retry(node2, "SELECT * FROM distributed ORDER BY id", expected_from_distributed)
    assert_eq_with_retry(node3, "SELECT * FROM distributed ORDER BY id", expected_from_distributed)

    # Now isolate node3 from other nodes and check that SELECTs on other nodes still work.
    with PartitionManager() as pm:
        pm.partition_instances(node3, node1, action='REJECT --reject-with tcp-reset')
        pm.partition_instances(node3, node2, action='REJECT --reject-with tcp-reset')

        assert_eq_with_retry(node1, "SELECT * FROM distributed ORDER BY id", expected_from_distributed)
        assert_eq_with_retry(node2, "SELECT * FROM distributed ORDER BY id", expected_from_distributed)

        with pytest.raises(Exception):
            print(node3.query_with_retry("SELECT * FROM distributed ORDER BY id", retry_count=5))


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
