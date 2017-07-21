import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)

instance_test_reconnect = cluster.add_instance('instance_test_reconnect', main_configs=['configs/remote_servers.xml'])
instance_test_inserts_batching = cluster.add_instance(
    'instance_test_inserts_batching',
    main_configs=['configs/remote_servers.xml'], user_configs=['configs/enable_distributed_inserts_batching.xml'])
remote = cluster.add_instance('remote', user_configs=['configs/forbid_background_merges.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        remote.query("CREATE TABLE local1 (x UInt32) ENGINE = Log")
        instance_test_reconnect.query('''
CREATE TABLE distributed (x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local1')
''')

        remote.query("CREATE TABLE local2 (d Date, x UInt32) ENGINE = MergeTree(d, x, 8192)")
        instance_test_inserts_batching.query('''
CREATE TABLE distributed (d Date, x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local2')
''')

        yield cluster

    finally:
        cluster.shutdown()


def test_reconnect(started_cluster):
    instance = instance_test_reconnect

    with PartitionManager() as pm:
        # Open a connection for insertion.
        instance.query("INSERT INTO distributed VALUES (1)")
        time.sleep(0.5)
        assert remote.query("SELECT count(*) FROM local1").strip() == '1'

        # Now break the connection.
        pm.partition_instances(instance, remote, action='REJECT --reject-with tcp-reset')
        instance.query("INSERT INTO distributed VALUES (2)")
        time.sleep(0.5)

        # Heal the partition and insert more data.
        # The connection must be reestablished and after some time all data must be inserted.
        pm.heal_all()
        instance.query("INSERT INTO distributed VALUES (3)")
        time.sleep(0.5)

        assert remote.query("SELECT count(*) FROM local1").strip() == '3'


def test_inserts_batching(started_cluster):
    instance = instance_test_inserts_batching

    with PartitionManager() as pm:
        pm.partition_instances(instance, remote)

        instance.query("INSERT INTO distributed(d, x) VALUES ('2000-01-01', 1)")
        # Sleep a bit so that this INSERT forms a batch of its own.
        time.sleep(0.1)

        instance.query("INSERT INTO distributed(x, d) VALUES (2, '2000-01-01')")

        for i in range(3, 7):
            instance.query("INSERT INTO distributed(d, x) VALUES ('2000-01-01', {})".format(i))

        for i in range(7, 9):
            instance.query("INSERT INTO distributed(x, d) VALUES ({}, '2000-01-01')".format(i))

        instance.query("INSERT INTO distributed(d, x) VALUES ('2000-01-01', 9)")

    time.sleep(1.0)

    result = remote.query("SELECT _part, groupArray(x) FROM local2 GROUP BY _part ORDER BY _part")

    # Explanation: as merges are turned off on remote instance, active parts in local2 table correspond 1-to-1
    # to inserted blocks.
    # Batches of max 3 rows are formed as min_insert_block_size_rows = 3.
    # Blocks:
    # 1. Failed batch that is retried with the same contents.
    # 2. Full batch of inserts with (d, x) order of columns.
    # 3. Full batch of inserts with (x, d) order of columns.
    # 4. What is left to insert with (d, x) order.
    expected = '''\
20000101_20000101_1_1_0	[1]
20000101_20000101_2_2_0	[3,4,5]
20000101_20000101_3_3_0	[2,7,8]
20000101_20000101_4_4_0	[6,9]
'''
    assert TSV(result) == TSV(expected)
