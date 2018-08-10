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

instance_test_inserts_local_cluster = cluster.add_instance(
    'instance_test_inserts_local_cluster',
    main_configs=['configs/remote_servers.xml'])

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        remote.query("CREATE TABLE local1 (x UInt32) ENGINE = Log")
        instance_test_reconnect.query('''
CREATE TABLE distributed (x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local1')
''')

        remote.query("CREATE TABLE local2 (d Date, x UInt32, s String) ENGINE = MergeTree(d, x, 8192)")
        instance_test_inserts_batching.query('''
CREATE TABLE distributed (d Date, x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local2')
''')

        instance_test_inserts_local_cluster.query("CREATE TABLE local (d Date, x UInt32) ENGINE = MergeTree(d, x, 8192)")
        instance_test_inserts_local_cluster.query('''
CREATE TABLE distributed_on_local (d Date, x UInt32) ENGINE = Distributed('test_local_cluster', 'default', 'local')
''')

        node1.query('''
CREATE TABLE replicated(date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/replicated', 'node1', date, id, 8192)
''')
        node2.query('''
CREATE TABLE replicated(date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/replicated', 'node2', date, id, 8192)
''')

        node1.query('''
CREATE TABLE distributed (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica', 'default', 'replicated')
''')

        node2.query('''
CREATE TABLE distributed (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica', 'default', 'replicated')
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

        # After ALTER the structure of the saved blocks will be different
        instance.query("ALTER TABLE distributed ADD COLUMN s String")

        for i in range(10, 13):
            instance.query("INSERT INTO distributed(d, x) VALUES ('2000-01-01', {})".format(i))

    time.sleep(1.0)

    result = remote.query("SELECT _part, groupArray(x) FROM local2 GROUP BY _part ORDER BY _part")

    # Explanation: as merges are turned off on remote instance, active parts in local2 table correspond 1-to-1
    # to inserted blocks.
    # Batches of max 3 rows are formed as min_insert_block_size_rows = 3.
    # Blocks:
    # 1. Failed batch that is retried with the same contents.
    # 2. Full batch of inserts with (d, x) order of columns.
    # 3. Full batch of inserts with (x, d) order of columns.
    # 4. Full batch of inserts after ALTER (that have different block structure).
    # 5. What was left to insert with (d, x) order before ALTER.
    expected = '''\
20000101_20000101_1_1_0\t[1]
20000101_20000101_2_2_0\t[3,4,5]
20000101_20000101_3_3_0\t[2,7,8]
20000101_20000101_4_4_0\t[10,11,12]
20000101_20000101_5_5_0\t[6,9]
'''
    assert TSV(result) == TSV(expected)


def test_inserts_local(started_cluster):
    instance = instance_test_inserts_local_cluster
    instance.query("INSERT INTO distributed_on_local VALUES ('2000-01-01', 1)")
    time.sleep(0.5)
    assert instance.query("SELECT count(*) FROM local").strip() == '1'

def test_prefer_localhost_replica(started_cluster):
    test_query = "SELECT * FROM distributed ORDER BY id;"
    node1.query("INSERT INTO distributed VALUES (toDate('2017-06-17'), 11)")
    node2.query("INSERT INTO distributed VALUES (toDate('2017-06-17'), 22)")
    time.sleep(1.0)
    expected_distributed = '''\
2017-06-17\t11
2017-06-17\t22
'''
    assert TSV(node1.query(test_query)) == TSV(expected_distributed)
    assert TSV(node2.query(test_query)) == TSV(expected_distributed)
    with PartitionManager() as pm:
        pm.partition_instances(node1, node2, action='REJECT --reject-with tcp-reset')
        node1.query("INSERT INTO replicated VALUES (toDate('2017-06-17'), 33)")
        node2.query("INSERT INTO replicated VALUES (toDate('2017-06-17'), 44)")
        time.sleep(1.0)
    expected_from_node2 =  '''\
2017-06-17\t11
2017-06-17\t22
2017-06-17\t44
'''
    # Query is sent to node2, as it local and prefer_localhost_replica=1
    assert TSV(node2.query(test_query)) == TSV(expected_from_node2)
    expected_from_node1 =  '''\
2017-06-17\t11
2017-06-17\t22
2017-06-17\t33
'''
    # Now query is sent to node1, as it higher in order
    assert TSV(node2.query("SET load_balancing='in_order'; SET prefer_localhost_replica=0;" + test_query)) == TSV(expected_from_node1)
