import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True,
                             macros={"shard": 1, "replica": 1, "shard_bk": 3, "replica_bk": 2})
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True,
                             macros={"shard": 2, "replica": 1, "shard_bk": 1, "replica_bk": 2})
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'], with_zookeeper=True,
                             macros={"shard": 3, "replica": 1, "shard_bk": 2, "replica_bk": 2})

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query('''
CREATE DATABASE replica_1 on cluster cross_3shards_2replicas;
CREATE DATABASE replica_2 on cluster cross_3shards_2replicas;

CREATE TABLE replica_1.replicated_local  on cluster cross_3shards_2replicas (part_key Date, id UInt32, shard_id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated', '{replica}') 
    partition by part_key order by id;
CREATE TABLE replica_1.replicated  on cluster cross_3shards_2replicas as replica_1.replicated_local  
    ENGINE = Distributed(cross_3shards_2replicas, '', replicated_local, shard_id);
    
CREATE TABLE replica_2.replicated_local on cluster cross_3shards_2replicas (part_key Date, id UInt32, shard_id UInt32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard_bk}/replicated', '{replica_bk}') 
    partition by part_key order by id;
CREATE TABLE replica_2.replicated  on cluster cross_3shards_2replicas as replica_2.replicated_local  
    ENGINE = Distributed(cross_3shards_2replicas, '', replicated_local, shard_id);
    ''')

        node1.query('''
CREATE DATABASE test_1 on cluster test_3shards_1replicas;
CREATE TABLE test_1.t1_local  on cluster test_3shards_1replicas (part_key Date, id UInt32, shard_id UInt32)
    ENGINE = MergeTree() partition by part_key order by id;
CREATE TABLE test_1.t1  on cluster test_3shards_1replicas as test_1.t1_local
    ENGINE = Distributed(test_3shards_1replicas, 'test_1', t1_local, shard_id);
            ''')

        to_insert = '''\
2017-06-16	10	0
2017-06-17	11	0
2017-06-16	20	1
2017-06-17	21	1
2017-06-16	30	2
2017-06-17	31	2
'''
        node1.query("INSERT INTO replica_1.replicated FORMAT TSV", stdin=to_insert)
        node1.query("INSERT INTO test_1.t1 FORMAT TSV", stdin=to_insert)

        time.sleep(0.5)

        yield cluster

    finally:
        cluster.shutdown()


def test_mutations_on_replicated_table(started_cluster):
    node1.query('''alter table replica_1.replicated update shard_id=shard_id+3 where part_key='2017-06-16'; ''')
    time.sleep(1)
    assert_eq_with_retry(node1, "SELECT count(*) FROM replica_2.replicated where shard_id >= 3 and part_key='2017-06-16'", '3')

    node1.query("alter table replica_1.replicated delete where shard_id >=3 ;")
    time.sleep(1)
    assert_eq_with_retry(node1, "SELECT count(*) FROM replica_2.replicated where shard_id >= 3", '0')

    node2.query("alter table replica_1.replicated drop partition '2017-06-17';")
    time.sleep(1)
    assert_eq_with_retry(node1, "SELECT count(*) FROM replica_2.replicated where part_key='2017-06-17'", '0')

def test_mutations_on_merge_tree_table(started_cluster):
    node1.query('''alter table test_1.t1 update shard_id=shard_id+3 where part_key='2017-06-16'; ''')
    time.sleep(1)
    assert_eq_with_retry(node1, "SELECT count(*) FROM test_1.t1 where shard_id >= 3 and part_key='2017-06-16'", '3')

    node1.query("alter table test_1.t1 delete where shard_id >=3;")
    time.sleep(1)
    assert_eq_with_retry(node1, "SELECT count(*) FROM test_1.t1 where shard_id >= 3", '0')

    node2.query("alter table test_1.t1 drop partition '2017-06-17';")
    time.sleep(1)
    assert_eq_with_retry(node1, "SELECT count(*) FROM test_1.t1", '0')

def test_multiple_mutations_on_merge_tree_table(started_cluster):
    node1.query('''alter table test_1.t1 update shard_id=shard_id+3 where part_key='2017-06-16', update shard_id=shard_id+4 where part_key='2017-06-17'; ''')
    time.sleep(3)
    assert_eq_with_retry(node1, "SELECT count(*) FROM test_1.t1 where shard_id >= 3 and part_key='2017-06-16'", '3')
    assert_eq_with_retry(node1, "SELECT count(*) FROM test_1.t1 where shard_id > 3 and part_key='2017-06-17'", '3')

    node1.query("alter table test_1.t1 delete where shard_id >=3 and part_key='2017-06-16', delete where shard_id >3;")
    time.sleep(3)
    assert_eq_with_retry(node1, "SELECT count(*) FROM test_1.t1 where shard_id >= 3", '0')
