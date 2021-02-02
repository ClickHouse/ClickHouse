import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

# Cluster with 1 shard of 3 replicas. node is the instance with Distributed table.
node = cluster.add_instance(
    'node', with_zookeeper=True, main_configs=['configs/remote_servers.xml'], user_configs=['configs/users.xml'])
node_1 = cluster.add_instance('node_1', with_zookeeper=True,  user_configs=['configs/users1.xml'])
node_2 = cluster.add_instance('node_2', with_zookeeper=True)
node_3 = cluster.add_instance('node_3', with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node_1.query('''CREATE TABLE replicated (id UInt32, date Date) ENGINE =
            ReplicatedMergeTree('/clickhouse/tables/replicated', 'node_1')  ORDER BY id PARTITION BY toYYYYMM(date)''')

        node_2.query('''CREATE TABLE replicated (id UInt32, date Date) ENGINE =
            ReplicatedMergeTree('/clickhouse/tables/replicated', 'node_2')  ORDER BY id PARTITION BY toYYYYMM(date)''')
        
        node_3.query('''CREATE TABLE replicated (id UInt32, date Date) ENGINE =
            ReplicatedMergeTree('/clickhouse/tables/replicated', 'node_3')  ORDER BY id PARTITION BY toYYYYMM(date)''')

        node.query('''CREATE TABLE distributed (id UInt32, date Date) ENGINE =
            Distributed('test_cluster', 'default', 'replicated')''')

        yield cluster

    finally:
        cluster.shutdown()

def test(started_cluster):
    node.query("INSERT INTO distributed VALUES (1, '2020-01-01')")
    
    # Without hedged requests select query will last more 30 seconds,
    # with hedged requests it will last just over 2 seconds

    start = time.time()
    node.query("SELECT * FROM distributed");
    query_time = time.time() - start
    
    print(query_time)

