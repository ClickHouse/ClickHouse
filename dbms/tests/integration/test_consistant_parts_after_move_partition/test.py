import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry


CLICKHOUSE_DATABASE = os.environ["CLICKHOUSE_DATABASE"]


def initialize_database(nodes, shard):
    for node in nodes:
        node.query((
        f'''
        CREATE DATABASE {CLICKHOUSE_DATABASE};
        CREATE TABLE src (p UInt64, d UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/{CLICKHOUSE_DATABASE}/tables/test{shard}/replicated', '{replica}')
        ORDER BY id PARTITION BY toYYYYMM(date)
        SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
        CREATE TABLE dest (p UInt64, d UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/{CLICKHOUSE_DATABASE}/tables/test{shard}/replicated', '{replica}')
        ORDER BY id PARTITION BY toYYYYMM(date)
        SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
        '''.format(shard=shard, replica=node.name))


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        initialize_database([node1, node2], 1)
        yield cluster
    except Exception as ex:
        print ex
    finally:
        cluster.shutdown()


def test_consistent_part_after_move_partition(start_cluster):
    # insert into all replicas
    for i in range(100):
        node1.query('INSERT INTO `{database}`.src VALUE ({value} % 2, {value})'.format(database=CLICKHOUSE_DATABASE,
                                                                                         value=i))
    query_source = f'SELECT COUNT(*) FROM `{CLICKHOUSE_DATABASE}`.src'
    query_dest = f'SELECT COUNT(*) FROM `{CLICKHOUSE_DATABASE}`.dest'
    assert_eq_with_retry(node2, query_source, node1.query(query_source))
    assert_eq_with_retry(node2, query_dest, node1.query(query_dest))

    node1.query(f'ALTER TABLE `{CLICKHOUSE_DATABASE}`.src MOVE PARTITION 1 TO TABLE `{CLICKHOUSE_DATABASE}`.dest')

    assert_eq_with_retry(node2, query_source, node1.query(query_source))
    assert_eq_with_retry(node2, query_dest, node1.query(query_dest))
