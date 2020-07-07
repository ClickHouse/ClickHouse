# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import uuid
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

n1 = cluster.add_instance('n1', main_configs=['configs/remote_servers.xml'])
n2 = cluster.add_instance('n2', main_configs=['configs/remote_servers.xml'])
n3 = cluster.add_instance('n3', main_configs=['configs/remote_servers.xml'])

nodes = len(cluster.instances)
queries = nodes*5

def create_tables():
    for n in cluster.instances.values():
        n.query('DROP TABLE IF EXISTS data')
        n.query('DROP TABLE IF EXISTS dist')
        n.query('CREATE TABLE data (key Int) Engine=Memory()')
        n.query("""
        CREATE TABLE dist AS data
        Engine=Distributed(
            replicas_cluster,
            currentDatabase(),
            data)
        """.format())

def make_uuid():
    return uuid.uuid4().hex

@pytest.fixture(scope='module', autouse=True)
def start_cluster():
    try:
        cluster.start()
        create_tables()
        yield cluster
    finally:
        cluster.shutdown()

def get_node(query_node, *args, **kwargs):
    query_id = make_uuid()

    settings = {
        'query_id': query_id,
        'log_queries': 1,
        'log_queries_min_type': 'QUERY_START',
        'prefer_localhost_replica': 0,
    }
    if 'settings' not in kwargs:
        kwargs['settings'] = settings
    else:
        kwargs['settings'].update(settings)

    query_node.query('SELECT * FROM dist', *args, **kwargs)

    for n in cluster.instances.values():
        n.query('SYSTEM FLUSH LOGS')

    rows = query_node.query("""
    SELECT c.host_name
    FROM (
        SELECT _shard_num
        FROM cluster(shards_cluster, system.query_log)
        WHERE
            initial_query_id = '{query_id}' AND
            is_initial_query = 0 AND
            type = 'QueryFinish'
        ORDER BY event_date DESC, event_time DESC
        LIMIT 1
    ) a
    JOIN system.clusters c
    ON a._shard_num = c.shard_num AND cluster = 'shards_cluster'
    """.format(query_id=query_id))
    return rows.strip()

# TODO: right now random distribution looks bad, but works
def test_load_balancing_default():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={'load_balancing': 'random'}))
    assert len(unique_nodes) == nodes, unique_nodes

def test_load_balancing_nearest_hostname():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={'load_balancing': 'nearest_hostname'}))
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(['n1'])

def test_load_balancing_in_order():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={'load_balancing': 'in_order'}))
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(['n1'])

def test_load_balancing_first_or_random():
    unique_nodes = set()
    for _ in range(0, queries):
        unique_nodes.add(get_node(n1, settings={'load_balancing': 'first_or_random'}))
    assert len(unique_nodes) == 1, unique_nodes
    assert unique_nodes == set(['n1'])

# TODO: last_used will be reset on config reload, hence may fail
def test_load_balancing_round_robin():
    unique_nodes = set()
    for _ in range(0, nodes):
        unique_nodes.add(get_node(n1, settings={'load_balancing': 'round_robin'}))
    assert len(unique_nodes) == nodes, unique_nodes
    assert unique_nodes == set(['n1', 'n2', 'n3'])
