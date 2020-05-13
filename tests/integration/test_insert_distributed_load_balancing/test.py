# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

n1 = cluster.add_instance('n1', main_configs=['configs/remote_servers.xml'])
n2 = cluster.add_instance('n2', main_configs=['configs/remote_servers.xml'])

@pytest.fixture(scope='module', autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(scope='function', autouse=True)
def create_tables():
    n1.query('DROP TABLE IF EXISTS data')
    n2.query('DROP TABLE IF EXISTS data')
    n1.query('DROP TABLE IF EXISTS dist')

    n1.query('CREATE TABLE data (key Int) Engine=Memory()')
    n2.query('CREATE TABLE data (key Int) Engine=Memory()')
    n1.query("""
    CREATE TABLE dist AS data
    Engine=Distributed(
        integration_test_cluster,
        currentDatabase(),
        data,
        rand()
    )
    """)

def insert_data(**settings):
    n1.query('INSERT INTO dist SELECT * FROM numbers(10)', settings=settings)
    n1.query('SYSTEM FLUSH DISTRIBUTED dist')

def test_prefer_localhost_replica_1():
    insert_data()
    assert int(n1.query('SELECT count() FROM data')) == 10
    assert int(n2.query('SELECT count() FROM data')) == 0

def test_prefer_localhost_replica_1_load_balancing_in_order():
    insert_data(load_balancing='in_order')
    assert int(n1.query('SELECT count() FROM data')) == 10
    assert int(n2.query('SELECT count() FROM data')) == 0

def test_prefer_localhost_replica_0_load_balancing_nearest_hostname():
    insert_data(load_balancing='nearest_hostname', prefer_localhost_replica=0)
    assert int(n1.query('SELECT count() FROM data')) == 10
    assert int(n2.query('SELECT count() FROM data')) == 0

def test_prefer_localhost_replica_0_load_balancing_in_order():
    insert_data(load_balancing='in_order', prefer_localhost_replica=0)
    assert int(n1.query('SELECT count() FROM data')) == 0
    assert int(n2.query('SELECT count() FROM data')) == 10

def test_prefer_localhost_replica_0_load_balancing_in_order_sync():
    insert_data(load_balancing='in_order', prefer_localhost_replica=0, insert_distributed_sync=1)
    assert int(n1.query('SELECT count() FROM data')) == 0
    assert int(n2.query('SELECT count() FROM data')) == 10
