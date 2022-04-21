# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', main_configs=['configs/remote_servers.xml'], stay_alive=True)

@pytest.fixture(scope='module', autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_insert_distributed_async_send_success():
    node.query('CREATE TABLE data (key Int, value String) Engine=Null()')
    node.query("""
    CREATE TABLE dist AS data
    Engine=Distributed(
        test_cluster,
        currentDatabase(),
        data,
        key
    )
    """)

    node.exec_in_container(['bash', '-c', 'mkdir /var/lib/clickhouse/data/default/dist/shard10000_replica10000'])
    node.exec_in_container(['bash', '-c', 'touch /var/lib/clickhouse/data/default/dist/shard10000_replica10000/1.bin'])

    node.exec_in_container(['bash', '-c', 'mkdir /var/lib/clickhouse/data/default/dist/shard1_replica10000'])
    node.exec_in_container(['bash', '-c', 'touch /var/lib/clickhouse/data/default/dist/shard1_replica10000/1.bin'])

    node.exec_in_container(['bash', '-c', 'mkdir /var/lib/clickhouse/data/default/dist/shard10000_replica1'])
    node.exec_in_container(['bash', '-c', 'touch /var/lib/clickhouse/data/default/dist/shard10000_replica1/1.bin'])

    # will check that clickhouse-server is alive
    node.restart_clickhouse()
