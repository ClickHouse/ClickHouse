import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__, zookeeper_config_path='configs/zookeeper.xml')

def add_instance(name):
    main_configs=[
        'configs/ddl.xml',
        'configs/ddl_timeout.xml',
        'configs/zookeeper.xml',
        'configs/remote_servers.xml',
    ]
    return cluster.add_instance(name,
        main_configs=main_configs,
        with_zookeeper=True)

add_instance('n1')
add_instance('n2')

@pytest.fixture(scope='module', autouse=True)
def test_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_clean(test_cluster):
    n1 = test_cluster.instances['n1']
    n2 = test_cluster.instances['n2']

    zk = cluster.get_kazoo_client('zoo1')

    n2.get_docker_handle().stop()
    n1.get_query_request("CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster'")

    # sleep longer than `task_max_lifetime`
    time.sleep(5);

    znodes = zk.get_children("/clickhouse/test_clean/ddl")
    assert TSV(znodes) == TSV("query-0000000000")

    n2.get_docker_handle().start()

    time.sleep(1);
    n1.query("DROP DATABASE IF EXISTS test ON CLUSTER 'cluster'")

