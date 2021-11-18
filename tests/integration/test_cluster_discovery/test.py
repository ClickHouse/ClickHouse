import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f'node{i}',
        main_configs=['config/config.xml'],
        user_configs=['config/settings.xml'],
        stay_alive=True,
        with_zookeeper=True
    ) for i in range(5)
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_nodes_count_in_cluster(nodes, expected, cluster_name, *, retries=5):
    """
    Check nodes count in system.clusters for specified cluster
    """
    assert 1 <= retries <= 6

    for retry in range(1, retries + 1):
        nodes_cnt = [
            int(node.query(f"SELECT count() FROM system.clusters WHERE cluster = '{cluster_name}'"))
            for node in nodes
        ]
        if all(actual == expected for actual in nodes_cnt):
            break

        if retry != retries:
            time.sleep(2 ** retry)
    else:
        raise Exception(f'Wrong nodes count in cluster: {nodes_cnt}, expected: {expected} (after {retries} retries)')


def test_cluster_discovery_startup_and_stop(start_cluster):
    """
    Start cluster, check nodes count in system.clusters,
    then stop/start some nodes and check that it (dis)appeared in cluster.
    """

    check_nodes_count_in_cluster([nodes[0], nodes[2]], len(nodes), 'test_auto_cluster')

    nodes[1].stop_clickhouse(kill=True)
    check_nodes_count_in_cluster([nodes[0], nodes[2]], len(nodes) - 1, 'test_auto_cluster')

    nodes[3].stop_clickhouse()
    check_nodes_count_in_cluster([nodes[0], nodes[2]], len(nodes) - 2, 'test_auto_cluster')

    nodes[1].start_clickhouse()
    check_nodes_count_in_cluster([nodes[0], nodes[2]], len(nodes) - 1, 'test_auto_cluster')

    nodes[3].start_clickhouse()
    check_nodes_count_in_cluster([nodes[0], nodes[2]], len(nodes), 'test_auto_cluster')

    check_nodes_count_in_cluster([nodes[1], nodes[2]], 2, 'two_shards', retries=1)
