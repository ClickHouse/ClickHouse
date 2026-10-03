import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=[
            "config/config_secure.xml",
            "config/server.crt",
            "config/server.key",
        ],
        stay_alive=True,
        with_zookeeper=True,
    )
    for i in range(2)
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_secure_cluster(start_cluster):
    # Every node must advertise itself as secure on `tcp_port_secure`,
    # otherwise peers either skip it or dial the plain port with TLS.
    check_on_cluster(
        nodes,
        len(nodes),
        cluster_name="test_auto_cluster_secure",
        what="countIf(port = 9440)",
        msg="Wrong secure nodes count in cluster",
    )

    result = nodes[0].query(
        "SELECT count() FROM clusterAllReplicas('test_auto_cluster_secure', system.one)"
    )
    assert result == f"{len(nodes)}\n", result
