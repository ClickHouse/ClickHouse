import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

nodes = {
    "node0": cluster.add_instance(
        "node0",
        main_configs=["config/config_with_pwd.xml", "config/config_with_secret1.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
    "node1": cluster.add_instance(
        "node1",
        main_configs=["config/config_with_pwd.xml", "config/config_with_secret2.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_connect_with_password(start_cluster):
    check_on_cluster(
        [nodes["node0"], nodes["node1"]],
        len(nodes),
        cluster_name="test_auto_cluster_with_pwd",
        what="count()",
        msg="Wrong nodes count in cluster",
        query_params={"password": "passwordAbc"},
    )

    result = nodes["node0"].query(
        "SELECT sum(number) FROM clusterAllReplicas('test_auto_cluster_with_pwd', numbers(3)) GROUP BY hostname()",
        password="passwordAbc",
    )
    assert result == "3\n3\n", result

    result = nodes["node0"].query_and_get_error(
        "SELECT sum(number) FROM clusterAllReplicas('test_auto_cluster_with_wrong_pwd', numbers(3)) GROUP BY hostname()",
        password="passwordAbc",
    )
    assert "Authentication failed" in result, result

    result = nodes["node0"].query(
        "SELECT sum(number) FROM clusterAllReplicas('test_auto_cluster_with_secret', numbers(3)) GROUP BY hostname()",
        password="passwordAbc",
    )
    assert result == "3\n3\n", result

    result = nodes["node0"].query_and_get_error(
        "SELECT sum(number) FROM clusterAllReplicas('test_auto_cluster_with_wrong_secret', numbers(3)) GROUP BY hostname()",
        password="passwordAbc",
    )

    # With an incorrect secret, we don't get "Authentication failed", but the connection is simply dropped.
    # So, we get messages like "Connection reset by peer" or "Attempt to read after eof".
    # We only check that an error occurred and the message is not empty.
    assert result
