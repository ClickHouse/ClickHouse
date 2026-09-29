import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)

DATABASE = "db_active_node_taken"
NODES = (node1, node2)
ZK_PATH = f"/clickhouse/databases/{DATABASE}"
REPLICA_PATH = f"{ZK_PATH}/replicas/s1|r1"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_active_node_owned_by_another_server(started_cluster):
    # A server that claims a replica a live server already holds must report
    # REPLICA_ALREADY_EXISTS (Code: 253) and keep running, instead of dying on a logical error.
    # Reachable from a single user query, see issue #115818.
    node1.query(
        f"CREATE DATABASE {DATABASE} ENGINE = Replicated('{ZK_PATH}', 's1', 'r1')"
    )
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper "
        f"WHERE path = '{REPLICA_PATH}' AND name = 'active'",
        "1",
        retry_count=120,
    )

    # One database UUID plus a literal replica name are what make two servers the same replica,
    # and this is the statement CREATE DATABASE ... ON CLUSTER sends to each host. One host at a
    # time, so the node under contention is the active node and not the replica itself.
    db_uuid = node1.query(
        f"SELECT uuid FROM system.databases WHERE name = '{DATABASE}'"
    ).strip()
    node2.query(
        f"CREATE DATABASE {DATABASE} UUID '{db_uuid}' "
        f"ENGINE = Replicated('{ZK_PATH}', 's1', 'r1')"
    )

    expected = f"Error on initialization of {DATABASE}: Code: 253"
    node2.wait_for_log_line(expected, timeout=60)
    assert node2.contains_in_log("still exists after .*s and is not owned by us")
    assert not node1.contains_in_log(expected)

    for node in NODES:
        assert not node.contains_in_log("Logical error: 'Ephemeral node")
        assert node.query("SELECT 1") == "1\n"
