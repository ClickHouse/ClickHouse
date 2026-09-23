import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    stay_alive=True,
)

DATABASE = "db_active_node_taken"
NODES = (node1, node2)
DATABASE_UUID = "2b3d1d8c-6f4e-4a7b-9c1e-5d8f0a6b7c21"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_active_node_owned_by_another_server(started_cluster):
    # Both hosts get one database UUID and literal shard/replica names, so they are the same replica,
    # and the second one finds <replica_path>/active owned by the live first one. This is what
    # `CREATE DATABASE ... ON CLUSTER` does (issue #115818), but there both hosts create the replica
    # concurrently, and a host that reads Keeper before the other one writes it fails the query
    # itself with REPLICA_ALREADY_EXISTS. Creating the replicas one after another makes the second
    # host always read the replica written by the first. The database is left unusable, but the
    # failure is reachable from a user query, so it must not be reported as a logical error.
    for node in NODES:
        node.query(
            f"CREATE DATABASE {DATABASE} UUID '{DATABASE_UUID}' "
            f"ENGINE = Replicated('/clickhouse/databases/{DATABASE}', 's1', 'r1')"
        )

    # REPLICA_ALREADY_EXISTS, reported by the losing replica's own DDL worker after 3x session_timeout_ms.
    expected = f"Error on initialization of {DATABASE}: Code: 253"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if any(node.contains_in_log(expected) for node in NODES):
            break
        time.sleep(1)
    else:
        raise AssertionError(f"No replica reported '{expected}'")

    assert any(
        node.contains_in_log("still exists after .*s and is not owned by us") for node in NODES
    )

    for node in NODES:
        assert not node.contains_in_log("Logical error: 'Ephemeral node")
        assert node.query("SELECT 1") == "1\n"
