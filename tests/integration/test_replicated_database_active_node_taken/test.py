import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/cluster.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/cluster.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

DATABASE = "db_active_node_taken"
NODES = (node1, node2)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_active_node_owned_by_another_server(started_cluster):
    # ON CLUSTER assigns one database UUID to every host, and literal shard/replica names make both
    # hosts the same replica, so one of them finds <replica_path>/active owned by the live other one.
    # The database is left unusable (issue #115818), but the failure is reachable from a single
    # user query, so it must not be reported as a logical error.
    node1.query(
        f"CREATE DATABASE {DATABASE} ON CLUSTER 'two_nodes' "
        f"ENGINE = Replicated('/clickhouse/databases/{DATABASE}', 's1', 'r1')",
        settings={"distributed_ddl_task_timeout": 60},
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
