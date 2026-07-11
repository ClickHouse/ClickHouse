import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Two-node setup where:
#   cluster1 - the target cluster with two distinct nodes (node1, node2)
#   cluster2 - uses the hostname "node_common", which each node resolves to its
#               own IP address.  This simulates the case where multiple ClickHouse
#               instances share the same physical host.  Without skip_distributed_ddl
#               both nodes would claim and execute the same DDL task.  With
#               skip_distributed_ddl=1 the replica is excluded from ON CLUSTER DDL
#               dispatch entirely.

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        # Add "node_common" to each container's /etc/hosts pointing to the
        # container's own IP.  From the perspective of each node, "node_common"
        # resolves to itself — the same effect as having two ClickHouse instances
        # running on the same physical host.
        node1.append_hosts("node_common", node1.ip_address)
        node2.append_hosts("node_common", node2.ip_address)

        yield cluster
    finally:
        cluster.shutdown()


def count_table(node, table_name):
    return int(
        node.query(
            f"SELECT count() FROM system.tables WHERE name='{table_name}'"
        ).strip()
    )


def test_create_table_on_cluster1(started_cluster):
    """
    Create a table on cluster1 (node1, node2) while cluster2 is also configured
    with a common hostname (node_common) that each node resolves as itself.

    With skip_distributed_ddl=1 on cluster2's replica, the DDLWorker on each node
    does not register itself under the node_common host ID, so DDL dispatched to
    cluster1 reaches exactly node1 and node2 — no more, no less.
    """
    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")

    node1.query(
        "CREATE TABLE t1 ON CLUSTER 'cluster1' (x INT) ENGINE=MergeTree() ORDER BY x",
        settings={"distributed_ddl_task_timeout": 30},
    )

    assert count_table(node1, "t1") == 1, "table must exist on node1 after ON CLUSTER DDL"
    assert count_table(node2, "t1") == 1, "table must exist on node2 after ON CLUSTER DDL"

    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t1 SYNC")


def test_create_table_on_cluster2_all_skipped(started_cluster):
    """
    When every replica in a cluster has skip_distributed_ddl=1, dispatching a DDL
    query to that cluster must raise a user-facing BAD_ARGUMENTS error instead of
    the internal LOGICAL_ERROR from enqueueQuery.
    """
    with pytest.raises(QueryRuntimeException, match="skip_distributed_ddl"):
        node1.query(
            "CREATE TABLE t2 ON CLUSTER 'cluster2' (x INT) ENGINE=MergeTree() ORDER BY x",
            settings={"distributed_ddl_task_timeout": 30},
        )
