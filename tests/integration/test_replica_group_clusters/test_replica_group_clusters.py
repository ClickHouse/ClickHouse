"""
Integration tests for the `replica_group` field of the clusters of a `Replicated` database.

Every database replica publishes its replica group (the `<replica_group_name>` server
configuration) to ZooKeeper, and the addresses of the clusters of the database carry it, so
`system.clusters` shows the replica group of every node. The tests verify:

1. The plain cluster of the database (`<database>`) consists of the replicas of the local
   replica group only, and the `replica_group` column is that group for every row. A node
   without a replica group sees only the replicas without a group, with an empty
   `replica_group`.
2. The `all_groups.<database>` cluster consists of every replica of the database, and the
   `replica_group` column shows the group of each node.
3. The clusters are usable for queries: `cluster()` over the plain cluster of a node reaches
   the nodes of its group only, `cluster()` over the all-groups cluster reaches every replica.

`node1`/`node2` are members of the replica group `read`, `node3`/`node4` of the replica group
`write`, `node5` is a member of no group.
"""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster


DB = "repldb"
ZK_PATH = "/clickhouse/databases/repldb"

READ_NODES = ["node1", "node2"]
WRITE_NODES = ["node3", "node4"]
UNGROUPED_NODES = ["node5"]
ALL_NODES = READ_NODES + WRITE_NODES + UNGROUPED_NODES

WAIT_SECONDS = 60
POLL_INTERVAL = 0.3


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    for index, name in enumerate(ALL_NODES):
        group = None
        if name in READ_NODES:
            group = "read"
        elif name in WRITE_NODES:
            group = "write"
        cluster.add_instance(
            name,
            main_configs=[] if group is None else [f"configs/config.d/replica_group_{group}.xml"],
            macros={"shard": str(index + 1), "replica": name},
            with_zookeeper=True,
            stay_alive=True,
        )

    try:
        logging.info("Starting cluster...")
        cluster.start()

        for name in ALL_NODES:
            cluster.instances[name].query(
                f"CREATE DATABASE {DB} ENGINE = Replicated('{ZK_PATH}', '{{shard}}', '{{replica}}')"
            )
        yield cluster
    finally:
        cluster.shutdown(ignore_logical_errors=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rows_of(node, cluster):
    """`[(replica_name, replica_group)]` of the rows of `cluster` in system.clusters.

    The lines are split without stripping, so that an empty trailing `replica_group`
    (a node without a group) is preserved."""
    return sorted(
        tuple(line.split("\t"))
        for line in node.query(
            f"SELECT database_replica_name, replica_group FROM system.clusters "
            f"WHERE cluster = '{cluster}'"
        ).splitlines()
    )


def wait_for_table(node, table, timeout=WAIT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.query(f"EXISTS TABLE {table}").strip() == "1":
            return
        time.sleep(POLL_INTERVAL)
    raise AssertionError(f"Timed out waiting for {table} to appear on {node.name}")


# ---------------------------------------------------------------------------
# 1. The plain cluster of the database: the local replica group only
# ---------------------------------------------------------------------------

def test_replica_group_of_local_cluster(started_cluster):
    expected_by_node = {
        "node1": [(name, "read") for name in READ_NODES],
        "node2": [(name, "read") for name in READ_NODES],
        "node3": [(name, "write") for name in WRITE_NODES],
        "node4": [(name, "write") for name in WRITE_NODES],
        "node5": [("node5", "")],
    }

    for name, expected in expected_by_node.items():
        node = started_cluster.instances[name]
        assert rows_of(node, DB) == sorted(expected), (
            f"On {name} the cluster '{DB}' is {rows_of(node, DB)} "
            f"instead of the local group {sorted(expected)}"
        )


# ---------------------------------------------------------------------------
# 2. The all-groups cluster: every replica with its group
# ---------------------------------------------------------------------------

def test_replica_group_of_all_groups_cluster(started_cluster):
    expected = sorted(
        [(name, "read") for name in READ_NODES]
        + [(name, "write") for name in WRITE_NODES]
        + [(name, "") for name in UNGROUPED_NODES]
    )

    # Every node that is a member of a replica group sees the all-groups cluster of the
    # whole database, with the group of every node (empty for the nodes without a group).
    for name in READ_NODES + WRITE_NODES:
        node = started_cluster.instances[name]
        rows = rows_of(node, f"all_groups.{DB}")
        assert rows == expected, (
            f"On {name} the cluster 'all_groups.{DB}' is {rows} instead of {expected}"
        )

    # A node without a replica group has no all-groups cluster: its plain cluster already
    # consists of the replicas of its (empty) group.
    node5 = started_cluster.instances["node5"]
    count = node5.query(
        f"SELECT count() FROM system.clusters WHERE cluster = 'all_groups.{DB}'"
    ).strip()
    assert count == "0", (
        f"The node without a replica group unexpectedly sees the cluster 'all_groups.{DB}'"
    )


# ---------------------------------------------------------------------------
# 3. The clusters are usable for queries
# ---------------------------------------------------------------------------

def test_query_local_and_all_groups_clusters(started_cluster):
    node1 = started_cluster.instances["node1"]  # replica group: read
    table = f"{DB}.t"

    # A table of the replicated database: every replica has its own copy, so every
    # node inserts its own name and a query over a cluster returns the names of the
    # nodes the query actually reached.
    node1.query(f"CREATE TABLE {table} (h String) ENGINE = MergeTree ORDER BY tuple()")

    # The DDL of a replicated database waits only for the replicas of the local group,
    # so wait until the other nodes get the table too.
    for name in ALL_NODES:
        wait_for_table(started_cluster.instances[name], table)

    for name in ALL_NODES:
        started_cluster.instances[name].query(f"INSERT INTO {table} VALUES ('{name}')")

    def hosts_from(node, cluster_name):
        return sorted(node.query(f"SELECT h FROM cluster('{cluster_name}', {table})").strip().split())

    # The plain cluster of node1 consists of the nodes of its replica group only.
    assert hosts_from(node1, DB) == READ_NODES, (
        f"cluster('{DB}') from node1 reached {hosts_from(node1, DB)} "
        f"instead of the read nodes {READ_NODES}"
    )

    # The all-groups cluster queries every replica of the database.
    assert hosts_from(node1, f"all_groups.{DB}") == sorted(ALL_NODES), (
        f"cluster('all_groups.{DB}') from node1 reached "
        f"{hosts_from(node1, f'all_groups.{DB}')} instead of all the replicas {sorted(ALL_NODES)}"
    )
