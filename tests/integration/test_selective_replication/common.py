"""Shared utilities for selective replication integration tests.

This module provides the cluster definition, fixtures, and helper functions
used by test_insert.py, test_select.py, test_merge.py, test_mutation.py,
and test_rebalance.py.
"""

import time
import uuid

from helpers.cluster import ClickHouseCluster

# ---------------------------------------------------------------------------
# Cluster definition (shared across all test files)
# ---------------------------------------------------------------------------

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/settings.xml"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/settings.xml"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r2"},
)
node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/settings.xml"],
    stay_alive=True,
    macros={"shard": "s1", "replica": "r3"},
)

ALL_NODES = [node1, node2, node3]
ALL_REPLICA_NAMES = {"r1", "r2", "r3"}
REPLICA_TO_NODE = {"r1": node1, "r2": node2, "r3": node3}

NO_FAULT = {}

# ---------------------------------------------------------------------------
# Fixture (re-exported by each test file)
# ---------------------------------------------------------------------------


def create_started_cluster_fixture():
    """Return a module-scoped fixture that starts/stops the shared cluster."""
    import pytest

    @pytest.fixture(scope="module")
    def started_cluster():
        try:
            cluster.start()
            yield cluster
        finally:
            cluster.shutdown()

    return started_cluster


# Shared module-scoped fixture re-exported by every test_*.py file.
started_cluster = create_started_cluster_fixture()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def wait_for_table_ready(node, table, database="default", timeout=30):
    """Poll until the table appears in system.replicas (ZK registration complete)."""
    start = time.time()
    while time.time() - start < timeout:
        result = node.query(
            f"SELECT count() FROM system.replicas "
            f"WHERE database = '{database}' AND table = '{table}'",
            settings=NO_FAULT,
        ).strip()
        if result == "1":
            return
        time.sleep(0.5)
    raise Exception(
        f"Timeout waiting for table {database}.{table} to appear in system.replicas on {node.name}"
    )


def wait_for_sync(node, table, database="default", timeout=120):
    """Poll until the replica queue drains and absolute_delay reaches 0."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = (
                node.query(
                    f"SELECT queue_size, absolute_delay, is_readonly "
                    f"FROM system.replicas "
                    f"WHERE database = '{database}' AND table = '{table}'",
                    settings=NO_FAULT,
                )
                .strip()
            )
            if not result:
                try:
                    node.query(f"SYSTEM SYNC REPLICA {database}.{table}", settings=NO_FAULT)
                except Exception:
                    pass
                time.sleep(1)
                continue
            parts = result.split("\t")
            if len(parts) >= 2 and int(parts[0]) == 0 and int(parts[1]) == 0:
                return
            time.sleep(1)
        except Exception:
            time.sleep(1)
    # Log diagnostic info before failing
    try:
        for n in ALL_NODES:
            diag = n.query(
                f"SELECT queue_size, absolute_delay, is_readonly, is_leader "
                f"FROM system.replicas "
                f"WHERE database = '{database}' AND table = '{table}' "
                f"FORMAT TSVWithNames",
                settings=NO_FAULT,
            ).strip()
            if diag:
                print(f"[DIAG] {n.name}: {diag}")
    except Exception as e:
        print(f"[DIAG] error getting replica info: {e}")
    raise TimeoutError(f"Replica {table} not synced within {timeout}s")


def sync_all(table, database="default", timeout=120):
    for n in ALL_NODES:
        wait_for_sync(n, table, database, timeout=timeout)


def get_assigned(node, table, partition_id, database="default"):
    result = (
        node.query(
            f"SELECT arrayJoin(assigned_replicas) "
            f"FROM system.selective_assignments "
            f"WHERE database = '{database}' AND table = '{table}' "
            f"AND partition_id = '{partition_id}'"
        )
        .strip()
    )
    if not result:
        return []
    return result.split("\n")


def get_row_count(node, table, database="default"):
    result = (
        node.query(
            f"SELECT sum(rows) FROM system.parts "
            f"WHERE table = '{table}' AND active "
            f"AND database = '{database}'"
        )
        .strip()
    )
    return int(result) if result else 0


def get_cluster_rows(table, database="default"):
    return sum(get_row_count(n, table, database) for n in ALL_NODES)


def get_nodes_with_partition(table, partition_id, database="default"):
    result = set()
    for n in ALL_NODES:
        count = int(
            n.query(
                f"SELECT count() FROM system.parts "
                f"WHERE table = '{table}' AND partition_id = '{partition_id}' "
                f"AND active AND database = '{database}'"
            ).strip()
        )
        if count > 0:
            result.add(n.name)
    return result


def create_table(node, table, zk_suffix, rf=2, database="default"):
    """In selective replication, CREATE TABLE must run on every replica
    because DDL is not auto-propagated across replicas.
    """
    zk_path = f"/clickhouse/tables/{zk_suffix}/{table}"
    for n in ALL_NODES:
        n.query(
            f"CREATE TABLE IF NOT EXISTS {database}.{table} "
            f"(d Date, k UInt64, v String) "
            f"ENGINE = ReplicatedMergeTree('{zk_path}', '{{replica}}') "
            f"PARTITION BY toYYYYMM(d) ORDER BY k "
            f"SETTINGS replication_factor = {rf}",
            settings=NO_FAULT,
        )
    for n in ALL_NODES:
        wait_for_table_ready(n, table, database)
    sync_all(table, database)


def drop_table(node, table, database="default"):
    try:
        node.query(f"DROP TABLE IF EXISTS {database}.{table} SYNC", settings=NO_FAULT)
    except Exception:
        pass


def zk_suffix():
    return uuid.uuid4().hex[:8]


def find_unassigned(node, table, partition_id, database="default"):
    """Return the replica name that is NOT assigned to the given partition."""
    assigned = set(get_assigned(node, table, partition_id, database))
    unassigned = ALL_REPLICA_NAMES - assigned
    assert len(unassigned) >= 1, f"No unassigned replica for {partition_id}"
    return unassigned.pop()


def find_assigned(node, table, partition_id, database="default"):
    """Return any one assigned replica for the partition."""
    assigned = get_assigned(node, table, partition_id, database)
    assert len(assigned) >= 1, f"No assigned replica for {partition_id}"
    return assigned[0]


def no_orphaned_cloning(node, table, database="default"):
    result = (
        node.query(
            f"SELECT count() FROM system.selective_assignments "
            f"WHERE database = '{database}' AND table = '{table}' "
            f"AND has(arrayMap(x -> x, assigned_replicas), ':cloning')"
        )
        .strip()
    )
    return int(result) == 0


def wait_for_cluster_rows(table, expected_count, database="default", timeout=30):
    """Wait until every node returns expected_count rows via SELECT routing."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for node in ALL_NODES:
            try:
                count = int(
                    node.query(
                        f"SELECT count() FROM {table}", settings=NO_FAULT
                    ).strip()
                )
                if count != expected_count:
                    ok = False
                    break
            except Exception:
                ok = False
                break
        if ok:
            return
        time.sleep(0.5)
    # Final check with detailed error
    for node in ALL_NODES:
        count = int(
            node.query(
                f"SELECT count() FROM {table}", settings=NO_FAULT
            ).strip()
        )
        assert count == expected_count, (
            f"{node.name}: expected {expected_count} rows, got {count}"
        )


def get_profile_event(node, event_name):
    """Return the current value of a ProfileEvent on a node."""
    result = node.query(
        f"SELECT value FROM system.events WHERE event = '{event_name}'"
    ).strip()
    return int(result) if result else 0


def get_node_by_replica(replica_name):
    """Return the cluster node object for a given replica name (r1/r2/r3)."""
    return REPLICA_TO_NODE[replica_name]


def get_metric(node, metric):
    """Return the current value of a system.events metric."""
    res = node.query(f"SELECT value FROM system.events WHERE event = '{metric}'").strip()
    return int(res) if res else 0


def get_parts_count(node, table, partition_id, database="default"):
    """Return the number of active parts for a given partition."""
    res = node.query(
        f"SELECT count() FROM system.parts "
        f"WHERE database = '{database}' AND table = '{table}' "
        f"AND partition_id = '{partition_id}' AND active = 1"
    ).strip()
    return int(res) if res else 0


# ---------------------------------------------------------------------------
# SELECT re-routing helpers (used by test_select.py)
# ---------------------------------------------------------------------------


def create_sr_table_with_cache_ttl(nodes, table, replication_factor=2):
    """Create a ReplicatedMergeTree table with SR enabled and a long cache TTL
    (3600s) to make stale-cache scenarios deterministic.
    """
    ddl = f"""
        CREATE TABLE {table}
        (
            p UInt8,
            v UInt64
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/s1/{table}', '{{replica}}')
        PARTITION BY p
        ORDER BY v
        SETTINGS replication_factor = {replication_factor},
                 selective_replication_assignment_cache_ttl_seconds = 3600
    """
    for node in nodes:
        node.query(ddl)


def drop_table_all_nodes(nodes, table):
    """Drop table on all nodes."""
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def prewarm_cache(node, table, partition_ids):
    """Force `node` to read current ZK assignments into its in-memory cache.
    A benign SELECT whose plan touches the partition id filter is enough;
    the SR routing path calls getAssignments(force_refresh=False).
    """
    pid_list = ",".join(str(p) for p in partition_ids)
    node.query(
        f"SELECT count() FROM {table} WHERE p IN ({pid_list})",
        settings={"allow_experimental_analyzer": 1},
    )


def refresh_cache_via_system_table(node, table):
    """Query system.selective_assignments to force a full cache refresh
    (getAssignments with force_refresh=true) on `node`.
    """
    node.query(
        f"SELECT * FROM system.selective_assignments WHERE database='default' AND table='{table}'",
        settings={"allow_experimental_analyzer": 1},
    )


def force_zk_assignment(zk, zk_path, partition_id, replicas):
    """Directly overwrite an /selective/assignments/<pid> node in ZK,
    simulating a completed migration that the cache has not yet picked up.
    """
    path = f"{zk_path}/selective/assignments/{partition_id}"
    data = "format version: 1\n" + ",".join(replicas)
    zk.set(path, data.encode())
