"""Integration tests for SELECT routing and re-routing under selective replication."""

import pytest
import time

from helpers.client import QueryRuntimeException

from .common import (
    cluster,
    started_cluster,
    node1,
    node2,
    node3,
    ALL_NODES,
    ALL_REPLICA_NAMES,
    REPLICA_TO_NODE,
    NO_FAULT,
    wait_for_sync,
    sync_all,
    get_assigned,
    create_table,
    drop_table,
    zk_suffix,
    wait_for_cluster_rows,
    get_node_by_replica,
    create_sr_table_with_cache_ttl,
    drop_table_all_nodes,
    prewarm_cache,
    refresh_cache_via_system_table,
    force_zk_assignment,
    get_metric,
)


# ---------------------------------------------------------------------------
# SELECT routing tests (from test.py)
# ---------------------------------------------------------------------------


def test_select_routing_basic(started_cluster):
    """SELECT from any node returns the full dataset regardless of local assignment."""
    suffix = zk_suffix()
    table = f"sel_route_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # INSERT 8 rows (8 partitions)
        values = ", ".join(
            f"('2024-{m:02d}-10', {m * 10}, 'v{m}')"
            for m in range(1, 9)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        # Every node should see all 8 rows
        for node in ALL_NODES:
            count = int(
                node.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
            )
            assert count == 8, (
                f"{node.name}: expected 8 rows, got {count}"
            )

            total_k = int(
                node.query(
                    f"SELECT sum(k) FROM {table}", settings=NO_FAULT
                ).strip()
            )
            expected_k = sum(m * 10 for m in range(1, 9))
            assert total_k == expected_k, (
                f"{node.name}: expected sum(k)={expected_k}, got {total_k}"
            )

            # WHERE filter
            filtered = int(
                node.query(
                    f"SELECT count() FROM {table} WHERE k > 30", settings=NO_FAULT
                ).strip()
            )
            assert filtered == 5, (
                f"{node.name}: expected 5 rows with k>30, got {filtered}"
            )
    finally:
        drop_table(node1, table)


def test_select_routing_partition_pruning(started_cluster):
    """SELECT with partition-key filter should route correctly."""
    suffix = zk_suffix()
    table = f"sel_prune_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        values = ", ".join(
            f"('2024-{m:02d}-05', {m}, 'p{m}')"
            for m in range(1, 13)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        # Query by single month on each node
        for node in ALL_NODES:
            # PREWHERE on partition key
            count_jan = int(
                node.query(
                    f"SELECT count() FROM {table} "
                    f"WHERE d >= '2024-01-01' AND d < '2024-02-01'",
                    settings=NO_FAULT,
                ).strip()
            )
            assert count_jan == 1, f"{node.name}: expected 1 row for Jan, got {count_jan}"

            # GROUP BY partition month
            months = (
                node.query(
                    f"SELECT toYYYYMM(d), count() FROM {table} "
                    f"GROUP BY toYYYYMM(d) ORDER BY toYYYYMM(d)",
                    settings=NO_FAULT,
                )
                .strip()
                .split("\n")
            )
            assert len(months) == 12, (
                f"{node.name}: expected 12 months, got {len(months)}"
            )
    finally:
        drop_table(node1, table)


def test_select_routing_ddl(started_cluster):
    """DROP PARTITION and OPTIMIZE work correctly with selective replication."""
    suffix = zk_suffix()
    table = f"sel_ddl_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        node1.query(
            f"INSERT INTO {table} VALUES "
            f"('2024-01-15', 1, 'a'), ('2024-02-10', 2, 'b'), ('2024-03-05', 3, 'c')",
            settings=NO_FAULT,
        )
        sync_all(table)

        # Initial INSERT: all nodes must see 3 rows via SELECT routing
        wait_for_cluster_rows(table, 3)
        node1.query(
            f"ALTER TABLE {table} DROP PARTITION '202401'",
            settings=NO_FAULT,
        )
        sync_all(table)

        # DROP PARTITION 202401: all nodes must see 2 rows via SELECT routing
        wait_for_cluster_rows(table, 2)

        # INSERT new data after drop
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-04-01', 4, 'd')", settings=NO_FAULT
        )
        sync_all(table)

        # New partition may not be in assignment cache on all nodes yet;
        # wait for SELECT routing to pick it up.
        wait_for_cluster_rows(table, 3)
    finally:
        drop_table(node1, table)


def test_select_failover_on_replica_down(started_cluster):
    """SELECT from a node whose non-local partition has one assigned replica down
    should still succeed by routing to the other assigned replica.

    Setup: 3 replicas, rf=2. Find a partition NOT assigned to node1.
    Stop one of its assigned replicas. SELECT from node1 must still return
    all rows by routing to the remaining assigned replica.
    """
    suffix = zk_suffix()
    table = f"sel_failover_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        values = ", ".join(
            f"('2024-{m:02d}-10', {m}, 'v{m}')"
            for m in range(1, 7)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        # Find a partition NOT assigned to r1 (node1)
        result = node1.query(
            f"SELECT partition_id, assigned_replicas "
            f"FROM system.selective_assignments "
            f"WHERE database = 'default' AND table = '{table}' "
            f"AND NOT has(assigned_replicas, 'r1') "
            f"ORDER BY partition_id LIMIT 1",
            settings=NO_FAULT,
        ).strip()

        if not result:
            pytest.skip("No partition found that excludes r1")

        pid = result.split("\t")[0]
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf

        # Stop one of the assigned replicas (not node1, which is the query node)
        stopped_replica = assigned[0] if assigned[0] != "r1" else assigned[1]
        stopped_node = get_node_by_replica(stopped_replica)
        stopped_node.stop_clickhouse()

        try:
            # SELECT from node1 must still return all 6 rows
            count = int(
                node1.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
            )
            assert count == 6, (
                f"Expected 6 rows with one replica down, got {count}"
            )
        finally:
            stopped_node.start_clickhouse()
            time.sleep(3)
            sync_all(table)

    finally:
        drop_table(node1, table)


def test_select_all_assigned_replicas_down(started_cluster):
    """SELECT from a node whose non-local partition has ALL assigned replicas
    down should raise ALL_REPLICAS_LOST.

    With skip_unavailable_shards=1, the query succeeds but returns fewer rows.
    """
    suffix = zk_suffix()
    table = f"sel_all_down_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        values = ", ".join(
            f"('2024-{m:02d}-10', {m}, 'v{m}')"
            for m in range(1, 7)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        # Find a partition NOT assigned to r1 (node1)
        result = node1.query(
            f"SELECT partition_id, assigned_replicas "
            f"FROM system.selective_assignments "
            f"WHERE database = 'default' AND table = '{table}' "
            f"AND NOT has(assigned_replicas, 'r1') "
            f"ORDER BY partition_id LIMIT 1",
            settings=NO_FAULT,
        ).strip()

        if not result:
            pytest.skip("No partition found that excludes r1")

        pid = result.split("\t")[0]
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf

        # Stop ALL assigned replicas
        stopped_nodes = [get_node_by_replica(r) for r in assigned]
        for n in stopped_nodes:
            n.stop_clickhouse()

        try:
            # Default: must raise ALL_REPLICAS_LOST or ALL_CONNECTION_TRIES_FAILED.
            got_expected_error = False
            for attempt in range(2):
                try:
                    node1.query(
                        f"SELECT count() FROM {table}", settings=NO_FAULT
                    )
                    raise AssertionError("Expected ALL_REPLICAS_LOST, but query succeeded")
                except Exception as e:
                    if "ALL_REPLICAS_LOST" in str(e) or "no reachable assigned replica" in str(e).lower():
                        got_expected_error = True
                        break
                    if "ALL_CONNECTION_TRIES_FAILED" in str(e):
                        got_expected_error = True
                        break
                    # Network-level error on first try — wait and retry so the
                    # routing layer has a chance to detect the failure.
                    if attempt == 0 and "NETWORK_ERROR" in str(e):
                        time.sleep(2)
                        continue
                    raise AssertionError(
                        f"Expected ALL_REPLICAS_LOST or ALL_CONNECTION_TRIES_FAILED, got: {e}"
                    )
            assert got_expected_error, "SELECT should have raised ALL_REPLICAS_LOST or ALL_CONNECTION_TRIES_FAILED"

            # With skip_unavailable_shards=1: succeeds, returns fewer than 6 rows
            count = int(
                node1.query(
                    f"SELECT count() FROM {table}",
                    settings={**NO_FAULT, "skip_unavailable_shards": "1"},
                ).strip()
            )
            assert count < 6, (
                f"Expected fewer than 6 rows with skip_unavailable_shards=1, got {count}"
            )
        finally:
            for n in stopped_nodes:
                n.start_clickhouse()
            time.sleep(3)
            sync_all(table)

    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# SELECT read fallback tests (from test.py)
# ---------------------------------------------------------------------------


def test_read_fallback_on_assignment_change(started_cluster):
    """A forwarded SELECT sub-query that lands on a replica no longer assigned
    to the requested partition must automatically re-route to the correct
    assigned replica via the read fallback mechanism.

    Setup: 3 replicas, rf=1. Insert data, migrate partition to a different
    replica. After cache refresh, SELECT from the old replica must still
    return the correct data by re-routing.
    """
    suffix = zk_suffix()
    table = f"rd_fallback_{suffix}"
    rf = 1

    create_table(node1, table, suffix, rf=rf)

    try:
        # Insert rows across multiple partitions.
        values = ", ".join(
            f"('2024-08-{d:02d}', {d}, 'v{d}')"
            for d in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        pid = "202408"
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf, f"Expected {rf} assigned replica, got {assigned}"

        original_replica = assigned[0]
        original_node = get_node_by_replica(original_replica)

        # Pick a different replica as migration target.
        other_replicas = ALL_REPLICA_NAMES - {original_replica}
        target_replica = other_replicas.pop()
        target_node = get_node_by_replica(target_replica)

        # Migrate the partition.
        node1.query(
            f"SYSTEM MIGRATE PARTITION '{pid}' OF {table} TO REPLICA '{target_replica}'",
            settings=NO_FAULT,
        )
        node1.query(f"SYSTEM SYNC SELECTIVE MIGRATIONS {table}", settings=NO_FAULT)

        # Wait for migration to complete.
        deadline = time.time() + 60
        while time.time() < deadline:
            new_assigned = get_assigned(node1, table, pid)
            if target_replica in new_assigned and original_replica not in new_assigned:
                break
            time.sleep(1)
        else:
            pytest.fail(f"Migration did not complete: assigned={get_assigned(node1, table, pid)}")

        # Force-refresh assignment cache on all nodes so read routing uses fresh assignments.
        for n in ALL_NODES:
            try:
                n.query("SELECT 1 FROM system.selective_assignments LIMIT 1", settings=NO_FAULT)
            except Exception:
                pass

        # All nodes should return 3 rows via SELECT routing.
        for n in ALL_NODES:
            total = int(
                n.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
            )
            assert total == 3, f"{n.name}: expected 3 rows after migration, got {total}"

    finally:
        drop_table(node1, table)


def test_read_fallback_via_distributed_table(started_cluster):
    """A Distributed table query that forwards to a replica no longer assigned
    to the requested partition must re-route via the read fallback mechanism.

    Setup: 3 replicas, rf=1. Insert data, create a Distributed table,
    migrate partition away. Verify the Distributed table query still returns
    correct data via read fallback (depth=1 < MAX_FORWARDING_DEPTH).
    """
    suffix = zk_suffix()
    table = f"rd_dist_{suffix}"
    dist_table = f"dist_rd_dist_{suffix}"
    rf = 1

    create_table(node1, table, suffix, rf=rf)

    try:
        # Insert a row to create a partition assignment.
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-09-15', 1, 'test')",
            settings=NO_FAULT,
        )
        sync_all(table)

        pid = "202409"
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf

        original_replica = assigned[0]
        original_node = get_node_by_replica(original_replica)

        # Create a Distributed table on the original replica.
        original_node.query(
            f"CREATE TABLE {dist_table} (d Date, k UInt64, v String) "
            f"ENGINE = Distributed('test_sr_cluster', 'default', '{table}')",
            settings=NO_FAULT,
        )

        # Verify normal read via Distributed table works before migration.
        count = int(
            original_node.query(
                f"SELECT count() FROM {dist_table}", settings=NO_FAULT
            ).strip()
        )
        assert count == 1, f"Expected 1 row before migration, got {count}"

        # Migrate partition away from original replica.
        target_replica = (ALL_REPLICA_NAMES - {original_replica}).pop()
        node1.query(
            f"SYSTEM MIGRATE PARTITION '{pid}' OF {table} TO REPLICA '{target_replica}'",
            settings=NO_FAULT,
        )
        node1.query(f"SYSTEM SYNC SELECTIVE MIGRATIONS {table}", settings=NO_FAULT)

        # Wait for migration to complete.
        deadline = time.time() + 60
        while time.time() < deadline:
            new_assigned = get_assigned(node1, table, pid)
            if target_replica in new_assigned and original_replica not in new_assigned:
                break
            time.sleep(1)
        else:
            pytest.fail("Migration did not complete")

        # Force-refresh assignment cache on original_node so read routing uses fresh assignments.
        try:
            original_node.query("SELECT 1 FROM system.selective_assignments LIMIT 1", settings=NO_FAULT)
        except Exception:
            pass

        # Verify the Distributed table also works (read fallback kicks in
        # at depth=1, which is < MAX_FORWARDING_DEPTH).
        count = int(
            original_node.query(
                f"SELECT count() FROM {dist_table}", settings=NO_FAULT
            ).strip()
        )
        assert count == 1, (
            f"Expected 1 row via Distributed table after migration, got {count}"
        )

    finally:
        try:
            original_node.query(f"DROP TABLE IF EXISTS {dist_table} SYNC", settings=NO_FAULT)
        except Exception:
            pass
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# SELECT re-routing tests (from test_select_reroute.py)
# ---------------------------------------------------------------------------


def _get_sr_nodes():
    """Return the node instances in the standard order."""
    return [cluster.instances[n] for n in ("node1", "node2", "node3")]


def test_select_misplaced_partition_reroute_single(started_cluster):
    """Single partition migrated away from the routing target.

    Setup (deterministic for this build):
      - p=2 with rf=2 is initially assigned to [node1, node2].
      - Node3 (initiator) is not in the assignment.
      - Node3 primes its cache with the pre-migration view [node1, node2].
      - ZK is mutated so p=2 now lives on [node2, node3].
      - Node1 refreshes its cache via system.selective_assignments.

    Flow:
      - depth=0 on node3: stale cache says p=2 -> node1, node2.
        Node3 is not in the list, hash routes to node1.
      - depth=1 on node1: fresh cache says p=2 -> node2, node3.
        Node1 is no longer assigned, re-routes to node2.
      - depth=2 on node2: cold cache, falls through to local read.
        Node2 has data from the original allocation.
    """
    table = "t_reroute_single"
    zk_path = f"/clickhouse/tables/s1/{table}"
    nodes = _get_sr_nodes()
    n1, n2, n3 = nodes

    try:
        create_sr_table_with_cache_ttl(nodes, table, replication_factor=2)

        n1.query(f"INSERT INTO {table} (p, v) VALUES (2, 100)")
        for n in nodes:
            wait_for_sync(n, table)

        # Prime node3's cache with the pre-migration assignment.
        prewarm_cache(n3, table, [2])

        # Simulate migration: p=2 moves from [node1, node2] to [node2, node3].
        zk = cluster.get_kazoo_client("zoo1")
        force_zk_assignment(zk, zk_path, "2", ["node2", "node3"])

        # Refresh node1's cache so it sees the post-migration state.
        refresh_cache_via_system_table(n1, table)

        result = n3.query(
            f"SELECT count() FROM {table} WHERE p = 2",
            settings={"allow_experimental_analyzer": 1},
        )
        assert result.strip() == "1", f"expected 1, got {result!r}"
    finally:
        drop_table_all_nodes(nodes, table)


def test_select_full_table_during_migration(started_cluster):
    """Multi-partition table with a subset migrated. Full-table SELECT
    must return the correct total count via mixed local + re-routed paths.

    Setup:
      - p=1 -> [node1, node3] (hash=2, deterministic)
      - p=2 -> [node1, node2] (hash=0, deterministic)
      - p=3 -> [node1, node3] (hash=2, deterministic)
      - Node3 primes its cache with pre-migration assignments.
      - Mutate ZK:
          p=1 -> [node1, node2]   (node3 still has data, reads local)
          p=2 -> [node2, node3]   (node3 no longer in stale cache, re-routed)
          p=3 unchanged
      - Node1 refreshes its cache.
    """
    table = "t_reroute_full"
    zk_path = f"/clickhouse/tables/s1/{table}"
    nodes = _get_sr_nodes()
    n1, n2, n3 = nodes

    try:
        create_sr_table_with_cache_ttl(nodes, table, replication_factor=2)

        n1.query(f"INSERT INTO {table} (p, v) VALUES (1, 10), (2, 20), (3, 30)")
        for n in nodes:
            wait_for_sync(n, table)

        prewarm_cache(n3, table, [1, 2, 3])

        zk = cluster.get_kazoo_client("zoo1")
        force_zk_assignment(zk, zk_path, "1", ["node1", "node2"])
        force_zk_assignment(zk, zk_path, "2", ["node2", "node3"])

        refresh_cache_via_system_table(n1, table)

        result = n3.query(
            f"SELECT count() FROM {table}",
            settings={"allow_experimental_analyzer": 1},
        )
        assert result.strip() == "3", f"expected 3, got {result!r}"
    finally:
        drop_table_all_nodes(nodes, table)


def test_select_depth_exhaustion(started_cluster):
    """Pathological cache chain forces ping-pong re-routing until
    MAX_FORWARDING_DEPTH catches it and raises TOO_LARGE_DISTRIBUTED_DEPTH.

    The test uses single-replica assignments to eliminate hash-based
    routing ambiguity. Each hop has exactly one possible destination.

    Setup:
      - Force ZK assignment: p=1 -> [node1] (single replica, stale view).
      - Refresh node2 and node3 caches with this stale [node1] view.
      - Mutate ZK: p=1 -> [node2] (new owner, still single replica).
      - Refresh node1 cache via system.selective_assignments.
        Now: n1.cache = [node2] (fresh), n2.cache = [node1] (stale),
             n3.cache = [node1] (stale).

    Flow from n3.query():
      - depth=0 on node3: stale [node1] -> route to node1 (sole target).
      - depth=1 on node1: fresh [node2] -> node1 misplaced -> route to node2.
      - depth=2 on node2: stale [node1] -> node2 misplaced -> route to node1.
      - depth=3 on node1: fresh [node2] -> re-route to node2.
      - ... bounced until depth reaches MAX_FORWARDING_DEPTH (5).
    """
    table = "t_reroute_depth"
    zk_path = f"/clickhouse/tables/s1/{table}"
    nodes = _get_sr_nodes()
    n1, n2, n3 = nodes

    try:
        create_sr_table_with_cache_ttl(nodes, table, replication_factor=2)

        n1.query(f"INSERT INTO {table} (p, v) VALUES (1, 100)")
        for n in nodes:
            wait_for_sync(n, table)

        # Force single-replica stale view: p=1 -> [r1].
        zk = cluster.get_kazoo_client("zoo1")
        force_zk_assignment(zk, zk_path, "1", ["r1"])

        # Force-refresh node2 and node3 caches so they pick up the [r1] view.
        refresh_cache_via_system_table(n2, table)
        refresh_cache_via_system_table(n3, table)

        # Mutate ZK to the post-migration single-replica owner: [r2].
        force_zk_assignment(zk, zk_path, "1", ["r2"])

        # Refresh node1 cache so it sees the fresh [node2] view.
        refresh_cache_via_system_table(n1, table)

        with pytest.raises(QueryRuntimeException) as exc_info:
            n3.query(
                f"SELECT count() FROM {table} WHERE p = 1",
                settings={"allow_experimental_analyzer": 1},
            )
        msg = str(exc_info.value)
        assert "TOO_LARGE_DISTRIBUTED_DEPTH" in msg or "exceeded max forwarding depth" in msg, (
            f"expected depth-exhaustion error, got: {msg}"
        )
    finally:
        drop_table_all_nodes(nodes, table)


def test_select_all_owned_no_reroute(started_cluster):
    """Happy-path regression: no ZK mutation, no stale cache, no metric bump."""
    table = "t_reroute_happy"
    nodes = _get_sr_nodes()
    n1, n3 = nodes[0], nodes[2]

    try:
        create_sr_table_with_cache_ttl(nodes, table, replication_factor=2)

        n1.query(f"INSERT INTO {table} (p, v) VALUES (1, 1)")
        for n in nodes:
            wait_for_sync(n, table)

        baseline = get_metric(n1, "SelectiveReplicationSelectForwardedPartitions")

        result = n3.query(
            f"SELECT count() FROM {table} WHERE p = 1",
            settings={"allow_experimental_analyzer": 1},
        )
        assert result.strip() == "1", f"expected 1, got {result!r}"

        final = get_metric(n1, "SelectiveReplicationSelectForwardedPartitions")
        assert final == baseline, (
            f"expected no metric increment, baseline={baseline}, final={final}"
        )
    finally:
        drop_table_all_nodes(nodes, table)


def test_select_partial_misplacement(started_cluster):
    """Two partitions: one still owned locally, one misplaced and re-routed.
    UnionStep merges the two results.

    Setup:
      - p=1 -> [node1, node3]
      - p=2 -> [node1, node2]
      - Node3 primes cache.
      - Mutate only p=2 -> [node2, node3].
      - Node1 refreshes cache.

    Flow:
      - depth=0 on node3:
          p=1: node3 in stale cache [node1, node3] -> local read.
          p=2: node3 not in stale cache [node1, node2] -> route to node1.
      - depth=1 on node1:
          p=2 fresh cache [node2, node3] -> node1 not assigned -> re-route to node2.
      - depth=2 on node2: cold cache, local read.
    """
    table = "t_reroute_partial"
    zk_path = f"/clickhouse/tables/s1/{table}"
    nodes = _get_sr_nodes()
    n1, n2, n3 = nodes

    try:
        create_sr_table_with_cache_ttl(nodes, table, replication_factor=2)

        n1.query(f"INSERT INTO {table} (p, v) VALUES (1, 100), (2, 200)")
        for n in nodes:
            wait_for_sync(n, table)

        prewarm_cache(n3, table, [1, 2])

        zk = cluster.get_kazoo_client("zoo1")
        force_zk_assignment(zk, zk_path, "2", ["node2", "node3"])

        refresh_cache_via_system_table(n1, table)

        result = n3.query(
            f"SELECT count() FROM {table} WHERE p IN (1, 2)",
            settings={"allow_experimental_analyzer": 1},
        )
        assert result.strip() == "2", f"expected 2, got {result!r}"
    finally:
        drop_table_all_nodes(nodes, table)
