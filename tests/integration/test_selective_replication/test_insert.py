"""Integration tests for INSERT forwarding under selective replication."""

import pytest
import time
import threading

from .common import (
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
    get_row_count,
    get_cluster_rows,
    get_nodes_with_partition,
    create_table,
    drop_table,
    zk_suffix,
    find_unassigned,
    wait_for_cluster_rows,
    get_node_by_replica,
)


# ---------------------------------------------------------------------------
# 1. test_insert_forwarding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entry", ["direct", "via_distributed", "via_remote"])
def test_insert_forwarding(started_cluster, entry):
    """INSERT into a node whose replica is NOT assigned gets forwarded;
    each partition (rf=2) should appear on exactly 2 replicas.

    Parameterized by entry point:
      - direct: plain INSERT (distributed_depth=0, Phase 1 runs).
      - via_distributed: INSERT through a Distributed table (depth=1,
        Phase 1 runs after the selective replication depth fix).
      - via_remote: INSERT through remote() function (depth=1, same path).
    """
    suffix = zk_suffix()
    table = f"ins_fwd_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # INSERT 6 rows, each in a different month = 6 partitions
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, '{m}')"
            for m in range(1, 7)
        )
        if entry == "direct":
            node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        elif entry == "via_distributed":
            dist = f"dist_{table}"
            node1.query(
                f"CREATE TABLE {dist} (d Date, k UInt64, v String) "
                f"ENGINE = Distributed('test_sr_cluster', 'default', '{table}')",
                settings=NO_FAULT,
            )
            try:
                node1.query(f"INSERT INTO {dist} VALUES {values}", settings=NO_FAULT)
            finally:
                node1.query(f"DROP TABLE IF EXISTS {dist} SYNC", settings=NO_FAULT)
        else:  # via_remote
            node1.query(
                f"INSERT INTO FUNCTION remote('node1', 'default', '{table}') VALUES {values}",
                settings=NO_FAULT,
            )
        sync_all(table)

        # For non-direct entries, Phase 1 must have fired at depth > 0.
        if entry != "direct":
            found = any(
                n.grep_in_log("phase 1 pre-routing at distributed_depth=")
                for n in ALL_NODES
            )
            assert found, (
                f"[{entry}] Expected Phase 1 pre-routing log at distributed_depth > 0, "
                f"but no node emitted it"
            )

        # Verify total physical rows = 6 * 2 = 12 across all nodes
        assert get_cluster_rows(table) == 6 * rf, (
            f"Expected {6 * rf} physical rows, got {get_cluster_rows(table)}"
        )

        # Verify each partition is on exactly rf replicas
        for m in range(1, 7):
            pid = f"2024{m:02d}"
            owners = get_nodes_with_partition(table, pid)
            assert len(owners) == rf, (
                f"Partition {pid} on {len(owners)} nodes, expected {rf}: {owners}"
            )
    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# 2. test_async_insert_basic
# ---------------------------------------------------------------------------


def test_async_insert_basic(started_cluster):
    """Async INSERT with wait_for_async_insert=1 must be compatible with selective replication."""
    suffix = zk_suffix()
    table = f"async_basic_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # INSERT 4 rows using async_insert path.
        values = ", ".join(
            f"('2024-01-{day:02d}', {day}, 'v{day}')" for day in range(1, 5)
        )
        node1.query(
            f"INSERT INTO {table} VALUES {values}",
            settings={**NO_FAULT, "async_insert": 1, "wait_for_async_insert": 1},
        )
        sync_all(table)

        # All nodes must see 4 logical rows via SELECT routing.
        wait_for_cluster_rows(table, 4)

        # Physical rows across all replicas should equal logical_rows * rf.
        assert get_cluster_rows(table) == 4 * rf, (
            f"Expected {4 * rf} physical rows after async insert, got {get_cluster_rows(table)}"
        )
    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# 3. test_write_failover_on_replica_down
# ---------------------------------------------------------------------------


def test_write_failover_on_replica_down(started_cluster):
    """INSERT to a non-assigned replica must failover to other assigned replicas
    when the primary target is stopped.

    Setup: 3 replicas, rf=2. Pick a partition assigned to two replicas.
    Stop the primary target. INSERT from the non-assigned node must succeed
    by forwarding to the fallback assigned replica.
    """
    suffix = zk_suffix()
    table = f"wr_failover_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # Insert a row to establish partition assignment.
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-06-15', 100, 'seed')",
            settings=NO_FAULT,
        )
        sync_all(table)

        pid = "202406"
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf, f"Expected {rf} assigned replicas, got {assigned}"

        primary_replica = assigned[0]
        fallback_replica = assigned[1]
        primary_node = get_node_by_replica(primary_replica)
        fallback_node = get_node_by_replica(fallback_replica)
        unassigned_replica = (ALL_REPLICA_NAMES - set(assigned)).pop()
        insert_node = get_node_by_replica(unassigned_replica)

        # Stop the primary target replica.
        primary_node.stop_clickhouse()

        try:
            # INSERT from non-assigned node — must failover to fallback_replica.
            insert_node.query(
                f"INSERT INTO {table} VALUES ('2024-06-20', 200, 'failover')",
                settings=NO_FAULT,
            )
        finally:
            primary_node.start_clickhouse()
            time.sleep(3)
            sync_all(table)

        # Verify data landed on the fallback replica.
        count = int(
            fallback_node.query(
                f"SELECT count() FROM {table} WHERE k = 200",
                settings=NO_FAULT,
            ).strip()
        )
        assert count == 1, (
            f"Failover row not found on {fallback_replica} "
            f"after primary {primary_replica} was down"
        )

        # Verify full data visible from every node after primary recovery.
        for n in ALL_NODES:
            total = int(
                n.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
            )
            assert total == 2, (
                f"{n.name}: expected 2 rows after failover recovery, got {total}"
            )

    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# 4. test_write_all_assigned_replicas_down
# ---------------------------------------------------------------------------


def test_write_all_assigned_replicas_down(started_cluster):
    """INSERT must fail with ALL_REPLICAS_ARE_STALE when every assigned replica
    for the target partition is unavailable.

    Setup: 3 replicas, rf=2. Identify a partition NOT assigned to insert_node.
    Stop all assigned replicas. INSERT must raise ALL_REPLICAS_ARE_STALE.
    """
    suffix = zk_suffix()
    table = f"wr_all_down_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # Seed a row to create assignment.
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-08-15', 300, 'seed')",
            settings=NO_FAULT,
        )
        sync_all(table)

        pid = "202408"
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf, f"Expected {rf} assigned replicas, got {assigned}"

        unassigned_replica = (ALL_REPLICA_NAMES - set(assigned)).pop()
        insert_node = get_node_by_replica(unassigned_replica)

        # Stop ALL assigned replicas.
        stopped_nodes = [get_node_by_replica(r) for r in assigned]
        for n in stopped_nodes:
            n.stop_clickhouse()

        try:
            try:
                insert_node.query(
                    f"INSERT INTO {table} VALUES ('2024-08-20', 400, 'should_fail')",
                    settings=NO_FAULT,
                )
                raise AssertionError(
                    "Expected INSERT to fail with ALL_REPLICAS_ARE_STALE, but it succeeded"
                )
            except Exception as e:
                assert (
                    "ALL_REPLICAS_ARE_STALE" in str(e)
                    or "all assigned replicas" in str(e).lower()
                ), f"Expected ALL_REPLICAS_ARE_STALE error, got: {e}"
        finally:
            for n in stopped_nodes:
                n.start_clickhouse()
            time.sleep(3)
            sync_all(table)

    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# 5. test_write_reforward_on_assignment_change
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entry", ["direct", "via_distributed"])
def test_write_reforward_on_assignment_change(started_cluster, entry):
    """INSERT that arrives at a replica no longer assigned to the partition
    (because the assignment changed between allocation and commit) must
    automatically re-forward to the new assigned replica instead of failing.

    Setup: 3 replicas, rf=1. Insert a row, then manually migrate the
    partition to a different replica. Insert from the old replica — the
    write should be re-forwarded to the new assigned replica automatically.
    """
    suffix = zk_suffix()
    table = f"wr_refwd_{suffix}"
    rf = 1

    create_table(node1, table, suffix, rf=rf)

    try:
        # Insert a seed row to establish partition assignment.
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-07-15', 100, 'seed')",
            settings=NO_FAULT,
        )
        sync_all(table)

        pid = "202407"
        assigned = get_assigned(node1, table, pid)
        assert len(assigned) == rf, f"Expected {rf} assigned replica, got {assigned}"

        original_replica = assigned[0]
        original_node = get_node_by_replica(original_replica)

        # Pick a different replica as migration target.
        target_replica = (ALL_REPLICA_NAMES - {original_replica}).pop()
        target_node = get_node_by_replica(target_replica)

        # Manually migrate the partition to the target replica.
        node1.query(
            f"SYSTEM MIGRATE PARTITION '{pid}' OF {table} TO REPLICA '{target_replica}'",
            settings=NO_FAULT,
        )
        node1.query(f"SYSTEM SYNC SELECTIVE MIGRATIONS {table}", settings=NO_FAULT)

        # Wait for migration to complete and assignment to update.
        deadline = time.time() + 60
        while time.time() < deadline:
            new_assigned = get_assigned(node1, table, pid)
            if target_replica in new_assigned and original_replica not in new_assigned:
                break
            time.sleep(1)
        else:
            pytest.fail(f"Migration did not complete: assigned={get_assigned(node1, table, pid)}")

        # Now INSERT from the original (no longer assigned) replica.
        # The write should be re-forwarded to the new assigned replica.
        if entry == "direct":
            original_node.query(
                f"INSERT INTO {table} VALUES ('2024-07-20', 200, 'reforwarded')",
                settings=NO_FAULT,
            )
        else:  # via_distributed
            dist = f"dist_{table}"
            original_node.query(
                f"CREATE TABLE {dist} (d Date, k UInt64, v String) "
                f"ENGINE = Distributed('test_sr_cluster', 'default', '{table}')",
                settings=NO_FAULT,
            )
            try:
                original_node.query(
                    f"INSERT INTO {dist} VALUES ('2024-07-20', 200, 'reforwarded')",
                    settings=NO_FAULT,
                )
            finally:
                original_node.query(f"DROP TABLE IF EXISTS {dist} SYNC", settings=NO_FAULT)

        sync_all(table)

        # Verify the re-forwarded row is visible on the target replica.
        count = int(
            target_node.query(
                f"SELECT count() FROM {table} WHERE k = 200",
                settings=NO_FAULT,
            ).strip()
        )
        assert count == 1, (
            f"Re-forwarded row not found on {target_replica}, "
            f"expected 1, got {count}\n"
            f"original_replica={original_replica} target_replica={target_replica}\n"
            f"assigned={get_assigned(node1, table, pid)}"
        )

        # Verify all rows are visible from every node via SELECT routing.
        for n in ALL_NODES:
            total = int(
                n.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
            )
            assert total == 2, f"{n.name}: expected 2 rows, got {total}"

    finally:
        drop_table(node1, table)
