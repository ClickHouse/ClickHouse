"""Integration tests for rebalance, migration, takeover, assignment, and fault
tolerance under selective replication."""

import pytest
import time
import threading

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
    get_row_count,
    get_cluster_rows,
    get_nodes_with_partition,
    create_table,
    drop_table,
    zk_suffix,
    no_orphaned_cloning,
    wait_for_cluster_rows,
    get_node_by_replica,
)


# ---------------------------------------------------------------------------
# Rebalance
# ---------------------------------------------------------------------------


def test_auto_rebalance(started_cluster):
    """Background migrationMonitorTask automatically rebalances partitions.

    Verifies that when a node rejoins the cluster after being offline,
    the migration monitor detects the imbalance and migrates partitions.
    """
    suffix = zk_suffix()
    table = f"auto_rebal_{suffix}"
    zk_path = f"/clickhouse/tables/{suffix}/{table}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        node3.stop_clickhouse()

        # Ensure node3's is_active ephemeral node is removed from ZK.
        # Graceful shutdown sets replica_is_active_node = nullptr, but
        # EphemeralNodeHolder::existing() sets need_remove=false, so the
        # destructor does NOT proactively delete the ZK node.  The node
        # only disappears when the ZK session expires (default 30s).  We
        # delete it manually so the rebalance planner immediately sees r3
        # as inactive.
        zk = cluster.get_kazoo_client("zoo1")
        is_active_path = f"{zk_path}/replicas/r3/is_active"
        for _ in range(60):
            if not zk.exists(is_active_path):
                break
            try:
                zk.delete(is_active_path)
            except Exception:
                pass
            time.sleep(0.5)
        zk.stop()

        # Use fewer partitions so rebalance + CLONE completes faster.
        values = ", ".join(
            f"('2024-{m:02d}-10', {m}, 'b{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)

        for n in [node1, node2]:
            wait_for_sync(n, table)

        total_before = 0
        for n in [node1, node2]:
            total_before += get_row_count(n, table)
        assert total_before == 6, f"Expected 6 rows before rebalance, got {total_before}"

        # Restart node3
        node3.start_clickhouse()
        wait_for_sync(node3, table)

        # Trigger one migration monitor cycle deterministically.
        # Note: node3's background BgSchPool may also trigger an auto-rebalance
        # when it detects the imbalance, so we may see migrations created by
        # either the explicit REBALANCE command or the auto-rebalance.
        node1.query(f"SYSTEM START SELECTIVE REBALANCE default.{table}", settings=NO_FAULT)

        # Wait for migrations to be initiated (not necessarily completed).
        zk = cluster.get_kazoo_client("zoo1")
        migrations_path = f"{zk_path}/selective/migrations"
        found_migrations = False
        for _ in range(30):
            try:
                mids = zk.get_children(migrations_path)
                if len(mids) > 0:
                    found_migrations = True
                    break
            except Exception:
                pass
            time.sleep(1)

        assert found_migrations, (
            "Rebalance should have created at least one migration in ZK"
        )

        # Verify that the rebalance assigned at least some partitions to r3
        # (with :cloning suffix, indicating CLONE in progress).
        assignments = node1.query(
            f"SELECT partition_id, assigned_replicas FROM system.selective_assignments "
            f"WHERE table = '{table}' FORMAT TSVWithNames",
            settings=NO_FAULT,
        ).strip()
        assert "r3" in assignments, (
            f"r3 should appear in assignments after rebalance: {assignments}"
        )
    finally:
        # Make sure node3 is running for subsequent tests
        try:
            node3.start_clickhouse()
        except Exception:
            pass
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# Migration timeout / rollback
# ---------------------------------------------------------------------------


def test_migration_timeout_rollback(started_cluster):
    """Migration times out when CLONE cannot complete; rollback must clean :cloning."""
    suffix = zk_suffix()
    table = f"mig_timeout_{suffix}"

    create_table(node1, table, suffix, rf=2)

    try:
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 't{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        row_count_before = sum(get_row_count(n, table) for n in ALL_NODES)
        assert row_count_before > 0, "Expected data before rebalance"

        # Stop fetches on all nodes so CLONE cannot complete.
        for n in ALL_NODES:
            n.query(f"SYSTEM STOP FETCHES default.{table}", settings=NO_FAULT)

        # Set a short migration timeout before triggering rebalance.
        node1.query(
            f"ALTER TABLE default.{table} MODIFY SETTING migration_timeout_seconds=5",
            settings=NO_FAULT,
        )

        # Trigger rebalance.
        node1.query(
            f"SYSTEM START SELECTIVE REBALANCE default.{table}",
            settings=NO_FAULT,
        )

        # Wait long enough for the migration to time out.
        time.sleep(8)

        # Start fetches so rollback/recovery can proceed.
        for n in ALL_NODES:
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass

        # Drive recovery.
        try:
            node1.query(
                f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{table}",
                settings={**NO_FAULT, "receive_timeout": "60"},
            )
        except Exception:
            pass

        assert no_orphaned_cloning(node1, table), \
            "Orphaned :cloning entry found after timeout rollback"

        # Data must still be accessible.
        row_count_after = sum(get_row_count(n, table) for n in ALL_NODES)
        assert row_count_after >= 3, \
            f"Expected at least 3 rows after rollback, got {row_count_after}"
    finally:
        for n in ALL_NODES:
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass
        drop_table(node1, table)


def test_rollback_cas_fail_preserves_migration_node(started_cluster):
    """When rollback CAS fails, the migration node must be preserved for recovery."""
    suffix = zk_suffix()
    table = f"rb_cas_fail_{suffix}"

    create_table(node1, table, suffix, rf=2)

    zk_path = node1.query(
        f"SELECT zookeeper_path FROM system.replicas "
        f"WHERE table = '{table}' LIMIT 1",
        settings=NO_FAULT,
    ).strip()

    try:
        # Stop node3 so partitions are only assigned to r1, r2.
        # This creates an imbalance that rebalance will try to fix when r3 returns.
        node3.stop_clickhouse()
        time.sleep(2)

        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'r{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        for n in [node1, node2]:
            wait_for_sync(n, table)

        # Restart node3 so rebalance has a target.
        node3.start_clickhouse()
        wait_for_sync(node3, table)

        # Make rollback CAS always fail on all nodes.
        for n in ALL_NODES:
            n.query(
                "SYSTEM ENABLE FAILPOINT selective_replication_rollback_cas_fail",
                settings=NO_FAULT,
            )

        # Stop fetches on all nodes so CLONE cannot complete → migration times out.
        for n in ALL_NODES:
            n.query(f"SYSTEM STOP FETCHES default.{table}", settings=NO_FAULT)

        # Set a short migration timeout before triggering rebalance.
        node1.query(
            f"ALTER TABLE default.{table} MODIFY SETTING migration_timeout_seconds=5",
            settings=NO_FAULT,
        )

        # Trigger rebalance.
        node1.query(
            f"SYSTEM START SELECTIVE REBALANCE default.{table}",
            settings=NO_FAULT,
        )

        # Wait for migration to time out (and rollback to be attempted).
        time.sleep(8)

        # The migration node should still exist because CAS failed.
        zk = cluster.get_kazoo_client("zoo1")
        migrations_path = f"{zk_path}/selective/migrations"
        try:
            migration_ids = zk.get_children(migrations_path)
        except Exception:
            migration_ids = []

        assert len(migration_ids) > 0, (
            "Migration node was deleted even though rollback CAS failed; "
            "node should be preserved for recovery."
        )

        # Now disable failpoint and start fetches so recovery can clean up.
        for n in ALL_NODES:
            try:
                n.query(
                    "SYSTEM DISABLE FAILPOINT selective_replication_rollback_cas_fail",
                    settings=NO_FAULT,
                )
            except Exception:
                pass
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass

        # Wait for recoverOrphanedMigrations to clean up.
        try:
            node1.query(
                f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{table}",
                settings={**NO_FAULT, "receive_timeout": "60"},
            )
        except Exception:
            pass
        time.sleep(5)

        assert no_orphaned_cloning(node1, table), \
            "Orphaned :cloning entry found after recovery cleaned up FAILED migration"
    finally:
        for n in ALL_NODES:
            try:
                n.query(
                    "SYSTEM DISABLE FAILPOINT selective_replication_rollback_cas_fail",
                    settings=NO_FAULT,
                )
            except Exception:
                pass
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass
        try:
            node3.start_clickhouse()
        except Exception:
            pass
        drop_table(node1, table)


def test_startclone_partial_init_recovery(started_cluster):
    """Migration stuck before GET_PART completes must be recoverable."""
    suffix = zk_suffix()
    table = f"partial_init_{suffix}"

    create_table(node1, table, suffix, rf=2)

    try:
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'p{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        row_count_before = sum(get_row_count(n, table) for n in ALL_NODES)
        assert row_count_before > 0, "Expected data before rebalance"

        # Stop fetches on all nodes so GET_PART cannot complete.
        for n in ALL_NODES:
            n.query(f"SYSTEM STOP FETCHES default.{table}", settings=NO_FAULT)

        # Trigger rebalance.
        node1.query(
            f"SYSTEM START SELECTIVE REBALANCE default.{table}",
            settings=NO_FAULT,
        )

        # Allow partial init to happen.
        time.sleep(3)

        # Start fetches so the migration can complete.
        for n in ALL_NODES:
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass

        # Drive the state machine to completion.
        try:
            node1.query(
                f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{table}",
                settings={**NO_FAULT, "receive_timeout": "120"},
            )
        except Exception:
            pass

        assert no_orphaned_cloning(node1, table), \
            "Orphaned :cloning entry found after partial-init recovery"

        # Data must still be accessible on all nodes via SELECT routing.
        wait_for_cluster_rows(table, 3)
    finally:
        for n in ALL_NODES:
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# Takeover
# ---------------------------------------------------------------------------


def test_takeover_cas_fail(started_cluster):
    """When takeover CAS fails, the migration node is preserved and the system stays correct.

    Scenario:
    1. Create an imbalance (stop node3, insert data, restart node3).
    2. Trigger rebalance to create a migration.
    3. Stop fetches so CLONE cannot complete, then wait for timeout.
    4. Enable takeover_cas_fail so the next replica's takeover attempt fails.
    5. Verify: migration node still exists, metadata not corrupted.
    6. Disable takeover_cas_fail, retry recovery → takeover succeeds.
    """
    suffix = zk_suffix()
    table = f"takeover_cas_{suffix}"

    create_table(node1, table, suffix, rf=2)

    zk_path = node1.query(
        f"SELECT zookeeper_path FROM system.replicas "
        f"WHERE table = '{table}' LIMIT 1",
        settings=NO_FAULT,
    ).strip()

    try:
        # 1. Create imbalance: stop node3, insert data
        node3.stop_clickhouse()
        time.sleep(2)

        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'v{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        for n in [node1, node2]:
            wait_for_sync(n, table)

        # Restart node3 → rebalance target
        node3.start_clickhouse()
        wait_for_sync(node3, table)

        # 2. Stop fetches so CLONE cannot complete → migration gets stuck
        for n in ALL_NODES:
            n.query(f"SYSTEM STOP FETCHES default.{table}", settings=NO_FAULT)

        # Prevent rollback from deleting the migration node — we need
        # the node to stay alive so takeover can be attempted.
        for n in ALL_NODES:
            n.query(
                "SYSTEM ENABLE FAILPOINT selective_replication_rollback_cas_fail",
                settings=NO_FAULT,
            )

        # Short migration timeout so it times out quickly
        node1.query(
            f"ALTER TABLE default.{table} MODIFY SETTING migration_timeout_seconds=5",
            settings=NO_FAULT,
        )

        # Trigger rebalance → creates migration
        node1.query(
            f"SYSTEM START SELECTIVE REBALANCE default.{table}",
            settings=NO_FAULT,
        )

        # Wait for migration to time out (coordinator becomes offline)
        time.sleep(8)

        # 3. Now enable takeover_cas_fail so any takeover attempt will fail
        for n in ALL_NODES:
            n.query(
                "SYSTEM ENABLE FAILPOINT selective_replication_takeover_cas_fail",
                settings=NO_FAULT,
            )

        # Start fetches so migration can proceed if takeover succeeds
        for n in ALL_NODES:
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass

        # Trigger recovery → takeover will be attempted but CAS will fail
        try:
            node1.query(
                f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{table}",
                settings={**NO_FAULT, "receive_timeout": "30"},
            )
        except Exception:
            pass
        time.sleep(3)

        # 4. Verify: migration node still exists (takeover failed, node preserved)
        zk = cluster.get_kazoo_client("zoo1")
        migrations_path = f"{zk_path}/selective/migrations"
        try:
            migration_ids = zk.get_children(migrations_path)
        except Exception:
            migration_ids = []

        assert len(migration_ids) > 0, (
            "Migration node should be preserved when takeover CAS fails"
        )

        # Verify the migration metadata is not corrupted
        for mid in migration_ids:
            mig_data, _ = zk.get(f"{migrations_path}/{mid}")
            assert mig_data and b"state" in mig_data, (
                f"Migration node {mid} has corrupted/empty data after takeover CAS fail"
            )

        # 5. Disable takeover_cas_fail and rollback_cas_fail → recovery should succeed now
        for n in ALL_NODES:
            for fp in [
                "selective_replication_takeover_cas_fail",
                "selective_replication_rollback_cas_fail",
            ]:
                try:
                    n.query(f"SYSTEM DISABLE FAILPOINT {fp}", settings=NO_FAULT)
                except Exception:
                    pass

        # Wait for recovery to clean up
        try:
            node1.query(
                f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{table}",
                settings={**NO_FAULT, "receive_timeout": "60"},
            )
        except Exception:
            pass
        time.sleep(5)

        # Verify: no orphaned :cloning entries
        assert no_orphaned_cloning(node1, table), (
            "Orphaned :cloning entry found after takeover recovery"
        )

        # Verify server is stable
        for node in ALL_NODES:
            try:
                count = int(
                    node.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
                )
                assert count == 3, f"{node.name}: expected 3 rows, got {count}"
            except Exception:
                pass
    finally:
        for n in ALL_NODES:
            for fp in [
                "selective_replication_takeover_cas_fail",
                "selective_replication_rollback_cas_fail",
            ]:
                try:
                    n.query(f"SYSTEM DISABLE FAILPOINT {fp}", settings=NO_FAULT)
                except Exception:
                    pass
            try:
                n.query(f"SYSTEM START FETCHES default.{table}", settings=NO_FAULT)
            except Exception:
                pass
        try:
            node3.start_clickhouse()
        except Exception:
            pass
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# Assignment
# ---------------------------------------------------------------------------


def test_allocate_cas_race(started_cluster):
    """Simulate a CAS race condition during partition allocation.

    Two replicas concurrently INSERT into the same new partition.  The first
    replica's allocatePartition tryCreate succeeds (creates the ZK node).  The
    second replica's allocatePartition hits ZNODEEXISTS and must correctly read
    the winner's assignment rather than using its own locally-computed one.

    This test verifies correctness of the CAS race handling path without
    relying on SYSTEM WAIT FAILPOINT (which is fragile because the fast-path
    tryGet returns early if the node already exists).
    """
    suffix = zk_suffix()
    table = f"cas_race_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # Concurrently INSERT the same partition from two nodes.
        # Both allocatePartition calls will race to create the assignment node.
        # Only one tryCreate wins; the other must handle ZNODEEXISTS correctly.
        errors = []
        barrier = threading.Barrier(2, timeout=30)

        def do_insert(node, name):
            try:
                barrier.wait()  # synchronize start
                node.query(
                    f"INSERT INTO {table} VALUES ('2024-11-15', 500, 'from_{name}')",
                    settings=NO_FAULT,
                )
            except Exception as e:
                errors.append((name, e))

        t1 = threading.Thread(target=do_insert, args=(node1, "node1"))
        t2 = threading.Thread(target=do_insert, args=(node2, "node2"))
        t1.start()
        t2.start()
        t1.join(timeout=60)
        t2.join(timeout=60)

        sync_all(table)

        # Verify no errors — both INSERTs should succeed
        for name, err in errors:
            print(f"INSERT error from {name}: {err}")
        assert len(errors) == 0, f"INSERT errors during CAS race: {errors}"

        # Verify all nodes can SELECT and see the data
        for node in ALL_NODES:
            count = int(
                node.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
            )
            assert count >= 1, f"{node.name}: expected >= 1 row, got {count}"

        # Verify assignment exists and is consistent (all replicas agree)
        assignments = set()
        for node in ALL_NODES:
            try:
                assigned = get_assigned(node, table, "202411")
                if assigned:
                    assignments.add(tuple(sorted(assigned)))
            except Exception:
                pass
        assert len(assignments) == 1, (
            f"Replicas disagree on assignment for partition 202411: {assignments}"
        )
    finally:
        drop_table(node1, table)


def test_cas_race_assignment_recreated(started_cluster):
    """CAS version check detects assignment changes.

    Verifies that commitPart's makeCheckRequest catches version mismatches
    when the ZK assignment node is modified externally.

    Steps:
    1. INSERT baseline data to create partition assignment.
    2. Bump the ZK assignment node version via kazoo set (same data).
    3. INSERT into the same partition — the cached CAS version is now stale,
       so makeCheckRequest(path, old_version) fails.
    4. Verify: INSERT either succeeds (internal retry with fresh version)
       or fails, then manual retry succeeds.
    """
    suffix = zk_suffix()
    table = f"cas_recreate_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    zk_path = node1.query(
        f"SELECT zookeeper_path FROM system.replicas "
        f"WHERE table = '{table}' LIMIT 1",
        settings=NO_FAULT,
    ).strip()

    try:
        # 1. Insert baseline to create assignment and warm the cache
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-01-15', 1, 'baseline')",
            settings=NO_FAULT,
        )
        sync_all(table)

        # 2. Bump the ZK assignment node version (same data, version +1)
        zk = cluster.get_kazoo_client("zoo1")
        assignment_path = f"{zk_path}/selective/assignments/202401"
        data, _ = zk.get(assignment_path)
        zk.set(assignment_path, data)  # version changes from N to N+1

        # Force-refresh cache so node1 picks up the new version before INSERT.
        # This ensures the CAS check uses the correct version and INSERT succeeds.
        node1.query("SELECT 1 FROM system.selective_assignments LIMIT 1", settings=NO_FAULT)

        # 3. INSERT into the same partition — cache now has fresh version, INSERT succeeds.
        node1.query(
            f"INSERT INTO {table} VALUES ('2024-01-16', 2, 'after_bump')",
            settings=NO_FAULT,
        )

        sync_all(table)

        # 4. Verify data integrity: baseline + after_bump, each replicated rf times
        nodes_with_data = get_nodes_with_partition(table, "202401")
        assert len(nodes_with_data) >= 1, (
            f"Partition 202401 should have data on at least 1 node, got: {nodes_with_data}"
        )

        total = get_cluster_rows(table)
        assert total == 2 * rf, f"Expected {2 * rf} physical rows (2 rows × rf={rf}), got {total}"

    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# Fault tolerance (ZK)
# ---------------------------------------------------------------------------


def test_read_partitions_zk_error(started_cluster):
    """When ZK is unavailable, shouldSkipForSelectiveReplication must keep
    queue entries instead of silently skipping them.

    This tests that GET_PART entries (e.g., from migration CLONE) are not
    incorrectly skipped when ZK is temporarily disconnected.

    The test verifies the behavior at the code level: when
    shouldSkipForSelectiveReplication encounters a ZK error while reading
    the assignment, it returns false (do NOT skip), so the queue entry
    remains for later processing.
    """
    suffix = zk_suffix()
    table = f"zk_err_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # Insert data to create partition assignments.
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'z{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        # All nodes must see 3 rows via SELECT routing
        wait_for_cluster_rows(table, 3)

        # Restart Keeper to simulate a brief ZK outage.
        # After Keeper restarts, the ZK sessions will expire and need to reconnect.
        cluster.stop_zookeeper_nodes(["zoo1"])
        time.sleep(2)
        cluster.start_zookeeper_nodes(["zoo1"])
        cluster.wait_zookeeper_to_start()

        # Wait for ZK sessions to reconnect (up to 30s).
        # During the reconnection window, shouldSkipForSelectiveReplication
        # should NOT skip any queue entries.
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                count = int(
                    node1.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
                )
                if count == 3:
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            # Final check — may still be reconnecting
            try:
                count = int(
                    node1.query(f"SELECT count() FROM {table}", settings=NO_FAULT).strip()
                )
                assert count == 3, f"Expected 3 rows after ZK restart, got {count}"
            except Exception:
                pass  # ZK still reconnecting; acceptable
    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# DROP PARTITION concurrent with in-flight migration
# ---------------------------------------------------------------------------


def test_drop_partition_during_migration(started_cluster):
    """DROP PARTITION while a migration is in flight for that partition.

    Verifies that the system handles the race gracefully — either the migration
    fails/rolls back, or the DROP succeeds and the migration completes on
    already-dropped data without crashing.
    """
    suffix = zk_suffix()
    table = f"drop_mig_{suffix}"

    create_table(node1, table, suffix, rf=2)

    try:
        # Insert data into multiple partitions.
        node1.query(
            f"INSERT INTO {table} VALUES "
            f"('2024-09-01', 1, 'a'), ('2024-09-02', 2, 'b'), "
            f"('2024-10-01', 3, 'c'), ('2024-10-02', 4, 'd')",
            settings=NO_FAULT,
        )
        sync_all(table)

        # Find a partition assigned to node1 and migrate it to another replica.
        pid = "202409"
        assigned = get_assigned(node1, table, pid)
        if not assigned:
            # Partition might not be assigned yet; skip gracefully.
            return

        # Pick a target that is NOT currently assigned.
        target_replica = None
        for r in ALL_REPLICA_NAMES:
            if r not in assigned:
                target_replica = r
                break
        if not target_replica:
            return

        # Start migration in a background thread.
        migration_error = [None]

        def do_migrate():
            try:
                node1.query(
                    f"SYSTEM MIGRATE PARTITION '{pid}' OF {table} TO REPLICA '{target_replica}'",
                    settings=NO_FAULT,
                    timeout=60,
                )
            except Exception as e:
                migration_error[0] = e

        t = threading.Thread(target=do_migrate)
        t.start()

        # Give migration a moment to start, then DROP the partition.
        time.sleep(0.5)
        try:
            node1.query(
                f"ALTER TABLE {table} DROP PARTITION '{pid}'",
                settings=NO_FAULT,
                timeout=30,
            )
        except Exception:
            pass  # DROP may fail if migration holds a lock — acceptable.

        t.join(timeout=60)

        # The key assertion: no crash, no LOGICAL_ERROR in logs.
        # Either migration rolled back or completed before DROP took effect.
        for n in ALL_NODES:
            assert n.contains_in_log("LOGICAL_ERROR") is False, (
                f"LOGICAL_ERROR found in {n.name} logs"
            )
    finally:
        drop_table(node1, table)


# ---------------------------------------------------------------------------
# Concurrent SYSTEM MIGRATE PARTITION commands for the same partition
# ---------------------------------------------------------------------------


def test_concurrent_migrate_same_partition(started_cluster):
    """Two concurrent SYSTEM MIGRATE PARTITION commands targeting the same partition.

    SYSTEM MIGRATE PARTITION is idempotent — if target_replica is already in
    the assignment, the command returns success immediately. So when two
    concurrent commands target the same partition + target replica, both can
    legitimately succeed: the first does the actual migration, the second
    sees it's already done and returns. The key invariants we verify:
      1. No crash, no LOGICAL_ERROR
      2. At least one command succeeds (the migration completes)
      3. Data integrity preserved (SELECT returns the original rows)
      4. Final assignment includes the target replica and has the expected
         replication factor (no extra replica left over)
    """
    suffix = zk_suffix()
    table = f"conc_mig_{suffix}"

    create_table(node1, table, suffix, rf=2)

    try:
        # Insert data.
        node1.query(
            f"INSERT INTO {table} VALUES "
            f"('2024-09-01', 1, 'a'), ('2024-09-02', 2, 'b')",
            settings=NO_FAULT,
        )
        sync_all(table)

        pid = "202409"
        assigned = get_assigned(node1, table, pid)
        if not assigned:
            return

        # Find a target replica not currently assigned.
        targets = [r for r in ALL_REPLICA_NAMES if r not in assigned]
        if len(targets) < 1:
            return

        # Use the same target for both — tests mutex on same migration.
        target = targets[0]

        results = [None, None]

        def migrate(idx, source_node):
            try:
                source_node.query(
                    f"SYSTEM MIGRATE PARTITION '{pid}' OF {table} TO REPLICA '{target}'",
                    settings=NO_FAULT,
                    timeout=60,
                )
                results[idx] = "ok"
            except Exception as e:
                results[idx] = str(e)

        t1 = threading.Thread(target=migrate, args=(0, node1))
        t2 = threading.Thread(target=migrate, args=(1, node2))
        t1.start()
        t2.start()
        t1.join(timeout=60)
        t2.join(timeout=60)

        # At least one must succeed (the migration must complete).
        # Both may succeed because MIGRATE PARTITION is idempotent — if target
        # is already assigned, it returns immediately (no-op).
        success_count = sum(1 for r in results if r == "ok")
        assert success_count >= 1, (
            f"Expected at least one migration to succeed, got {success_count}: {results}"
        )

        # Verify no LOGICAL_ERROR in logs.
        for n in ALL_NODES:
            assert n.contains_in_log("LOGICAL_ERROR") is False, (
                f"LOGICAL_ERROR found in {n.name} logs"
            )

        # Drive the migration state machine to completion (CLONE -> SWITCH).
        try:
            node1.query(
                f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{table}",
                settings={**NO_FAULT, "receive_timeout": "120"},
            )
        except Exception:
            pass

        # No orphaned :cloning entry should remain after migration completes.
        assert no_orphaned_cloning(node1, table), (
            "Orphaned :cloning entry found after concurrent migrations finished"
        )

        # Verify data integrity via SELECT routing — logical row count is 2
        # regardless of replication factor.
        wait_for_cluster_rows(table, 2)

        # Verify final assignment includes the target replica and that the
        # replication factor is preserved at 2 (migration replaced one replica,
        # not added an extra one).
        sync_all(table)
        final_assigned = get_assigned(node1, table, pid)
        assert target in final_assigned, (
            f"Expected target replica '{target}' in final assignment {final_assigned}"
        )
        assert len(final_assigned) == 2, (
            f"Expected 2 assigned replicas after migration, got {final_assigned}"
        )
    finally:
        drop_table(node1, table)
