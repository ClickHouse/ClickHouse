"""Integration tests for OPTIMIZE/MERGE forwarding under selective replication."""

import time

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
    find_assigned,
    find_unassigned,
    get_metric,
    get_parts_count,
)


def test_optimize_partition_local_assigned(started_cluster):
    table = "test_opt_local"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        # insert some parts
        node1.query(f"INSERT INTO {table} VALUES ('2020-01-01', 1, 'a'), ('2020-01-01', 2, 'b')")
        node1.query(f"INSERT INTO {table} VALUES ('2020-01-01', 3, 'c'), ('2020-01-01', 4, 'd')")
        sync_all(table)

        partition_id = "202001"
        assigned_replica = find_assigned(node1, table, partition_id)
        assigned_node = REPLICA_TO_NODE[assigned_replica]

        assert get_parts_count(assigned_node, table, partition_id) == 2

        metric_before = get_metric(assigned_node, "SelectiveReplicationOptimizeForwardedPartitions")
        assigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202001' FINAL")
        sync_all(table)

        metric_after = get_metric(assigned_node, "SelectiveReplicationOptimizeForwardedPartitions")
        assert metric_after == metric_before
        assert get_parts_count(assigned_node, table, partition_id) == 1
    finally:
        drop_table(node1, table)


def test_optimize_partition_remote_assigned(started_cluster):
    table = "test_opt_remote"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        node1.query(f"INSERT INTO {table} VALUES ('2020-02-01', 1, 'a'), ('2020-02-01', 2, 'b')")
        node1.query(f"INSERT INTO {table} VALUES ('2020-02-01', 3, 'c'), ('2020-02-01', 4, 'd')")
        sync_all(table)

        partition_id = "202002"
        unassigned_replica = find_unassigned(node1, table, partition_id)
        unassigned_node = REPLICA_TO_NODE[unassigned_replica]

        assigned_replica = find_assigned(node1, table, partition_id)
        assigned_node = REPLICA_TO_NODE[assigned_replica]

        assert get_parts_count(assigned_node, table, partition_id) == 2

        unassigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202002' FINAL")
        sync_all(table)

        assert get_parts_count(assigned_node, table, partition_id) == 1
    finally:
        drop_table(node1, table)


def test_optimize_partition_rf_gt_1_single_log_entry(started_cluster):
    table = "test_opt_rf2"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=2)

    try:
        node1.query(f"INSERT INTO {table} VALUES ('2020-03-01', 1, 'a'), ('2020-03-01', 2, 'b')")
        node1.query(f"INSERT INTO {table} VALUES ('2020-03-01', 3, 'c'), ('2020-03-01', 4, 'd')")
        sync_all(table)

        partition_id = "202003"
        assigned = get_assigned(node1, table, partition_id)
        assert len(assigned) == 2

        unassigned_replica = (ALL_REPLICA_NAMES - set(assigned)).pop()
        unassigned_node = REPLICA_TO_NODE[unassigned_replica]

        # Verify initial state: each assigned replica has 2 parts
        for r in assigned:
            assert get_parts_count(REPLICA_TO_NODE[r], table, partition_id) == 2
        # Unassigned replica has no parts
        assert get_parts_count(unassigned_node, table, partition_id) == 0

        unassigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202003' FINAL")
        sync_all(table)

        # After merge, each assigned replica should have exactly 1 part
        for r in assigned:
            n = REPLICA_TO_NODE[r]
            assert get_parts_count(n, table, partition_id) == 1
        # Unassigned replica still has no parts
        assert get_parts_count(unassigned_node, table, partition_id) == 0
    finally:
        drop_table(node1, table)


def test_optimize_partition_unassigned_no_parts(started_cluster):
    table = "test_opt_no_parts"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)
    try:
        # partition X doesn't exist.
        # with optimize_throw_if_noop=1, should throw CANNOT_ASSIGN_OPTIMIZE
        error_msg = ""
        try:
            node1.query(f"OPTIMIZE TABLE {table} PARTITION ID '202004' FINAL", settings={"optimize_throw_if_noop": 1})
        except Exception as e:
            error_msg = str(e)
        assert "CANNOT_ASSIGN_OPTIMIZE" in error_msg
        assert "has no assignment and no local parts" in error_msg

        # with optimize_throw_if_noop=0, it should be silent
        node1.query(f"OPTIMIZE TABLE {table} PARTITION ID '202004' FINAL", settings={"optimize_throw_if_noop": 0})
    finally:
        drop_table(node1, table)


def test_optimize_table_final_all_partitions(started_cluster):
    table = "test_opt_all_parts"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        # We need to insert in a way that partitions end up on different replicas.
        # But assignments might be random.
        # Just insert many partitions
        for i in range(1, 6):
            node1.query(f"INSERT INTO {table} VALUES ('2020-0{i}-01', 1, 'a'), ('2020-0{i}-01', 2, 'b')")
            node1.query(f"INSERT INTO {table} VALUES ('2020-0{i}-01', 3, 'c'), ('2020-0{i}-01', 4, 'd')")
        sync_all(table)

        # Check initial parts
        for i in range(1, 6):
            part_id = f"20200{i}"
            assigned = get_assigned(node1, table, part_id)
            assert len(assigned) == 1
            n = REPLICA_TO_NODE[assigned[0]]
            assert get_parts_count(n, table, part_id) == 2

        # The client will be node1. Some partitions might be on node1, some on other nodes.
        # Let's count how many partitions are NOT assigned to node1.
        remote_partitions = 0
        for i in range(1, 6):
            part_id = f"20200{i}"
            assigned = get_assigned(node1, table, part_id)
            if assigned[0] != "r1":
                remote_partitions += 1

        node1.query(f"OPTIMIZE TABLE {table} FINAL")
        sync_all(table)

        for i in range(1, 6):
            part_id = f"20200{i}"
            assigned = get_assigned(node1, table, part_id)
            n = REPLICA_TO_NODE[assigned[0]]
            assert get_parts_count(n, table, part_id) == 1
    finally:
        drop_table(node1, table)


def test_optimize_partition_unassigned_with_local_parts(started_cluster):
    table = "test_opt_local_no_assign"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        node1.query(f"INSERT INTO {table} VALUES ('2020-06-01', 1, 'a'), ('2020-06-01', 2, 'b')")
        node1.query(f"INSERT INTO {table} VALUES ('2020-06-01', 3, 'c'), ('2020-06-01', 4, 'd')")
        sync_all(table)

        partition_id = "202006"
        assigned_replica = find_assigned(node1, table, partition_id)
        assigned_node = REPLICA_TO_NODE[assigned_replica]

        assert get_parts_count(assigned_node, table, partition_id) == 2

        # Delete the assignment from ZK
        zk = cluster.get_kazoo_client('zoo1')
        zk_path = f"/clickhouse/tables/{zs}/{table}/selective/assignments/{partition_id}"
        if zk.exists(zk_path):
            zk.delete(zk_path)

        for _ in range(20):
            if len(get_assigned(node1, table, partition_id)) == 0:
                break
            time.sleep(0.5)

        assigned_after = get_assigned(node1, table, partition_id)
        assert len(assigned_after) == 0

        metric_before = get_metric(assigned_node, "SelectiveReplicationOptimizeForwardedPartitions")
        assigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202006' FINAL")
        sync_all(table)

        metric_after = get_metric(assigned_node, "SelectiveReplicationOptimizeForwardedPartitions")
        assert metric_after == metric_before
        assert get_parts_count(assigned_node, table, partition_id) == 1
    finally:
        drop_table(node1, table)


def test_optimize_forwarding_all_unavailable(started_cluster):
    table = "test_opt_unavail"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        node1.query(f"INSERT INTO {table} VALUES ('2020-07-01', 1, 'a'), ('2020-07-01', 2, 'b')")
        sync_all(table)

        partition_id = "202007"
        assigned_replica = find_assigned(node1, table, partition_id)
        assigned_node = REPLICA_TO_NODE[assigned_replica]

        unassigned_replica = find_unassigned(node1, table, partition_id)
        unassigned_node = REPLICA_TO_NODE[unassigned_replica]

        # Stop the assigned node
        with cluster.pause_container(assigned_node.name):
            error_msg = ""
            try:
                # Query from unassigned node, optimize should be forwarded to assigned node but it's down.
                unassigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202007' FINAL", settings={"skip_unavailable_shards": 0, "optimize_throw_if_noop": 1})
            except Exception as e:
                error_msg = str(e)

            assert "ALL_REPLICAS_LOST" in error_msg or "Cannot execute query" in error_msg or "Connection refused" in error_msg or "Cannot select parts for optimization" in error_msg

            # With skip_unavailable_shards=1
            try:
                unassigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202007' FINAL", settings={"skip_unavailable_shards": 1})
                # Should be silent
            except Exception as e:
                assert False, f"Should not throw when skip_unavailable_shards=1, got {e}"

    finally:
        drop_table(node1, table)


def test_optimize_forwarding_connection_fallback(started_cluster):
    table = "test_opt_fallback"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=2)

    try:
        node1.query(f"INSERT INTO {table} VALUES ('2020-08-01', 1, 'a'), ('2020-08-01', 2, 'b')")
        node1.query(f"INSERT INTO {table} VALUES ('2020-08-01', 3, 'c'), ('2020-08-01', 4, 'd')")
        sync_all(table)

        partition_id = "202008"
        assigned = get_assigned(node1, table, partition_id)
        assert len(assigned) == 2

        unassigned_replica = (ALL_REPLICA_NAMES - set(assigned)).pop()
        unassigned_node = REPLICA_TO_NODE[unassigned_replica]

        node_to_pause = REPLICA_TO_NODE[assigned[0]]
        with cluster.pause_container(node_to_pause.name):
            unassigned_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202008' FINAL")

        sync_all(table)
        active_assigned = REPLICA_TO_NODE[assigned[1]]
        assert get_parts_count(active_assigned, table, partition_id) == 1
    finally:
        drop_table(node1, table)


def test_optimize_forwarding_assignment_race(started_cluster):
    table = "test_opt_race"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        node1.query(f"INSERT INTO {table} VALUES ('2020-09-01', 1, 'a'), ('2020-09-01', 2, 'b')")
        node1.query(f"INSERT INTO {table} VALUES ('2020-09-01', 3, 'c'), ('2020-09-01', 4, 'd')")
        sync_all(table)

        partition_id = "202009"
        old_assigned = find_assigned(node1, table, partition_id)

        # We need a new assigned replica that is not the old one.
        candidates = list(ALL_REPLICA_NAMES - {old_assigned})
        new_assigned = candidates[0]
        unassigned_client = candidates[1]

        client_node = REPLICA_TO_NODE[unassigned_client]
        new_assigned_node = REPLICA_TO_NODE[new_assigned]

        # Change assignment in ZK directly
        zk = cluster.get_kazoo_client('zoo1')
        zk_path = f"/clickhouse/tables/{zs}/{table}/selective/assignments/{partition_id}"

        # Write new assignment (format: "format version: 1\nreplica1,replica2")
        zk.set(zk_path, f"format version: 1\n{new_assigned}".encode('utf-8'))

        # Immediately query from unassigned_client before its cache updates.
        # Wait, if we use a client that wasn't touched, it might still have the old cache or no cache.
        # It's better to prime the cache on client_node first.
        client_node.query(f"SELECT count() FROM system.selective_assignments WHERE table = '{table}'")

        metric_before = get_metric(client_node, "SelectiveReplicationOptimizeForwardedPartitions")

        # Race condition: cache has old_assigned, but actual is new_assigned.
        # client_node forwards to old_assigned. old_assigned forwards to new_assigned.
        # The query should complete without exception regardless of cache state.
        client_node.query(f"OPTIMIZE TABLE {table} PARTITION ID '202009' FINAL")

        # Just verify the query did not crash or hang.
        sync_all(table)
    finally:
        drop_table(node1, table)


def test_insert_forwarding_depth_limit_regression(started_cluster):
    """6-layer Distributed table chain INSERT triggers TOO_LARGE_DISTRIBUTED_DEPTH."""
    table = "test_insert_depth_reg"
    zs = zk_suffix()
    create_table(node1, table, zs, rf=1)

    try:
        # We need a non-assigned partition.
        # First let's insert a row to let assignment happen.
        node1.query(f"INSERT INTO {table} VALUES ('2020-10-01', 1, 'a')")
        sync_all(table)

        partition_id = "202010"

        unassigned_replica = find_unassigned(node1, table, partition_id)
        unassigned_node = REPLICA_TO_NODE[unassigned_replica]

        # Create distributed tables chain on the unassigned node
        unassigned_node.query(f"CREATE TABLE dist1 AS {table} ENGINE = Distributed('test_sr_cluster', 'default', '{table}')")
        unassigned_node.query(f"CREATE TABLE dist2 AS {table} ENGINE = Distributed('test_sr_cluster', 'default', 'dist1')")
        unassigned_node.query(f"CREATE TABLE dist3 AS {table} ENGINE = Distributed('test_sr_cluster', 'default', 'dist2')")
        unassigned_node.query(f"CREATE TABLE dist4 AS {table} ENGINE = Distributed('test_sr_cluster', 'default', 'dist3')")
        unassigned_node.query(f"CREATE TABLE dist5 AS {table} ENGINE = Distributed('test_sr_cluster', 'default', 'dist4')")
        unassigned_node.query(f"CREATE TABLE dist6 AS {table} ENGINE = Distributed('test_sr_cluster', 'default', 'dist5')")

        # Now INSERT to dist6. This will forward to dist5 -> dist4 -> dist3 -> dist2 -> dist1 -> test_insert_depth_reg (local)
        # But wait, local node is unassigned! So the ReplicatedMergeTree will try to forward the INSERT to the assigned node.
        # Since distributed_depth is already 6, it should throw TOO_LARGE_DISTRIBUTED_DEPTH.

        error_msg = ""
        try:
            unassigned_node.query(f"INSERT INTO dist6 VALUES ('2020-10-01', 2, 'b')")
        except Exception as e:
            error_msg = str(e)

        assert "TOO_LARGE_DISTRIBUTED_DEPTH" in error_msg or "exceeded max" in error_msg.lower() or "too large" in error_msg.lower()

    finally:
        drop_table(node1, table)
        for i in range(1, 7):
            try:
                unassigned_node.query(f"DROP TABLE IF EXISTS dist{i}")
            except:
                pass
