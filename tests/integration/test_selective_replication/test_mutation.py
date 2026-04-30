"""Integration tests for mutations (DELETE, UPDATE, REPLACE PARTITION) under selective replication."""

from .common import (
    started_cluster,
    node1,
    node2,
    node3,
    ALL_NODES,
    NO_FAULT,
    sync_all,
    create_table,
    drop_table,
    zk_suffix,
    wait_for_cluster_rows,
    get_assigned,
    get_nodes_with_partition,
    get_node_by_replica,
    no_orphaned_cloning,
)


def test_select_routing_after_delete(started_cluster):
    """SELECT from any node returns correct data after ALTER DELETE with selective replication."""
    suffix = zk_suffix()
    table = f"sel_delete_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # INSERT 3 rows across 3 partitions
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'v{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        # All nodes must see 3 rows
        wait_for_cluster_rows(table, 3)

        # DELETE one row
        node1.query(
            f"ALTER TABLE {table} DELETE WHERE k = 2 SETTINGS mutations_sync = 2",
            settings=NO_FAULT,
        )
        sync_all(table)

        # All nodes must see 2 rows after DELETE
        wait_for_cluster_rows(table, 2)

        # Verify the specific row is gone from every node
        for node in ALL_NODES:
            count_k2 = int(
                node.query(
                    f"SELECT count() FROM {table} WHERE k = 2", settings=NO_FAULT
                ).strip()
            )
            assert count_k2 == 0, (
                f"{node.name}: row with k=2 should be deleted, got {count_k2}"
            )
    finally:
        drop_table(node1, table)


def test_select_routing_after_update(started_cluster):
    """SELECT from any node returns correct data after ALTER UPDATE with selective replication."""
    suffix = zk_suffix()
    table = f"sel_update_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # INSERT 3 rows
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'v{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        wait_for_cluster_rows(table, 3)

        # UPDATE all values
        node1.query(
            f"ALTER TABLE {table} UPDATE v = 'updated' WHERE 1 SETTINGS mutations_sync = 2",
            settings=NO_FAULT,
        )
        sync_all(table)

        # All nodes must still see 3 rows
        wait_for_cluster_rows(table, 3)

        # Verify no old values remain
        for node in ALL_NODES:
            old_count = int(
                node.query(
                    f"SELECT count() FROM {table} WHERE v != 'updated'", settings=NO_FAULT
                ).strip()
            )
            assert old_count == 0, (
                f"{node.name}: expected 0 rows with old values, got {old_count}"
            )
    finally:
        drop_table(node1, table)


def test_select_routing_after_replace_partition(started_cluster):
    """SELECT from any node returns correct data after REPLACE PARTITION with selective replication."""
    suffix = zk_suffix()
    table = f"sel_replace_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)

    try:
        # INSERT 3 rows
        values = ", ".join(
            f"('2024-{m:02d}-15', {m}, 'v{m}')"
            for m in range(1, 4)
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        wait_for_cluster_rows(table, 3)

        # DROP partition 202401
        node1.query(
            f"ALTER TABLE {table} DROP PARTITION '202401'",
            settings=NO_FAULT,
        )
        sync_all(table)

        # Should see 2 rows now
        wait_for_cluster_rows(table, 2)

        # Create source table for REPLACE PARTITION (must have same partition key)
        src_table = f"src_{suffix}"
        node1.query(
            f"CREATE TABLE {src_table} (d Date, k UInt64, v String) "
            f"ENGINE = MergeTree() PARTITION BY toYYYYMM(d) ORDER BY k",
            settings=NO_FAULT,
        )

        # Insert replacement data into source
        node1.query(
            f"INSERT INTO {src_table} VALUES ('2024-01-20', 100, 'replaced')",
            settings=NO_FAULT,
        )

        # REPLACE PARTITION from source
        node1.query(
            f"ALTER TABLE {table} REPLACE PARTITION '202401' FROM {src_table}",
            settings=NO_FAULT,
        )
        sync_all(table)

        # All nodes must see 3 rows again (2 original + 1 replaced)
        wait_for_cluster_rows(table, 3)

        # Verify replaced data is visible from every node
        for node in ALL_NODES:
            replaced_count = int(
                node.query(
                    f"SELECT count() FROM {table} WHERE k = 100", settings=NO_FAULT
                ).strip()
            )
            assert replaced_count == 1, (
                f"{node.name}: expected 1 replaced row, got {replaced_count}"
            )

        # Cleanup source table
        try:
            node1.query(f"DROP TABLE {src_table} SYNC", settings=NO_FAULT)
        except Exception:
            pass
    finally:
        drop_table(node1, table)


def test_select_routing_after_move_partition_to_table(started_cluster):
    """MOVE PARTITION TO TABLE preserves logical rows and assignment under selective replication."""
    suffix = zk_suffix()
    table = f"sel_move_src_{suffix}"
    target_table = f"sel_move_dst_{suffix}"
    rf = 2

    create_table(node1, table, suffix, rf=rf)
    create_table(node1, target_table, suffix, rf=rf)

    try:
        values = ", ".join(
            [
                "('2024-01-15', 1, 'jan')",
                "('2024-02-15', 2, 'feb')",
                "('2024-03-15', 3, 'mar')",
            ]
        )
        node1.query(f"INSERT INTO {table} VALUES {values}", settings=NO_FAULT)
        sync_all(table)

        wait_for_cluster_rows(table, 3)
        wait_for_cluster_rows(target_table, 0)

        node1.query(
            f"ALTER TABLE {table} MOVE PARTITION '202401' TO TABLE {target_table}",
            settings=NO_FAULT,
        )

        for current_table in [table, target_table]:
            try:
                node1.query(
                    f"SYSTEM SYNC SELECTIVE MIGRATIONS default.{current_table}",
                    settings={**NO_FAULT, "receive_timeout": "60"},
                )
            except Exception:
                pass

        sync_all(table)
        sync_all(target_table)

        wait_for_cluster_rows(table, 2)
        wait_for_cluster_rows(target_table, 1)

        assigned = get_assigned(node1, target_table, "202401")
        assert len(assigned) == rf, (
            f"expected {rf} assigned replicas for moved partition, got {assigned}"
        )
        assert no_orphaned_cloning(node1, target_table), (
            "orphaned :cloning assignment remained after MOVE PARTITION TO TABLE"
        )

        expected_nodes = {get_node_by_replica(replica).name for replica in assigned}
        actual_nodes = get_nodes_with_partition(target_table, "202401")
        assert actual_nodes == expected_nodes, (
            f"expected moved partition on {expected_nodes}, got {actual_nodes}"
        )
    finally:
        try:
            drop_table(node1, target_table)
        finally:
            drop_table(node1, table)
