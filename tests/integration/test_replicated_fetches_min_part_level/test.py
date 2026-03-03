#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_part(node, table, part_name, timeout=30):
    """Wait until a specific part appears on a node."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = node.query(
            f"SELECT name FROM system.parts WHERE table = '{table}' AND active AND name = '{part_name}'"
        ).strip()
        if result == part_name:
            return True
        time.sleep(0.5)
    return False


def wait_for_count(node, table, expected_count, timeout=30):
    """Wait until a table has the expected row count."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = int(node.query(f"SELECT count() FROM {table}").strip())
        if result == expected_count:
            return True
        time.sleep(0.5)
    return False


def test_level0_parts_not_fetched(started_cluster):
    """
    When replicated_fetches_min_part_level = 1, level-0 (freshly inserted,
    unmerged) parts should NOT be fetched by the replica.
    """
    node1.query(
        """
        CREATE TABLE test_min_level (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level', '1')
        ORDER BY x
        SETTINGS replicated_fetches_min_part_level = 1
        """
    )
    node2.query(
        """
        CREATE TABLE test_min_level (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level', '2')
        ORDER BY x
        SETTINGS replicated_fetches_min_part_level = 1
        """
    )

    try:
        # Stop fetches on node2 so we can inspect the queue state
        node2.query("SYSTEM STOP FETCHES test_min_level")

        # Insert on node1 — creates a level-0 part (all_1_1_0)
        node1.query("INSERT INTO test_min_level VALUES (1)")

        # Give the replication log entry time to appear on node2
        time.sleep(2)

        # node2 should have the GET_PART entry in its queue but NOT execute it
        # because the part is level 0 and min_level = 1
        queue_entry = node2.query(
            """
            SELECT count()
            FROM system.replication_queue
            WHERE table = 'test_min_level'
              AND type = 'GET_PART'
              AND postpone_reason LIKE '%replicated_fetches_min_part_level%'
            """
        ).strip()

        # The entry should be postponed with our reason
        assert queue_entry == "1", (
            f"Expected 1 postponed GET_PART entry, got: {queue_entry}\n"
            + node2.query(
                "SELECT type, new_part_name, postpone_reason FROM system.replication_queue WHERE table = 'test_min_level'"
            )
        )

        # node2 should have 0 rows (part not fetched)
        count_before = int(node2.query("SELECT count() FROM test_min_level").strip())
        assert count_before == 0, f"Expected 0 rows on node2 before merge, got {count_before}"

    finally:
        node2.query("SYSTEM START FETCHES test_min_level")
        node1.query("DROP TABLE IF EXISTS test_min_level SYNC")
        node2.query("DROP TABLE IF EXISTS test_min_level SYNC")


def test_merged_parts_are_fetched(started_cluster):
    """
    When replicated_fetches_min_part_level = 1, parts that have been merged
    (level >= 1) SHOULD be fetched by the replica.
    """
    node1.query(
        """
        CREATE TABLE test_min_level_merge (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level_merge', '1')
        ORDER BY x
        SETTINGS replicated_fetches_min_part_level = 1
        """
    )
    node2.query(
        """
        CREATE TABLE test_min_level_merge (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level_merge', '2')
        ORDER BY x
        SETTINGS replicated_fetches_min_part_level = 1
        """
    )

    try:
        # Stop fetches on node2 so level-0 parts don't get fetched
        node2.query("SYSTEM STOP FETCHES test_min_level_merge")

        # Insert two rows on node1 — creates two level-0 parts
        node1.query("INSERT INTO test_min_level_merge VALUES (1)")
        node1.query("INSERT INTO test_min_level_merge VALUES (2)")

        # Force a merge on node1 — produces a level-1 part
        node1.query("OPTIMIZE TABLE test_min_level_merge FINAL")

        # Wait for the merge to complete on node1
        assert wait_for_count(node1, "test_min_level_merge", 2, timeout=30), (
            "node1 did not complete merge in time"
        )

        # Now start fetches on node2 — it should fetch the merged (level-1) part
        node2.query("SYSTEM START FETCHES test_min_level_merge")

        # node2 should eventually have 2 rows (the merged part was fetched)
        assert wait_for_count(node2, "test_min_level_merge", 2, timeout=30), (
            "node2 did not receive merged part in time\n"
            + "Parts on node2: "
            + node2.query(
                "SELECT name, level FROM system.parts WHERE table = 'test_min_level_merge' AND active"
            )
        )

        # Verify the fetched part has level >= 1
        level = node2.query(
            "SELECT max(level) FROM system.parts WHERE table = 'test_min_level_merge' AND active"
        ).strip()
        assert int(level) >= 1, f"Expected fetched part to have level >= 1, got {level}"

    finally:
        node1.query("DROP TABLE IF EXISTS test_min_level_merge SYNC")
        node2.query("DROP TABLE IF EXISTS test_min_level_merge SYNC")


def test_default_setting_fetches_all(started_cluster):
    """
    With the default setting (replicated_fetches_min_part_level = 0),
    all parts including level-0 should be fetched normally.
    """
    node1.query(
        """
        CREATE TABLE test_default_fetch (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_default_fetch', '1')
        ORDER BY x
        """
    )
    node2.query(
        """
        CREATE TABLE test_default_fetch (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_default_fetch', '2')
        ORDER BY x
        """
    )

    try:
        # Insert on node1 — creates a level-0 part
        node1.query("INSERT INTO test_default_fetch VALUES (42)")

        # node2 should replicate the level-0 part normally
        assert wait_for_count(node2, "test_default_fetch", 1, timeout=30), (
            "node2 did not receive level-0 part with default setting"
        )

    finally:
        node1.query("DROP TABLE IF EXISTS test_default_fetch SYNC")
        node2.query("DROP TABLE IF EXISTS test_default_fetch SYNC")


def test_level0_parts_fetched_after_timeout(started_cluster):
    """
    When replicated_fetches_min_part_level = 1 and timeout is set,
    level-0 parts are eventually fetched after timeout expires.
    """
    node1.query(
        """
        CREATE TABLE test_timeout_fetch (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_timeout_fetch', '1')
        ORDER BY x
        SETTINGS
            replicated_fetches_min_part_level = 1,
            replicated_fetches_min_part_level_timeout_sec = 5
        """
    )
    node2.query(
        """
        CREATE TABLE test_timeout_fetch (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_timeout_fetch', '2')
        ORDER BY x
        SETTINGS
            replicated_fetches_min_part_level = 1,
            replicated_fetches_min_part_level_timeout_sec = 5
        """
    )

    try:
        node2.query("SYSTEM STOP FETCHES test_timeout_fetch")
        node1.query("INSERT INTO test_timeout_fetch VALUES (1)")

        count_before = int(node2.query("SELECT count() FROM test_timeout_fetch").strip())
        assert count_before == 0, f"Expected 0 rows on node2 before timeout, got {count_before}"

        node2.query("SYSTEM START FETCHES test_timeout_fetch")

        assert wait_for_count(node2, "test_timeout_fetch", 1, timeout=30), (
            "node2 did not fetch level-0 part after timeout"
        )
    finally:
        node2.query("SYSTEM START FETCHES test_timeout_fetch")
        node1.query("DROP TABLE IF EXISTS test_timeout_fetch SYNC")
        node2.query("DROP TABLE IF EXISTS test_timeout_fetch SYNC")


def test_permanent_skip_with_zero_timeout(started_cluster):
    """
    When replicated_fetches_min_part_level_timeout_sec = 0,
    level-0 parts are skipped permanently.
    """
    node1.query(
        """
        CREATE TABLE test_permanent_skip (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_permanent_skip', '1')
        ORDER BY x
        SETTINGS
            replicated_fetches_min_part_level = 1,
            replicated_fetches_min_part_level_timeout_sec = 0
        """
    )
    node2.query(
        """
        CREATE TABLE test_permanent_skip (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_permanent_skip', '2')
        ORDER BY x
        SETTINGS
            replicated_fetches_min_part_level = 1,
            replicated_fetches_min_part_level_timeout_sec = 0
        """
    )

    try:
        node2.query("SYSTEM STOP FETCHES test_permanent_skip")
        node1.query("INSERT INTO test_permanent_skip VALUES (1)")

        time.sleep(3)
        count = int(node2.query("SELECT count() FROM test_permanent_skip").strip())
        assert count == 0, f"Expected 0 rows on node2 with zero timeout, got {count}"

        node2.query("SYSTEM START FETCHES test_permanent_skip")
    finally:
        node2.query("SYSTEM START FETCHES test_permanent_skip")
        node1.query("DROP TABLE IF EXISTS test_permanent_skip SYNC")
        node2.query("DROP TABLE IF EXISTS test_permanent_skip SYNC")
