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


def wait_for_postpone_reason(node, table, reason_substring, timeout=30):
    """Wait until a GET_PART entry appears with the given postpone_reason substring."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = int(
            node.query(
                f"""
            SELECT count()
            FROM system.replication_queue
            WHERE table = '{table}'
              AND type = 'GET_PART'
              AND postpone_reason LIKE '%{reason_substring}%'
            """
            ).strip()
        )
        if result >= 1:
            return True
        time.sleep(0.3)
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
    When replicated_fetches_min_part_level = 1 and timeout = 0, level-0
    (freshly inserted, unmerged) parts should NOT be fetched by the replica.
    The queue entry must carry the setting name in postpone_reason.
    """
    node1.query(
        """
        CREATE TABLE test_min_level (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level', '1')
        ORDER BY x
        """
    )
    node2.query(
        """
        CREATE TABLE test_min_level (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level', '2')
        ORDER BY x
        SETTINGS
            replicated_fetches_min_part_level = 1,
            replicated_fetches_min_part_level_timeout_sec = 0
        """
    )

    try:
        # Insert on node1 — creates a level-0 part
        node1.query("INSERT INTO test_min_level VALUES (1)")

        # With fetches running, node2's queue should postpone the GET_PART
        # because the part is level 0 and min_level = 1.
        assert wait_for_postpone_reason(
            node2, "test_min_level", "replicated_fetches_min_part_level"
        ), (
            "Expected GET_PART postponed with replicated_fetches_min_part_level reason\n"
            + node2.query(
                "SELECT type, new_part_name, postpone_reason "
                "FROM system.replication_queue WHERE table = 'test_min_level'"
            )
        )

        # node2 should have 0 rows (level-0 part blocked permanently by timeout=0)
        count = int(node2.query("SELECT count() FROM test_min_level").strip())
        assert count == 0, f"Expected 0 rows on node2, got {count}"

    finally:
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
        """
    )
    node2.query(
        """
        CREATE TABLE test_min_level_merge (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_min_level_merge', '2')
        ORDER BY x
        SETTINGS
            replicated_fetches_min_part_level = 1,
            replicated_fetches_min_part_level_timeout_sec = 0,
            prefer_fetch_merged_part_time_threshold = 0,
            prefer_fetch_merged_part_size_threshold = 0
        """
    )

    try:
        # Insert two rows on node1 — creates two level-0 parts
        node1.query("INSERT INTO test_min_level_merge VALUES (1)")
        node1.query("INSERT INTO test_min_level_merge VALUES (2)")

        # Wait for at least one GET_PART to be postponed on node2 by our setting
        assert wait_for_postpone_reason(
            node2, "test_min_level_merge", "replicated_fetches_min_part_level"
        ), "Level-0 parts were not postponed on node2"

        # Confirm node2 has 0 rows (level-0 parts are blocked)
        count_before = int(
            node2.query("SELECT count() FROM test_min_level_merge").strip()
        )
        assert (
            count_before == 0
        ), f"Expected 0 rows on node2 before merge, got {count_before}"

        # Force a merge on node1 — produces a level-1 part
        node1.query("OPTIMIZE TABLE test_min_level_merge FINAL")
        node1.query("SYSTEM SYNC REPLICA test_min_level_merge")

        # node2 should eventually receive the merged part (level >= 1 is allowed).
        # The MERGE_PARTS entry on node2 falls back to fetching the merged result
        # from node1 because node2 lacks the source level-0 parts.
        assert wait_for_count(node2, "test_min_level_merge", 2, timeout=60), (
            "node2 did not receive merged part in time\n"
            + "Parts on node2: "
            + node2.query(
                "SELECT name, level FROM system.parts "
                "WHERE table = 'test_min_level_merge' AND active"
            )
        )

        # Verify the fetched part has level >= 1
        level = int(
            node2.query(
                "SELECT max(level) FROM system.parts "
                "WHERE table = 'test_min_level_merge' AND active"
            ).strip()
        )
        assert level >= 1, f"Expected fetched part to have level >= 1, got {level}"

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
    When replicated_fetches_min_part_level = 1 and timeout = 5s,
    level-0 parts are initially postponed, then fetched after timeout expires.
    """
    node1.query(
        """
        CREATE TABLE test_timeout_fetch (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_timeout_fetch', '1')
        ORDER BY x
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
        node1.query("INSERT INTO test_timeout_fetch VALUES (1)")

        # Phase 1: Verify the part is initially postponed by our setting
        assert wait_for_postpone_reason(
            node2, "test_timeout_fetch", "replicated_fetches_min_part_level", timeout=10
        ), (
            "Expected GET_PART to be postponed with replicated_fetches_min_part_level reason\n"
            + node2.query(
                "SELECT type, new_part_name, postpone_reason "
                "FROM system.replication_queue WHERE table = 'test_timeout_fetch'"
            )
        )

        # Confirm node2 has 0 rows while postponed
        count_before = int(
            node2.query("SELECT count() FROM test_timeout_fetch").strip()
        )
        assert (
            count_before == 0
        ), f"Expected 0 rows on node2 while part is postponed, got {count_before}"

        # Phase 2: Wait for the 5s timeout to expire — part should be force-fetched
        assert wait_for_count(node2, "test_timeout_fetch", 1, timeout=30), (
            "node2 did not fetch level-0 part after timeout expired"
        )

    finally:
        node1.query("DROP TABLE IF EXISTS test_timeout_fetch SYNC")
        node2.query("DROP TABLE IF EXISTS test_timeout_fetch SYNC")


def test_permanent_skip_with_zero_timeout(started_cluster):
    """
    When replicated_fetches_min_part_level_timeout_sec = 0,
    level-0 parts are skipped permanently (no timeout override).
    """
    node1.query(
        """
        CREATE TABLE test_permanent_skip (x UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/test_permanent_skip', '1')
        ORDER BY x
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
        node1.query("INSERT INTO test_permanent_skip VALUES (1)")

        # Verify the GET_PART is postponed with our setting's reason
        assert wait_for_postpone_reason(
            node2, "test_permanent_skip", "replicated_fetches_min_part_level"
        ), (
            "Expected GET_PART to be postponed with replicated_fetches_min_part_level reason\n"
            + node2.query(
                "SELECT type, new_part_name, postpone_reason "
                "FROM system.replication_queue WHERE table = 'test_permanent_skip'"
            )
        )

        # With timeout=0, the part should remain blocked permanently.
        # Wait a generous amount of time and confirm it's still not fetched.
        time.sleep(5)
        count = int(node2.query("SELECT count() FROM test_permanent_skip").strip())
        assert count == 0, f"Expected 0 rows on node2 with zero timeout, got {count}"

        # Confirm the queue entry is STILL postponed by our setting
        queue_count = int(
            node2.query(
                """
            SELECT count()
            FROM system.replication_queue
            WHERE table = 'test_permanent_skip'
              AND type = 'GET_PART'
              AND postpone_reason LIKE '%replicated_fetches_min_part_level%'
            """
            ).strip()
        )
        assert (
            queue_count >= 1
        ), f"Expected queue entry still postponed after 5s, got count={queue_count}"

    finally:
        node1.query("DROP TABLE IF EXISTS test_permanent_skip SYNC")
        node2.query("DROP TABLE IF EXISTS test_permanent_skip SYNC")
