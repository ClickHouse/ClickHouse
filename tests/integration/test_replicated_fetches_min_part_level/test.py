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


def wait_for_postpone_count_increase(node, table, reason_substring, min_increase=3, timeout=30):
    """Wait until num_postponed has grown by at least min_increase, proving active re-postponement."""
    initial = int(
        node.query(
            f"""
        SELECT coalesce(max(num_postponed), 0)
        FROM system.replication_queue
        WHERE table = '{table}'
          AND type = 'GET_PART'
          AND postpone_reason LIKE '%{reason_substring}%'
        """
        ).strip()
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = int(
            node.query(
                f"""
            SELECT coalesce(max(num_postponed), 0)
            FROM system.replication_queue
            WHERE table = '{table}'
              AND type = 'GET_PART'
              AND postpone_reason LIKE '%{reason_substring}%'
            """
            ).strip()
        )
        if current >= initial + min_increase:
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
            replicated_fetches_min_part_level_timeout_seconds = 0
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
        # from node1 because node2 lacks the source level-0 parts. This fallback
        # fetch goes through executeFetch directly, bypassing the
        # replicated_fetches_min_part_level check (which only gates GET_PART
        # entries in shouldExecuteLogEntry).
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
            replicated_fetches_min_part_level_timeout_seconds = 5
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

        # Phase 2: Wait for the 5s timeout to expire — part should be force-fetched
        assert wait_for_count(node2, "test_timeout_fetch", 1, timeout=30), (
            "node2 did not fetch level-0 part after timeout expired"
        )

    finally:
        node1.query("DROP TABLE IF EXISTS test_timeout_fetch SYNC")
        node2.query("DROP TABLE IF EXISTS test_timeout_fetch SYNC")


def test_permanent_skip_with_zero_timeout(started_cluster):
    """
    When replicated_fetches_min_part_level_timeout_seconds = 0,
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
            replicated_fetches_min_part_level_timeout_seconds = 0
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
        # Verify the queue processor keeps re-postponing the entry (num_postponed grows)
        # rather than sleeping an arbitrary duration.
        assert wait_for_postpone_count_increase(
            node2, "test_permanent_skip", "replicated_fetches_min_part_level", min_increase=3
        ), (
            "Expected num_postponed to keep increasing for permanently blocked entry\n"
            + node2.query(
                "SELECT type, new_part_name, num_postponed, postpone_reason "
                "FROM system.replication_queue WHERE table = 'test_permanent_skip'"
            )
        )

        # After multiple re-postponements, node2 must still have 0 rows
        count = int(node2.query("SELECT count() FROM test_permanent_skip").strip())
        assert count == 0, f"Expected 0 rows on node2 with zero timeout, got {count}"

    finally:
        node1.query("DROP TABLE IF EXISTS test_permanent_skip SYNC")
        node2.query("DROP TABLE IF EXISTS test_permanent_skip SYNC")
