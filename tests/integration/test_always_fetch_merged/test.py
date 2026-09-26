import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


def test_replica_always_download(started_cluster):
    node1.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table/replicated', '1')
        ORDER BY tuple()
    """
    )
    node2.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table/replicated', '2')
        ORDER BY tuple()
        SETTINGS always_fetch_merged_part=1
    """
    )

    # Stop merges on single node
    node1.query("SYSTEM STOP MERGES")

    for i in range(0, 10):
        node1.query_with_retry("INSERT INTO test_table VALUES ({}, '{}')".format(i, i))

    assert node1.query("SELECT COUNT() FROM test_table") == "10\n"
    assert_eq_with_retry(node2, "SELECT COUNT() FROM test_table", "10\n")

    time.sleep(5)

    # Nothing is merged
    assert (
        node1.query(
            "SELECT COUNT() FROM system.parts WHERE table = 'test_table' and active=1"
        )
        == "10\n"
    )
    assert (
        node2.query(
            "SELECT COUNT() FROM system.parts WHERE table = 'test_table' and active=1"
        )
        == "10\n"
    )

    node1.query("SYSTEM START MERGES")
    node1.query("OPTIMIZE TABLE test_table")
    node2.query("SYSTEM SYNC REPLICA test_table")

    node1_parts = node1.query(
        "SELECT COUNT() FROM system.parts WHERE table = 'test_table' and active=1"
    ).strip()
    node2_parts = node2.query(
        "SELECT COUNT() FROM system.parts WHERE table = 'test_table' and active=1"
    ).strip()

    assert int(node1_parts) < 10
    assert int(node2_parts) < 10

    node1.query_with_retry("DROP TABLE test_table SYNC")
    node2.query_with_retry("DROP TABLE test_table SYNC")


def test_replica_always_download_mutated_part(started_cluster):
    node1.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_mutated_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mutated_table/replicated', '1')
        ORDER BY tuple()
    """
    )
    node2.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_mutated_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mutated_table/replicated', '2')
        ORDER BY tuple()
        SETTINGS always_fetch_mutated_part=1
    """
    )

    node1.query("INSERT INTO test_mutated_table VALUES (1, 'value')")
    node2.query("SYSTEM SYNC REPLICA test_mutated_table")

    mutations_before = int(
        node2.query(
            "SELECT sum(value) FROM system.events WHERE event = 'ReplicatedPartMutations'"
        )
    )

    node1.query(
        "ALTER TABLE test_mutated_table UPDATE value = 'mutated' WHERE 1",
        settings={"mutations_sync": 2},
    )

    assert node1.query("SELECT value FROM test_mutated_table") == "mutated\n"
    assert node2.query("SELECT value FROM test_mutated_table") == "mutated\n"
    assert (
        int(
            node2.query(
                "SELECT sum(value) FROM system.events WHERE event = 'ReplicatedPartMutations'"
            )
        )
        == mutations_before
    )
    assert node2.contains_in_log(
        "because setting 'always_fetch_mutated_part' is true"
    )

    node1.query_with_retry("DROP TABLE test_mutated_table SYNC")
    node2.query_with_retry("DROP TABLE test_mutated_table SYNC")


def test_no_mutation_failure_while_waiting_for_mutated_part(started_cluster):
    """A replica with `always_fetch_mutated_part` must not record `NO_REPLICA_HAS_PART`
    as a mutation failure while no replica has produced the mutated part yet, otherwise
    `ALTER ... SETTINGS mutations_sync = 1/2` issued on the fetch-only replica fails for
    long-running mutations."""
    node1.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_mutated_wait_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mutated_wait_table/replicated', '1')
        ORDER BY tuple()
    """
    )
    node2.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_mutated_wait_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mutated_wait_table/replicated', '2')
        ORDER BY tuple()
        SETTINGS always_fetch_mutated_part=1
    """
    )

    node1.query("INSERT INTO test_mutated_wait_table VALUES (1, 'value')")
    node2.query("SYSTEM SYNC REPLICA test_mutated_wait_table")

    # Hold the mutation on the only replica that can execute it, so that node2
    # keeps trying to fetch a part that no replica has yet.
    node1.query("SYSTEM STOP MERGES test_mutated_wait_table")
    try:
        node2.query("ALTER TABLE test_mutated_wait_table UPDATE value = 'mutated' WHERE 1")

        # Wait until node2 attempts to execute (i.e. fetch) the mutation entry.
        assert_eq_with_retry(
            node2,
            "SELECT count() > 0 FROM system.replication_queue WHERE table = 'test_mutated_wait_table' AND type = 'MUTATE_PART' AND num_tries >= 1",
            "1",
        )

        # While the mutated part exists nowhere, the fetch-only replica must treat it as
        # a normal wait, not as a mutation failure.
        for _ in range(20):
            fail_reason = node2.query(
                "SELECT latest_fail_reason FROM system.mutations WHERE table = 'test_mutated_wait_table' AND NOT is_done"
            )
            assert "NO_REPLICA_HAS_PART" not in fail_reason, fail_reason
            time.sleep(0.5)
    finally:
        node1.query("SYSTEM START MERGES test_mutated_wait_table")

    assert_eq_with_retry(
        node2,
        "SELECT count() FROM system.mutations WHERE table = 'test_mutated_wait_table' AND NOT is_done",
        "0",
        retry_count=120,
    )
    assert node1.query("SELECT value FROM test_mutated_wait_table") == "mutated\n"
    assert node2.query("SELECT value FROM test_mutated_wait_table") == "mutated\n"

    node1.query_with_retry("DROP TABLE test_mutated_wait_table SYNC")
    node2.query_with_retry("DROP TABLE test_mutated_wait_table SYNC")


def test_sync_mutation_rejected_on_fetch_only_replica(started_cluster):
    """A replica with `always_fetch_mutated_part` does not execute mutations and cannot
    observe mutation failures on the replicas executing them, so a synchronous wait there
    could hang if the mutation fails. Such waits must be rejected explicitly (fail closed)
    instead."""
    node1.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_mutated_sync_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mutated_sync_table/replicated', '1')
        ORDER BY tuple()
    """
    )
    node2.query_with_retry(
        """
        CREATE TABLE IF NOT EXISTS test_mutated_sync_table(
            key UInt64,
            value String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_mutated_sync_table/replicated', '2')
        ORDER BY tuple()
        SETTINGS always_fetch_mutated_part=1
    """
    )

    node1.query("INSERT INTO test_mutated_sync_table VALUES (1, 'value')")
    node2.query("SYSTEM SYNC REPLICA test_mutated_sync_table")

    # A synchronous mutation on the fetch-only replica is rejected up front,
    # before the mutation entry is created.
    for mutations_sync in (1, 2):
        error = node2.query_and_get_error(
            "ALTER TABLE test_mutated_sync_table UPDATE value = 'mutated' WHERE 1",
            settings={"mutations_sync": mutations_sync},
        )
        assert "SUPPORT_IS_DISABLED" in error, error
        assert "always_fetch_mutated_part" in error, error

    assert (
        node2.query(
            "SELECT count() FROM system.mutations WHERE table = 'test_mutated_sync_table'"
        )
        == "0\n"
    )

    # A synchronous ALTER that mutates data is rejected as well; the ALTER itself is
    # submitted and applies asynchronously.
    error = node2.query_and_get_error(
        "ALTER TABLE test_mutated_sync_table DROP COLUMN value",
        settings={"alter_sync": 1},
    )
    assert "SUPPORT_IS_DISABLED" in error, error
    assert "always_fetch_mutated_part" in error, error

    for node in (node1, node2):
        assert_eq_with_retry(
            node,
            "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'test_mutated_sync_table' AND name = 'value'",
            "0",
        )

    # The same mutation issued on the executing replica still works synchronously.
    node1.query(
        "ALTER TABLE test_mutated_sync_table UPDATE key = 42 WHERE 1",
        settings={"mutations_sync": 2},
    )
    assert node2.query("SELECT key FROM test_mutated_sync_table") == "42\n"

    node1.query_with_retry("DROP TABLE test_mutated_sync_table SYNC")
    node2.query_with_retry("DROP TABLE test_mutated_sync_table SYNC")
