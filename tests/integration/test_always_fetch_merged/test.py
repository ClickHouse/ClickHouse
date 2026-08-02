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
