import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/fast_background_pool.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/fast_background_pool.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def count_ttl_merges_in_queue(node, table):
    result = node.query(
        f"SELECT count() FROM system.replication_queue WHERE merge_type = 'TTL_DELETE' and table = '{table}'"
    )
    if not result:
        return 0
    return int(result.strip())


def count_ttl_merges_in_background_pool(node, table, level):
    result = TSV(
        node.query(
            f"SELECT * FROM system.merges WHERE merge_type = 'TTL_DELETE' and table = '{table}'"
        )
    )
    count = len(result)
    if count >= level:
        logging.debug(
            f"count_ttl_merges_in_background_pool: merges more than warn level:\n{result}"
        )
    return count


def count_regular_merges_in_background_pool(node, table):
    result = node.query(
        f"SELECT count() FROM system.merges WHERE merge_type = 'REGULAR' and table = '{table}'"
    )
    if not result:
        return 0
    return int(result.strip())


def count_running_mutations(node, table):
    result = node.query(
        f"SELECT count() FROM system.merges WHERE table = '{table}' and is_mutation=1"
    )
    if not result:
        return 0
    return int(result.strip())


# This test was introduced to check concurrency for TTLs merges and mutations
# but it revealed a bug when we assign different merges to the same part
# on the borders of partitions.
def test_no_ttl_merges_in_busy_pool(started_cluster):
    node1.query(
        "CREATE TABLE test_ttl (d DateTime, key UInt64, data UInt64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY key TTL d + INTERVAL 1 MONTH SETTINGS merge_with_ttl_timeout = 0, number_of_free_entries_in_pool_to_execute_mutation = 0"
    )

    node1.query("SYSTEM STOP TTL MERGES")

    for i in range(1, 7):
        node1.query(
            f"INSERT INTO test_ttl SELECT now() - INTERVAL 1 MONTH + number - 1, {i}, number FROM numbers(5)"
        )

    node1.query("ALTER TABLE test_ttl UPDATE data = data + 1 WHERE sleepEachRow(1) = 0")

    while count_running_mutations(node1, "test_ttl") < 6:
        logging.debug(f"Mutations count {count_running_mutations(node1, 'test_ttl')}")
        assert count_ttl_merges_in_background_pool(node1, "test_ttl", 1) == 0
        time.sleep(0.5)

    node1.query("SYSTEM START TTL MERGES")

    rows_count = []
    while count_running_mutations(node1, "test_ttl") == 6:
        logging.debug(
            f"Mutations count after start TTL{count_running_mutations(node1, 'test_ttl')}"
        )
        rows_count.append(int(node1.query("SELECT count() FROM test_ttl").strip()))
        time.sleep(0.5)

    assert_eq_with_retry(node1, "SELECT COUNT() FROM test_ttl", "0")
    node1.query("DROP TABLE test_ttl SYNC")


def test_limited_ttl_merges_in_empty_pool(started_cluster):
    node1.query(
        "CREATE TABLE test_ttl_v2 (d DateTime, key UInt64, data UInt64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY key TTL d + INTERVAL 1 MONTH SETTINGS merge_with_ttl_timeout = 0"
    )

    node1.query("SYSTEM STOP TTL MERGES")

    for i in range(100):
        node1.query(
            f"INSERT INTO test_ttl_v2 SELECT now() - INTERVAL 1 MONTH, {i}, number FROM numbers(1)"
        )

    assert node1.query("SELECT COUNT() FROM test_ttl_v2") == "100\n"

    node1.query("SYSTEM START TTL MERGES")

    merges_with_ttl_count = set({})
    while True:
        merges_with_ttl_count.add(
            count_ttl_merges_in_background_pool(node1, "test_ttl_v2", 3)
        )
        time.sleep(0.01)
        if node1.query("SELECT COUNT() FROM test_ttl_v2") == "0\n":
            break

    assert max(merges_with_ttl_count) <= 2
    node1.query("DROP TABLE test_ttl_v2 SYNC")


def test_limited_ttl_merges_in_empty_pool_replicated(started_cluster):
    node1.query(
        "CREATE TABLE replicated_ttl (d DateTime, key UInt64, data UInt64) ENGINE = ReplicatedMergeTree('/test/t', '1') ORDER BY tuple() PARTITION BY key TTL d + INTERVAL 1 MONTH SETTINGS merge_with_ttl_timeout = 0"
    )

    node1.query("SYSTEM STOP TTL MERGES")

    for i in range(100):
        node1.query_with_retry(
            f"INSERT INTO replicated_ttl SELECT now() - INTERVAL 1 MONTH, {i}, number FROM numbers(1)"
        )

    assert node1.query("SELECT COUNT() FROM replicated_ttl") == "100\n"

    node1.query("SYSTEM START TTL MERGES")

    merges_with_ttl_count = set({})
    entries_with_ttl_count = set({})
    while True:
        merges_with_ttl_count.add(
            count_ttl_merges_in_background_pool(node1, "replicated_ttl", 3)
        )
        entries_with_ttl_count.add(count_ttl_merges_in_queue(node1, "replicated_ttl"))
        time.sleep(0.01)
        if node1.query("SELECT COUNT() FROM replicated_ttl") == "0\n":
            break

    assert max(merges_with_ttl_count) <= 2
    assert max(entries_with_ttl_count) <= 1

    node1.query("DROP TABLE replicated_ttl SYNC")


def test_limited_ttl_merges_two_replicas(started_cluster):
    # Actually this test quite fast and often we cannot catch any merges.
    node1.query(
        "CREATE TABLE replicated_ttl_2 (d DateTime, key UInt64, data UInt64) ENGINE = ReplicatedMergeTree('/test/t2', '1') ORDER BY tuple() PARTITION BY key TTL d + INTERVAL 1 MONTH SETTINGS merge_with_ttl_timeout = 0"
    )
    node2.query(
        "CREATE TABLE replicated_ttl_2 (d DateTime, key UInt64, data UInt64) ENGINE = ReplicatedMergeTree('/test/t2', '2') ORDER BY tuple() PARTITION BY key TTL d + INTERVAL 1 MONTH SETTINGS merge_with_ttl_timeout = 0"
    )

    node1.query("SYSTEM STOP TTL MERGES")
    node2.query("SYSTEM STOP TTL MERGES")

    for i in range(100):
        node1.query_with_retry(
            f"INSERT INTO replicated_ttl_2 SELECT now() - INTERVAL 1 MONTH, {i}, number FROM numbers(10000)"
        )

    node2.query("SYSTEM SYNC REPLICA replicated_ttl_2", timeout=10)
    assert node1.query("SELECT COUNT() FROM replicated_ttl_2") == "1000000\n"
    assert node2.query("SELECT COUNT() FROM replicated_ttl_2") == "1000000\n"

    node1.query("SYSTEM START TTL MERGES")
    node2.query("SYSTEM START TTL MERGES")

    merges_with_ttl_count_node1 = set({})
    merges_with_ttl_count_node2 = set({})
    while True:
        merges_with_ttl_count_node1.add(
            count_ttl_merges_in_background_pool(node1, "replicated_ttl_2", 3)
        )
        merges_with_ttl_count_node2.add(
            count_ttl_merges_in_background_pool(node2, "replicated_ttl_2", 3)
        )
        if (
            node1.query("SELECT COUNT() FROM replicated_ttl_2") == "0\n"
            and node2.query("SELECT COUNT() FROM replicated_ttl_2") == "0\n"
        ):
            break

    # Both replicas can assign merges with TTL. If one will perform better than
    # the other slow replica may have several merges in queue, so we don't
    # check them
    assert max(merges_with_ttl_count_node1) <= 2
    assert max(merges_with_ttl_count_node2) <= 2

    node1.query("DROP TABLE replicated_ttl_2 SYNC")
    node2.query("DROP TABLE replicated_ttl_2 SYNC")
