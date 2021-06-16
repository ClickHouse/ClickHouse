import time
import pytest

import helpers.client as client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/fast_background_pool.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/fast_background_pool.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def count_ttl_merges_in_queue(node, table):
    result = node.query("SELECT count() FROM system.replication_queue WHERE merge_type = 'TTL_DELETE' and table = '{}'".format(table))
    if not result:
        return 0
    return int(result.strip())


def count_regular_merges_in_queue(node, table):
    result = node.query("SELECT count() FROM system.replication_queue WHERE merge_type = 'REGULAR' and table = '{}'".format(table))
    if not result:
        return 0
    return int(result.strip())


def count_ttl_merges_in_background_pool(node, table):
    result = node.query("SELECT count() FROM system.merges WHERE merge_type = 'TTL_DELETE' and table = '{}'".format(table))
    if not result:
        return 0
    return int(result.strip())


def count_regular_merges_in_background_pool(node, table):
    result = node.query("SELECT count() FROM system.merges WHERE merge_type = 'REGULAR' and table = '{}'".format(table))
    if not result:
        return 0
    return int(result.strip())


def count_running_mutations(node, table):
    result = node.query("SELECT count() FROM system.merges WHERE table = '{}' and is_mutation=1".format(table))
    if not result:
        return 0
    return int(result.strip())


# This test was introduced to check concurrency for TTLs merges and mutations
# but it revealed a bug when we assign different merges to the same part
# on the borders of partitions.
def test_no_ttl_merges_in_busy_pool(started_cluster):
    node1.query("CREATE TABLE test_ttl (d DateTime, key UInt64, data UInt64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY key TTL d + INTERVAL 1 MONTH SETTINGS merge_with_ttl_timeout = 0, number_of_free_entries_in_pool_to_execute_mutation = 0")

    node1.query("SYSTEM STOP TTL MERGES")

    for i in range(1, 7):
        node1.query("INSERT INTO test_ttl SELECT now() - INTERVAL 1 MONTH + number - 1, {}, number FROM numbers(5)".format(i))

    node1.query("ALTER TABLE test_ttl UPDATE data = data + 1 WHERE sleepEachRow(1) = 0")

    while count_running_mutations(node1, "test_ttl") < 6:
        print "Mutations count", count_running_mutations(node1, "test_ttl")
        assert count_ttl_merges_in_background_pool(node1, "test_ttl") == 0
        time.sleep(0.5)

    node1.query("SYSTEM START TTL MERGES")

    rows_count = []
    while count_running_mutations(node1, "test_ttl") == 6:
        print "Mutations count after start TTL", count_running_mutations(node1, "test_ttl")
        rows_count.append(int(node1.query("SELECT count() FROM test_ttl").strip()))
        time.sleep(0.5)

    # at least several seconds we didn't run any TTL merges and rows count equal
    # to the original value
    assert sum([1 for count in rows_count if count == 30]) > 4

    assert_eq_with_retry(node1, "SELECT COUNT() FROM test_ttl", "0")
