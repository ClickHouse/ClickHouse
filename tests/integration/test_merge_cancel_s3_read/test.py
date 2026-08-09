#!/usr/bin/env python3

import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

# Enough GETs to prove the merge is inside the retry loop rather than merely slow.
ATTEMPTS_TO_ACCRUE = 20


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def init_broken_s3(cluster):
    yield start_s3_mock(cluster, "broken_s3", "8087")


@pytest.fixture(scope="function")
def broken_s3(init_broken_s3):
    init_broken_s3.reset()
    yield init_broken_s3


# `DiskS3ReadRequestsCount` counts every request attempt, so it moves whatever error class the
# mock injects. The alternatives are both wrong here: `DiskS3GetObject` counts only the read
# buffer's outer attempts, not the SDK retries doing the spinning (measured 15 vs 2064 over one
# such read), and `...Errors` stays flat for a 429, which is accounted as throttling instead.
# `sumIf` because an event that has never fired has no row in `system.events` at all.
ATTEMPTS_QUERY = (
    "SELECT sumIf(value, event = 'DiskS3ReadRequestsCount') FROM system.events"
)


def get_attempts(node):
    return int(node.query(ATTEMPTS_QUERY).strip() or 0)


def wait_for(node, query, predicate, attempts=300, sleep_time=0.2):
    for _ in range(attempts):
        value = node.query(query).strip()
        if predicate(value):
            return True
        time.sleep(sleep_time)
    return False


@pytest.mark.parametrize("replicated", [False, True])
def test_stop_merges_cancels_s3_read(cluster, broken_s3, replicated):
    """`SYSTEM STOP MERGES` must interrupt a merge stuck retrying an S3 read.

    A merge/mutate thread group's cancellation predicates resolve through a process-list
    element it does not have, so before the fix they were constant `false` and the S3 retry
    loop never observed the cancellation: the merge retried to its budget while holding the
    table, which is what the stress-test hung check reported.

    Both engines are covered because the two root callers pass different contexts: the plain
    one passes its per-task context, the replicated one passes the shared storage context.
    A guard keyed on the argument rather than on the merge list entry's own thread group
    would cover only the plain case.
    """
    node = cluster.instances["node"]
    table = "t_repl" if replicated else "t_plain"
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/{table}', 'r1')"
        if replicated
        else "MergeTree"
    )

    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (id UInt64, s String) ENGINE = {engine} ORDER BY id
        SETTINGS storage_policy = 'broken_s3', min_bytes_for_wide_part = 0
        """
    )
    node.query("SYSTEM STOP MERGES " + table)
    node.query(
        f"INSERT INTO {table} SELECT number, repeat('x', 200) FROM numbers(200000)"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + 200000, repeat('y', 200) FROM numbers(200000)"
    )
    assert (
        int(
            node.query(
                f"SELECT count() FROM system.parts "
                f"WHERE database = currentDatabase() AND table = '{table}' AND active"
            )
        )
        == 2
    )

    # The merge must read the parts from the mock, not out of memory.
    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")

    # Immediate retryable failures, deliberately NOT `slow_get`: a sleep inside the request
    # blocks a fixed and an unfixed server equally, so the arm would prove nothing. These
    # spin the retry loop instead, so the cancellation predicate is polled promptly.
    broken_s3.setup_at_object_get(count=1000000, action="slow_down")

    before = get_attempts(node)
    node.query("SYSTEM START MERGES " + table)
    # Not `node.query`: for a plain table OPTIMIZE runs the merge in this thread, so awaiting it
    # would deadlock against the very hang under test.
    optimize = node.get_query_request(
        f"OPTIMIZE TABLE {table} FINAL SETTINGS alter_sync = 0, optimize_throw_if_noop = 0"
    )

    assert wait_for(
        node,
        f"SELECT count() FROM system.merges "
        f"WHERE database = currentDatabase() AND table = '{table}'",
        lambda v: int(v) >= 1,
    ), "the merge never started, so the assertions below would be vacuous"

    assert wait_for(
        node,
        ATTEMPTS_QUERY,
        lambda v: int(v or 0) - before >= ATTEMPTS_TO_ACCRUE,
    ), "the merge is not retrying the read, so there is nothing to cancel"

    node.query("SYSTEM STOP MERGES " + table)

    # Oracle (a): the merge exits instead of retrying to its budget.
    assert wait_for(
        node,
        f"SELECT count() FROM system.merges "
        f"WHERE database = currentDatabase() AND table = '{table}'",
        lambda v: int(v) == 0,
    ), "the merge kept running after SYSTEM STOP MERGES"

    # Oracle (b): and it stops issuing requests. (a) alone could pass on a slow but finite
    # read; (b) alone could pass on a merge that never started.
    settled = get_attempts(node)
    assert wait_for(
        node,
        ATTEMPTS_QUERY,
        lambda v: int(v or 0) - settled <= 2,
        attempts=15,
        sleep_time=1,
    ), "the read kept retrying after the merge left system.merges"

    broken_s3.reset()
    optimize.get_answer_and_error()
    node.query("SYSTEM START MERGES " + table)

    # A cancelled merge keeps its source parts, so no data is lost.
    assert int(node.query(f"SELECT count() FROM {table}")) == 400000
    node.query(f"DROP TABLE {table} SYNC")


def test_stop_ttl_merges_does_not_cancel_regular_merge(cluster, broken_s3):
    """`SYSTEM STOP TTL MERGES` must not abort a merge that only happens to drop TTL values.

    The installed predicate deliberately excludes `ttl_merges_blocker`, which is state-aware
    on the merge path: it aborts an assigned TTL merge but only disables opportunistic TTL
    removal in a regular one.
    """
    node = cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS t_ttl SYNC")
    node.query(
        """
        CREATE TABLE t_ttl (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x
        TTL d + INTERVAL 1 SECOND
        SETTINGS merge_with_ttl_timeout = 0
        """
    )
    node.query("SYSTEM STOP MERGES t_ttl")
    node.query("INSERT INTO t_ttl SELECT now() - 3600, number FROM numbers(20000)")
    node.query(
        "INSERT INTO t_ttl SELECT now() - 3600, number + 20000 FROM numbers(20000)"
    )

    node.query("SYSTEM STOP TTL MERGES t_ttl")
    node.query("SYSTEM START MERGES t_ttl")
    node.query("OPTIMIZE TABLE t_ttl PARTITION tuple() FINAL SETTINGS optimize_throw_if_noop = 1")

    assert (
        int(
            node.query(
                "SELECT count() FROM system.parts "
                "WHERE database = currentDatabase() AND table = 't_ttl' AND active"
            )
        )
        == 1
    ), "the merge was aborted instead of completing without TTL removal"
    # TTL removal was skipped, which is what stopping TTL merges is supposed to do.
    assert int(node.query("SELECT count() FROM t_ttl")) == 40000

    node.query("SYSTEM START TTL MERGES t_ttl")
    node.query("DROP TABLE t_ttl SYNC")
