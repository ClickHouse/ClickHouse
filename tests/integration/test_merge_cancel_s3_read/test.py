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

# The window oracle (b) must observe in full before it may pass.
SETTLED_WINDOW_SECONDS = 15


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
    # Every sample across the whole window must hold, so a pass cannot be granted by the first
    # one: `wait_for` would return on its initial evaluation, before any request had time to land.
    settled = get_attempts(node)
    for _ in range(SETTLED_WINDOW_SECONDS):
        time.sleep(1)
        attempts = get_attempts(node)
        assert attempts - settled <= 2, (
            f"the read kept retrying after the merge left system.merges "
            f"({attempts - settled} requests since it was cancelled)"
        )

    broken_s3.reset()
    optimize.get_answer_and_error()
    node.query("SYSTEM START MERGES " + table)

    # A cancelled merge keeps its source parts, so no data is lost.
    assert int(node.query(f"SELECT count() FROM {table}")) == 400000
    node.query(f"DROP TABLE {table} SYNC")


def test_stop_ttl_merges_does_not_cancel_regular_merge(cluster, broken_s3):
    """`SYSTEM STOP TTL MERGES` must not abort a merge that only happens to drop TTL values.

    The blocker is state-aware on the merge path: when it is already set as a regular merge
    starts, `prepare()` disables opportunistic TTL removal and the merge must then run to
    completion. The predicate therefore keys on the *settled* removal state, so an unguarded
    condition -- or one publishing the pre-downgrade value -- would abort this merge.

    The merge runs through the broken mock, so it is parked in the retry loop polling the
    predicate. Without that the arm would never reach the code it exists to constrain, and the
    unguarded form -- the single mutant it targets -- would leave it green.
    """
    node = cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS t_ttl SYNC")
    node.query(
        """
        CREATE TABLE t_ttl (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x
        TTL d + INTERVAL 1 SECOND
        SETTINGS storage_policy = 'broken_s3', min_bytes_for_wide_part = 0,
                 merge_with_ttl_timeout = 0
        """
    )
    node.query("SYSTEM STOP MERGES t_ttl")
    node.query("INSERT INTO t_ttl SELECT now() - 3600, number FROM numbers(20000)")
    node.query(
        "INSERT INTO t_ttl SELECT now() - 3600, number + 20000 FROM numbers(20000)"
    )

    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")

    node.query("SYSTEM STOP TTL MERGES t_ttl")
    broken_s3.setup_at_object_get(count=1000000, action="slow_down")

    before = get_attempts(node)
    node.query("SYSTEM START MERGES t_ttl")
    # Not `node.query`, for the same reason as the sibling arm: on a plain table OPTIMIZE runs
    # the merge in this thread, so awaiting it would block until the mock is reset.
    optimize = node.get_query_request(
        "OPTIMIZE TABLE t_ttl PARTITION tuple() FINAL "
        "SETTINGS alter_sync = 0, optimize_throw_if_noop = 0"
    )

    # The oracle is "the merge is still alive", so every wait below must fail the moment it dies:
    # otherwise an abort looks like a merge that is merely slow to get going, and the mutant this
    # arm targets gets reported as a vacuous arm instead of as the bug it is.
    # Keyed on our own OPTIMIZE still running: it drives the merge inline, so it survives exactly
    # as long as the merge does. `system.merges` cannot tell "aborted" from "not started yet",
    # and the server log is shared with the sibling arm, which cancels merges by design.
    # `NOT LIKE '%system.processes%'` so this query does not match its own text.
    optimize_running_query = (
        "SELECT count() FROM system.processes "
        "WHERE query LIKE '%OPTIMIZE TABLE t_ttl%' AND query NOT LIKE '%system.processes%'"
    )

    def assert_not_aborted():
        assert (
            int(node.query(optimize_running_query)) >= 1
        ), "SYSTEM STOP TTL MERGES aborted a regular merge"

    # Vacuity guard: the merge reaches the retry loop, so it does poll the predicate and the
    # window below is an observation rather than an idle wait.
    accrued = False
    for _ in range(300):
        assert_not_aborted()
        if get_attempts(node) - before >= ATTEMPTS_TO_ACCRUE:
            accrued = True
            break
        time.sleep(0.2)
    assert accrued, "the merge is not retrying the read, so it never polls the predicate"

    # The oracle: with TTL merges stopped, this merge must NOT be cancelled. It stays alive across
    # a window many times longer than the abort it guards against takes.
    settled = get_attempts(node)
    for _ in range(SETTLED_WINDOW_SECONDS):
        time.sleep(1)
        assert_not_aborted()
    assert (
        get_attempts(node) - settled >= 1
    ), "the merge stopped reading, so it is no longer in the retry loop"

    broken_s3.reset()
    optimize.get_answer_and_error()

    # A background merge may have claimed the parts first, in which case OPTIMIZE was a noop and
    # returned before that merge finished, so wait for the merge list to drain either way.
    assert wait_for(
        node,
        "SELECT count() FROM system.merges "
        "WHERE database = currentDatabase() AND table = 't_ttl'",
        lambda v: int(v) == 0,
    ), "the merge did not finish after the mock stopped failing"

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


def create_partly_expired_ttl_table(node, table, ttl, extra_settings=""):
    """Two wide parts on the broken disk, each half expired, with merges held off.

    Deliberately NOT fully expired: `TTLPartDropMergeSelector` runs at higher priority than the
    row-delete one and keys on the part's *max* TTL, so all-expired parts would be assigned
    `TTLDrop`, which skips creating the read pipeline altogether and so never reaches S3. The
    live half keeps `part_max_ttl` in the future. The expired half is also what makes the merge
    read enough to be cancellable.
    """
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x
        TTL {ttl}
        SETTINGS storage_policy = 'broken_s3', min_bytes_for_wide_part = 0,
                 merge_with_ttl_timeout = 0{extra_settings}
        """
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    for part in range(2):
        node.query(
            f"INSERT INTO {table} SELECT "
            f"if(number % 2, now() + 3600, now() - 3600), number + {part} * 40000 "
            f"FROM numbers(40000)"
        )

    # The merge must read the parts from the mock, not out of memory.
    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")


def drop_table_stopping_any_merge(node, table):
    """Teardown that must run even when an assertion above failed.

    `ATTEMPTS_QUERY` is a server-global counter, so a merge left retrying by a failed arm keeps
    issuing requests once the next arm's fixture resets the mock, and that arm's request-plateau
    oracle fails for a reason that has nothing to do with it. `SYSTEM STOP MERGES` cancels the
    merge outright, which is the path this PR's headline fix makes reliable.
    """
    node.query(f"SYSTEM STOP MERGES {table}")
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"SYSTEM START TTL MERGES {table}", ignore_error=True)


def assert_ttl_cancellation_reaches_the_read(node, table, before):
    """The two oracles the headline arm uses, after `SYSTEM STOP TTL MERGES`.

    Preconditions: the merge is in `system.merges` and inside the retry loop.
    """
    assert wait_for(
        node,
        ATTEMPTS_QUERY,
        lambda v: int(v or 0) - before >= ATTEMPTS_TO_ACCRUE,
    ), "the merge is not retrying the read, so there is nothing to cancel"

    node.query(f"SYSTEM STOP TTL MERGES {table}")

    # Oracle (a): the merge exits instead of retrying to its budget.
    assert wait_for(
        node,
        f"SELECT count() FROM system.merges "
        f"WHERE database = currentDatabase() AND table = '{table}'",
        lambda v: int(v) == 0,
    ), "the merge kept running after SYSTEM STOP TTL MERGES"

    # Only TTL merges are stopped, so the assignee is free to retry these same parts as a
    # regular merge, which would keep reading and defeat oracle (b) for reasons unrelated to
    # the branch under test. Stop merges outright now that oracle (a) has been observed.
    node.query(f"SYSTEM STOP MERGES {table}")

    # Oracle (b): and it stops issuing requests. Sampled across the whole window, so a pass
    # cannot be granted by the first sample, before any request had time to land.
    settled = get_attempts(node)
    for _ in range(SETTLED_WINDOW_SECONDS):
        time.sleep(1)
        attempts = get_attempts(node)
        assert attempts - settled <= 2, (
            f"the read kept retrying after the merge left system.merges "
            f"({attempts - settled} requests since it was cancelled)"
        )


def test_stop_ttl_merges_cancels_assigned_recompression_merge(cluster, broken_s3):
    """`SYSTEM STOP TTL MERGES` must interrupt an assigned TTL merge stuck in an S3 read.

    Recompression specifically, because it is the only assigned TTL merge that the sibling
    entry-flag condition does not already cover: a recompression TTL is recorded with
    `update_part_min_max_ttls = false` (`MergeTreeDataWriter.cpp:997`), so it never feeds
    `part_min_ttl` and `MergeTask::prepare` leaves `need_remove_expired_values` false. A
    `TTLDelete` or `TTLDrop` merge is instead selected *by* `part_min_ttl <= now`, which is the
    very condition that sets that flag, so for those the `isTTLMergeType` term is redundant and
    an arm built on them cannot observe it.

    The merge is left to the background assignee: an explicit `OPTIMIZE` is always
    `MergeType::Regular`.
    """
    node = cluster.instances["node"]
    table = "t_ttl_recompress"
    # `d - INTERVAL 1 HOUR` so the expired half is already due for recompression while the live
    # half keeps the part from being fully expired. `default_compression_codec` must differ from
    # the target, or `will_change_codec` is false and no recompression merge is ever selected.
    create_partly_expired_ttl_table(
        node,
        table,
        ttl="d - INTERVAL 1 HOUR RECOMPRESS CODEC(ZSTD(3))",
        extra_settings=", default_compression_codec = 'LZ4'",
    )

    broken_s3.setup_at_object_get(count=1000000, action="slow_down")

    try:
        before = get_attempts(node)
        node.query(f"SYSTEM START MERGES {table}")

        # Vacuity guard: the running merge really is an assigned TTL merge, so the branch under
        # test is the one being exercised. `merge_type` is `magic_enum::enum_name(MergeType)`
        # (`MergeList.cpp` -> `IO/WriteHelpers.h:1190`), so the enum spelling, not SCREAMING_CASE.
        assert wait_for(
            node,
            f"SELECT count() FROM system.merges WHERE database = currentDatabase() "
            f"AND table = '{table}' AND merge_type = 'TTLRecompress'",
            lambda v: int(v) >= 1,
        ), "no recompression TTL merge was assigned, so the assertions below would be vacuous"

        assert_ttl_cancellation_reaches_the_read(node, table, before)

        broken_s3.reset()
        # A cancelled merge keeps its source parts, so no data is lost.
        assert int(node.query(f"SELECT count() FROM {table}")) == 80000
    finally:
        drop_table_stopping_any_merge(node, table)


def test_stop_ttl_merges_cancels_regular_merge_removing_expired_values(
    cluster, broken_s3
):
    """`SYSTEM STOP TTL MERGES` must interrupt a regular merge that is removing expired values.

    `MergeTask` has two TTL cancellation conditions, and this is the second: a regular merge
    whose `need_remove_expired_values` survived the check in `prepare()` treats a later block
    as cancellation. The TTL blocker must therefore still be unset while `prepare()` runs --
    were it already set, removal would be disabled up front and the merge must instead run to
    completion, which is what `test_stop_ttl_merges_does_not_cancel_regular_merge` pins.
    """
    node = cluster.instances["node"]
    table = "t_ttl_regular"
    # `max_number_of_merges_with_ttl_in_pool = 0` makes `merge_with_ttl_allowed` false, so the
    # assignee cannot pick a TTL merge for these parts and steal them before `OPTIMIZE` -- which
    # bypasses the selector and is always `MergeType::Regular` -- gets them.
    create_partly_expired_ttl_table(
        node,
        table,
        ttl="d + INTERVAL 1 SECOND",
        extra_settings=", max_number_of_merges_with_ttl_in_pool = 0",
    )

    broken_s3.setup_at_object_get(count=1000000, action="slow_down")

    try:
        before = get_attempts(node)
        node.query(f"SYSTEM START MERGES {table}")
        # Not `node.query`: for a plain table OPTIMIZE runs the merge in this thread, so awaiting
        # it would deadlock against the very hang under test.
        optimize = node.get_query_request(
            f"OPTIMIZE TABLE {table} PARTITION tuple() FINAL "
            f"SETTINGS alter_sync = 0, optimize_throw_if_noop = 0"
        )

        # Vacuity guard: this arm is about the *regular* branch, so a background TTL merge
        # claiming the parts first would silently make it a duplicate of the sibling arm.
        assert wait_for(
            node,
            f"SELECT count() FROM system.merges WHERE database = currentDatabase() "
            f"AND table = '{table}' AND merge_type = 'Regular'",
            lambda v: int(v) >= 1,
        ), "no regular merge started, so the assertions below would be vacuous"

        assert_ttl_cancellation_reaches_the_read(node, table, before)

        broken_s3.reset()
        optimize.get_answer_and_error()
        assert int(node.query(f"SELECT count() FROM {table}")) == 80000
    finally:
        drop_table_stopping_any_merge(node, table)
