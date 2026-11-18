#!/usr/bin/env python3

import logging
import os
import time

import boto3
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")

# Enough GETs to prove the merge is inside the retry loop rather than merely slow.
ATTEMPTS_TO_ACCRUE = 20

# The window oracle (b) must observe in full before it may pass.
SETTLED_WINDOW_SECONDS = 15

# A cancellation reaching the retry loop is observed after at most the remaining backoff
# (`DEFAULT_RETRY_MAX_DELAY_MS`, `IO/S3Defines.h`), so within seconds, while the retry budget it
# replaces is tens of minutes. Past this budget `stop_clickhouse` escalates to SIGKILL and reports
# the stop as not graceful, which is what the shutdown arm keys on.
SHUTDOWN_BUDGET_SECONDS = 60

# A failed merge whose other reads stop leaves `system.merges` within one retry backoff of its failure.
FAILED_MERGE_MAX_AGE_SECONDS = 20

SETUP_FAILPOINT = "merge_task_throw_while_preparing_next_column"

SOURCE_FAILPOINT = "merge_tree_sequential_source_throw_in_constructor"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            # The oracles count S3 events, which are server-global: a metadata disk would add
            # its own requests to them.
            with_remote_database_disk=False,
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
    # Keyed on our own OPTIMIZE still running: it drives the merge inline, and being `FINAL` it waits
    # for a competing merge on these parts rather than returning, so it survives as long as the merge
    # does. `system.merges` cannot tell "aborted" from "not started yet", and the server log is
    # shared with the sibling arm, which cancels merges by design.
    # `NOT LIKE '%system.processes%'` so this query does not match its own text.
    optimize_running_query = (
        "SELECT count() FROM system.processes "
        "WHERE query LIKE '%OPTIMIZE TABLE t_ttl%' AND query NOT LIKE '%system.processes%'"
    )

    def assert_not_aborted():
        assert (
            int(node.query(optimize_running_query)) >= 1
        ), "SYSTEM STOP TTL MERGES aborted a regular merge"

    # `get_query_request` only spawns a client and returns, so until the query reaches the server
    # "not running" does not distinguish an abort from a connection still being made.
    assert wait_for(
        node, optimize_running_query, lambda v: int(v) >= 1
    ), "the OPTIMIZE never reached the server, so the assertions below would be vacuous"

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

    # A `FINAL` optimize waits for a competing merge on the same parts rather than returning, so
    # its answer above does not by itself mean the merge list has drained.
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


# Cold, the merge stalls in `MergeTask::prepare`, loading statistics and marks. Warm, with neither
# left to load, it stalls on the first data read, after `prepare` replaced the group's predicates.
@pytest.mark.parametrize("warm", [False, True])
def test_stop_ttl_merges_cancels_assigned_recompression_merge(cluster, broken_s3, warm):
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
        extra_settings=", default_compression_codec = 'LZ4'"
        + (", auto_statistics_types = ''" if warm else ""),
    )
    if warm:
        node.query(f"SELECT * FROM {table} FORMAT Null")
        node.query("SYSTEM DROP UNCOMPRESSED CACHE")

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


def test_shutdown_cancels_s3_read(cluster, broken_s3):
    """Shutdown must interrupt a background merge stuck retrying an S3 read.

    A different branch of the predicate from the arms above, and the only one shutdown can reach
    while it matters: `Context::shutdown` cancels through `MergeList::cancelAll`, which sets the
    merge list entry's own flag, and then waits for the merge executor, whereas the per-storage
    `merges_blocker` is cancelled only later, in `DatabaseCatalog::shutdown`
    (`Interpreters/Context.cpp`). That wait has no timeout: `Server.cpp`'s `safeExit` path is
    registered in a later scope guard, so it runs first and covers only connections, refresh
    tasks, background queries and backups.

    The merge is left to the background assignee. An `OPTIMIZE` would hold a client connection
    open across the shutdown, and `waitServersToFinish` would then reach that forced exit after
    `shutdown_wait_unfinished`, passing this arm with the merge never cancelled at all.
    """
    node = cluster.instances["node"]
    table = "t_shutdown"

    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    # `min_age_to_force_merge_seconds` because the selector declines two parts of this size on its
    # own heuristic ("no need to merge parts according to merge selector algorithm"), and the
    # merge has to come from the assignee rather than from `OPTIMIZE`. The short selecting sleep
    # keeps the wait for it inside `wait_for`'s window.
    node.query(
        f"""
        CREATE TABLE {table} (id UInt64, s String) ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'broken_s3', min_bytes_for_wide_part = 0,
                 min_age_to_force_merge_seconds = 1, merge_selecting_sleep_ms = 1000
        """
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    node.query(
        f"INSERT INTO {table} SELECT number, repeat('x', 200) FROM numbers(200000)"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + 200000, repeat('y', 200) FROM numbers(200000)"
    )

    # The merge must read the parts from the mock, not out of memory.
    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")

    broken_s3.setup_at_object_get(count=1000000, action="slow_down")

    try:
        before = get_attempts(node)
        node.query(f"SYSTEM START MERGES {table}")

        in_merges = (
            f"SELECT count() FROM system.merges "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        )
        assert wait_for(
            node, in_merges, lambda v: int(v) >= 1
        ), "the assignee never started the merge, so the assertions below would be vacuous"

        assert wait_for(
            node,
            ATTEMPTS_QUERY,
            lambda v: int(v or 0) - before >= ATTEMPTS_TO_ACCRUE,
        ), "the merge is not retrying the read, so there is nothing to cancel"

        # And it is still retrying as the shutdown starts: otherwise a prompt shutdown could be
        # granted by a merge that had already finished, or that something else had cancelled.
        assert (
            int(node.query(in_merges)) >= 1
        ), "the merge left system.merges before the shutdown, so this arm would be vacuous"

        # The oracle: SIGTERM completes on its own. `stop_clickhouse` returns True only when the
        # process is gone within the budget, and force-kills past it.
        assert node.stop_clickhouse(stop_wait_sec=SHUTDOWN_BUDGET_SECONDS) is True, (
            f"the server did not shut down within {SHUTDOWN_BUDGET_SECONDS}s and had to be "
            f"killed: the merge kept retrying the read after MergeList::cancelAll"
        )
    finally:
        # The mock must stop failing before the server reads these parts back on startup. The
        # stop is a no-op after the oracle and stops a still-running server after an earlier
        # failure, so the restart is deterministic either way.
        broken_s3.reset()
        node.stop_clickhouse()
        node.start_clickhouse()
        drop_table_stopping_any_merge(node, table)


# 429 and 503 answers to GET and HEAD only, so unlike `ATTEMPTS_QUERY` it moves only on the reads
# the mock answers with `slow_down`.
THROTTLED_QUERY = (
    "SELECT sumIf(value, event = 'DiskS3ReadRequestsThrottling') FROM system.events"
)


def get_throttled(node):
    return int(node.query(THROTTLED_QUERY).strip() or 0)


@pytest.mark.parametrize(
    "runner, failure",
    [
        pytest.param("background", "read", id="background"),
        pytest.param("optimize", "read", id="optimize"),
        pytest.param("dry-run", "read", id="dry-run"),
        pytest.param("background", "next-column-setup", id="next-column-setup"),
        pytest.param("background", "source-constructor", id="source-constructor"),
        pytest.param("mutation", "read", id="mutation"),
        pytest.param("mutation-probe", "read", id="mutation-probe"),
    ],
)
def test_failed_merge_stops_its_other_s3_reads(cluster, broken_s3, runner, failure):
    """A merge that fails on one S3 read must stop its other reads that are still in flight.

    A vertical merge on a remote disk prefetches the next column from every part before it
    gathers the current one, so while `a` is gathered the reads of `b` are in flight on other
    threads. They stop retrying only once the merge's cancellation predicates answer true, so a
    failure that sets nothing they read leaves them retrying to their budget, and the failed task,
    which waits for them in its destructor, keeps its `system.merges` entry, its source parts and
    the table.

    `background` is the executor's merge. `optimize` is the merge that `OPTIMIZE` runs in its own
    thread, which destroys a failed task without cancelling it. `dry-run` is `OPTIMIZE ... DRY RUN
    PARTS`, whose reads run in the group of its query rather than of the merge. All three fail on a
    read of `a`. `next-column-setup` fails while it sets up the pipeline of `b`, with the reads of
    `a` in flight. `source-constructor` fails while it builds the pipeline of `a`, in the source of
    the second part, after the source of the first part was created. `mutation` is a mutation of
    one part that fails on a read of `a` while another of its reads is in flight
    (`remote_filesystem_read_method = 'threadpool'`), and `mutation-probe` fails the same way in the
    query that counts the rows the mutation affects, in one of its streams while another of its
    streams is blocked in a read, and must report this failure rather than the cancellation of the
    blocked stream.
    """
    node = cluster.instances["node"]
    via_optimize = runner in ("optimize", "dry-run")
    is_mutation = runner.startswith("mutation")
    if failure == "read":
        table = {
            "background": "t_failed_read_bg",
            "optimize": "t_failed_read_opt",
            "dry-run": "t_failed_read_dry",
            "mutation": "t_failed_read_mut",
            "mutation-probe": "t_failed_read_mut_probe",
        }[runner]
    else:
        table = {
            "next-column-setup": "t_failed_setup_bg",
            "source-constructor": "t_failed_source_bg",
        }[failure]
    # The assignee needs `min_age_to_force_merge_seconds` to pick two parts of this size, see
    # `test_shutdown_cancels_s3_read`. A mutation reads one part, so no merge competes with it.
    num_parts = 1 if is_mutation else 2
    assignee_settings = (
        ""
        if via_optimize or is_mutation
        else ", min_age_to_force_merge_seconds = 1, merge_selecting_sleep_ms = 1000"
    )
    # A mutation fails on the sizes of `a`, see below.
    if is_mutation:
        assignee_settings += ", string_serialization_version = 'with_size_stream'"

    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'broken_s3', min_bytes_for_wide_part = 0,
                 vertical_merge_algorithm_min_rows_to_activate = 1,
                 vertical_merge_algorithm_min_columns_to_activate = 1,
                 vertical_merge_remote_filesystem_prefetch = 1{assignee_settings}
        """
    )
    optimize = None
    try:
        node.query(f"SYSTEM STOP MERGES {table}")
        for part in range(num_parts):
            node.query(
                f"INSERT INTO {table} SELECT number + {part} * 200000, "
                f"repeat('x', 100), repeat('y', 100) FROM numbers(200000)"
            )

        parts = node.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active ORDER BY name"
        ).split()
        assert len(parts) == num_parts, parts
        uuid = node.query(
            f"SELECT uuid FROM system.tables WHERE database = currentDatabase() "
            f"AND name = '{table}'"
        ).strip()

        def object_key(part, file):
            keys = node.query(
                f"SELECT remote_path FROM system.remote_data_paths WHERE disk_name = 'broken_s3' "
                f"AND endsWith(local_path, '{uuid}/{part}/{file}')"
            ).split()
            assert len(keys) == 1, (part, file, keys)
            return keys[0]

        a_keys = [object_key(part, "a.bin") for part in parts]
        b_keys = [object_key(part, "b.bin") for part in parts]

        # The merge must read the parts from the mock, not out of memory.
        node.query("SYSTEM DROP MARK CACHE")
        node.query("SYSTEM DROP UNCOMPRESSED CACHE")

        if failure == "read":
            # Not retryable, so the merge fails on this read. `NoSuchUpload` rather than
            # `AccessDenied`, which may be retried after refreshing the credentials.
            if is_mutation:
                # A mutation prefetches every stream before it reads the first one, and reads the sizes
                # of `a` before its data, so whichever column it reads first, the read of the data of `a`
                # is in flight when this one fails.
                failing_key = object_key(parts[0], "a.size.bin")
                in_flight_keys = [object_key(parts[0], "a.bin")]
            else:
                failing_key = a_keys[0]
                in_flight_keys = b_keys
            if runner == "mutation-probe":
                # The count reads the part in several streams. All of them wait for the sizes of `a`
                # until the reads of the sizes fail below.
                broken_s3.setup_at_object_get(
                    count=1000000, action="slow_down", key=failing_key
                )
            else:
                broken_s3.setup_at_object_get(action="no_such_upload", key=failing_key)
            expected_error = "upload does not exist"
        elif failure == "next-column-setup":
            node.query(f"SYSTEM ENABLE FAILPOINT {SETUP_FAILPOINT}")
            in_flight_keys = a_keys
            expected_error = "Injected failure while preparing the next column"
        else:
            # Holds the horizontal stage, which reads only `id`, in the retry loop until the failpoint is
            # enabled, so that the failpoint first fires in the sources of `a` rather than of `id`.
            id_key = object_key(parts[0], "id.bin")
            broken_s3.setup_at_object_get(count=1000000, action="slow_down", key=id_key)
            in_flight_keys = a_keys[:1]
            expected_error = "Injected failure in the constructor"
        # Retryable, so the other reads spin in the retry loop, as in the arms above.
        for key in in_flight_keys:
            broken_s3.setup_at_object_get(count=1000000, action="slow_down", key=key)

        # The count of the rows each one affects reads only `id` for the first, and `a` and `b` for the
        # second, so only the second fails in it.
        if runner == "mutation":
            node.query(
                f"ALTER TABLE {table} DELETE WHERE id % 2 = 0 SETTINGS mutations_sync = 0"
            )
        elif runner == "mutation-probe":
            node.query(
                f"ALTER TABLE {table} DELETE WHERE a = b SETTINGS mutations_sync = 0"
            )

        throttled_before = get_throttled(node)
        node.query(f"SYSTEM START MERGES {table}")
        if runner == "optimize":
            # Not `node.query`, for the same reason as in `test_stop_merges_cancels_s3_read`.
            optimize = node.get_query_request(
                f"OPTIMIZE TABLE {table} FINAL SETTINGS optimize_throw_if_noop = 0"
            )
        elif runner == "dry-run":
            optimize = node.get_query_request(
                f"OPTIMIZE TABLE {table} DRY RUN PARTS '{parts[0]}', '{parts[1]}'"
            )

        # Vacuity guard: another read of the merge reached the mock, so there is a read to stop.
        # The merge fails at about the same time, so the oracle below starts sampling here.
        assert wait_for(
            node,
            THROTTLED_QUERY,
            lambda v: int(v or 0) - throttled_before >= 1,
            attempts=120,
            sleep_time=1,
        ), "no other read of the merge reached S3, so the assertions below would be vacuous"
        if runner == "mutation-probe":
            # The first read of the sizes after this succeeds, and its stream goes on to the data of `a`,
            # so it is blocked in that read when the next stream fails.
            broken_s3.setup_at_object_get(
                after=1, action="no_such_upload", key=failing_key
            )
        if failure == "source-constructor":
            # Here the read that reached the mock is the one of `id`, held above.
            node.query(f"SYSTEM ENABLE FAILPOINT {SOURCE_FAILPOINT}")
            broken_s3.setup_at_object_get(count=0, action="slow_down", key=id_key)
        failed_at = time.monotonic()

        # The oracle: the failed task leaves `system.merges` only after every read it still had in
        # flight returned, so no entry may grow old. It is gone once every entry is younger than the
        # failure: the assignee keeps retrying the merge, and each new attempt fails the same way.
        age_query = (
            f"SELECT max(elapsed) FROM system.merges "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        )
        optimize_running_query = (
            "SELECT count() FROM system.processes "
            f"WHERE query LIKE '%OPTIMIZE TABLE {table}%' AND query NOT LIKE '%system.processes%'"
        )
        optimize_returned = not via_optimize
        while True:
            since_failure = time.monotonic() - failed_at
            age = float(node.query(age_query).strip() or 0)
            assert age < FAILED_MERGE_MAX_AGE_SECONDS, (
                f"the failed merge is still in system.merges {age:.0f}s after it started: "
                f"its other in-flight reads kept retrying"
            )
            if not optimize_returned:
                optimize_returned = int(node.query(optimize_running_query)) == 0
            if age < since_failure and optimize_returned:
                break
            assert since_failure < FAILED_MERGE_MAX_AGE_SECONDS, (
                "the failed merge did not leave system.merges"
                if optimize_returned
                else "the failed OPTIMIZE did not return: its merge's other reads kept retrying"
            )
            time.sleep(0.5)

        # Vacuity guard: the merge was vertical and failed as injected. Checked last: a failed task
        # writes this row only once its exception has left it, so a task stuck destroying its
        # in-flight reads writes none. A dry run writes no row, and its error is checked below.
        if runner != "dry-run":
            if is_mutation:
                event = "event_type = 'MutatePart'"
            else:
                event = "event_type = 'MergeParts' AND merge_algorithm = 'Vertical'"
            if failure == "source-constructor":
                # In the vertical stage: the failed attempts of the horizontal one read nothing.
                event += " AND read_rows > 0"
            node.query("SYSTEM FLUSH LOGS part_log")
            assert int(
                node.query(
                    f"SELECT count() FROM system.part_log WHERE database = currentDatabase() "
                    f"AND table = '{table}' AND {event} AND error != 0 "
                    f"AND exception LIKE '%{expected_error}%'"
                )
            ), "no merge or mutation failed as injected, so the oracle above was vacuous"
            if runner == "mutation-probe":
                # The streams that the failure stopped throw `ABORTED`, which must not be reported instead.
                other_errors = node.query(
                    f"SELECT groupUniqArray(error) FROM system.part_log WHERE database = currentDatabase() "
                    f"AND table = '{table}' AND {event} AND error != 0 "
                    f"AND exception NOT LIKE '%{expected_error}%'"
                ).strip()
                assert (
                    other_errors == "[]"
                ), f"failed attempts of the mutation reported errors {other_errors} instead of the injected one"

        if via_optimize:
            _, error = optimize.get_answer_and_error()
            optimize = None
            assert expected_error in error, error

        # A failed merge keeps its source parts, so no data is lost.
        assert int(node.query(f"SELECT count() FROM {table}")) == 200000 * num_parts
        assert (
            int(
                node.query(
                    f"SELECT count() FROM system.parts "
                    f"WHERE database = currentDatabase() AND table = '{table}' AND active"
                )
            )
            == num_parts
        )
    finally:
        if failure == "next-column-setup":
            node.query(f"SYSTEM DISABLE FAILPOINT {SETUP_FAILPOINT}")
        if failure == "source-constructor":
            node.query(f"SYSTEM DISABLE FAILPOINT {SOURCE_FAILPOINT}")
        broken_s3.reset()
        drop_table_stopping_any_merge(node, table)
        if optimize is not None:
            optimize.get_answer_and_error()


def test_failed_dry_run_retries_multipart_abort(cluster, broken_s3):
    """A failed `OPTIMIZE ... DRY RUN PARTS` must abort the multipart uploads of its merge, also
    when the first attempt of an abort fails.

    A dry run merges in the thread group of its query. When the merge fails, the dry run cancels
    its query and then the merge, so the merge aborts the uploads of its writers in a thread whose
    query is cancelled. The S3 client does not retry a request in such a thread, so one transient
    error of the abort leaves the upload incomplete in the bucket. The writer that fails aborts its
    own upload earlier, in `WriteBuffer::next`, while the query still runs, so the error of the
    abort hits another writer of the merge.
    """
    node = cluster.instances["node"]
    table = "t_failed_dry_run_abort"
    rows_per_part = 16384
    # The endpoint prefix of the disk `broken_s3_multipart`, which no other test uses.
    prefix = "data_multipart/"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cluster.minio_ip}:{cluster.minio_port}",
        aws_access_key_id=cluster.minio_access_key,
        aws_secret_access_key=cluster.minio_secret_key,
        region_name="us-east-1",
    )

    def incomplete_uploads():
        # MinIO takes a non-empty prefix of this listing as an exact key, so filter here.
        return [
            upload
            for page in s3.get_paginator("list_multipart_uploads").paginate(
                Bucket=cluster.minio_bucket
            )
            for upload in page.get("Uploads", [])
            if upload["Key"].startswith(prefix)
        ]

    def object_keys():
        return {
            obj.object_name
            for obj in cluster.minio_client.list_objects(
                cluster.minio_bucket, prefix=prefix, recursive=True
            )
        }

    aborts_query = (
        "SELECT sumIf(value, event = 'DiskS3AbortMultipartUpload') FROM system.events"
    )

    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    # The size limits keep the background merges away from the parts. `DRY RUN` ignores them.
    node.query(
        f"""
        CREATE TABLE {table} (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 'broken_s3_multipart', min_bytes_for_wide_part = 0,
                 enable_vertical_merge_algorithm = 0,
                 max_bytes_to_merge_at_max_space_in_pool = 1,
                 max_bytes_to_merge_at_min_space_in_pool = 1
        """
    )
    try:
        # The merged `a.bin` takes about 32 MiB and `b.bin` about 16 MiB. The merge writes both
        # columns row by row, so the writer of `a` opens its multipart upload at about 10 MiB, and
        # the writer of `b` opens its own one later.
        for part in range(2):
            node.query(
                f"INSERT INTO {table} SELECT number + {part} * {rows_per_part}, "
                f"randomString(1024), randomString(512) FROM numbers({rows_per_part})"
            )
        parts = node.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active ORDER BY name"
        ).split()
        assert len(parts) == 2, parts
        assert not incomplete_uploads()
        keys_before = object_keys()
        aborts_before = int(node.query(aborts_query).strip() or 0)

        # The upload of `b` fails to start. Not retryable, so the merge fails with the upload of
        # `a` open.
        broken_s3.setup_at_create_multi_part_upload(
            after=1, count=1, action="no_such_upload"
        )
        # Retryable. The writer of `b` has no upload to abort, so this is the abort of `a`.
        broken_s3.setup_at_abort_multi_part_upload(count=1, action="slow_down")

        error = node.query_and_get_error(
            f"OPTIMIZE TABLE {table} DRY RUN PARTS '{parts[0]}', '{parts[1]}'"
        )
        assert "upload does not exist" in error, error

        # Vacuity guard: the merge aborted an upload. Nothing else aborts one here, so the first
        # abort got the injected error.
        assert (
            int(node.query(aborts_query).strip() or 0) > aborts_before
        ), "the failed dry run aborted no upload, so the assertions below would be vacuous"

        # The merge failed while it wrote, before its finalization uploaded any file, so no object
        # of it may stay. A failure in the finalization would leave the small files: nothing
        # removes them, and the writers are cancelled inside the merge, before the query.
        new_keys = object_keys() - keys_before
        assert not new_keys, f"the failed dry run left objects: {new_keys}"

        # The merge waits for the abort before the query returns, so there is nothing to wait for.
        uploads = incomplete_uploads()
        assert not uploads, f"the failed dry run left incomplete uploads: {uploads}"

        # A failed merge keeps its source parts, so no data is lost.
        assert int(node.query(f"SELECT count() FROM {table}")) == 2 * rows_per_part
        assert (
            int(
                node.query(
                    f"SELECT count() FROM system.parts "
                    f"WHERE database = currentDatabase() AND table = '{table}' AND active"
                )
            )
            == 2
        )
    finally:
        broken_s3.reset()
        drop_table_stopping_any_merge(node, table)
        # A failure above must not leave the uploads to the next test.
        for upload in incomplete_uploads():
            s3.abort_multipart_upload(
                Bucket=cluster.minio_bucket,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )
