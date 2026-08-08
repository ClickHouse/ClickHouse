import random
import sys
import threading
import time

import pytest

from helpers.client import QueryTimeoutExceedException
from helpers.cluster import ClickHouseCluster

# This test is a manual reproduction, not an automatic regression test, so it is skipped in CI.
#
# It replays the exact AST-fuzzer scenario, but the kernel OOM it asserts cannot fire deterministically
# in the integration harness: there the docker `mem_limit` is NOT enforced as a hard cgroup `memory.max`
# (docker-in-docker, --cgroupns=host), so the workload never crosses a hard limit and the test only burns
# its 600s timeout. It would also pass on a foreground-query OOM rather than a merge-driven one, so it is
# not a faithful merge-memory signal either.
#
# The deterministic regression - that a merge's `peak_memory_usage` exceeds the per-query
# `max_memory_usage` - is covered by the stateless test
# `0_stateless/04402_merge_memory_unbounded_by_query_limit.sql`. The reliable kernel-OOM reproduction
# (creating a real enforced cgroup) lives next to this file in `manual_cgroup_oom_repro.py`. Run this
# module manually by removing the skip below on a host where the OOM can actually fire.
pytestmark = pytest.mark.skip(
    reason="Manual reproduction: the kernel OOM is not deterministic in the docker-in-docker harness "
    "(docker mem_limit is not a hard cgroup memory.max). The deterministic regression is the stateless "
    "test 04402_merge_memory_unbounded_by_query_limit; the reliable OOM repro is manual_cgroup_oom_repro.py."
)

cluster = ClickHouseCluster(__file__)

# Large memory-limited container with swap disabled (memswap_limit == mem_limit) so that, as on the
# fuzzer's host, reaching the limit means a kernel OOM rather than swapping. ClickHouse memory settings
# are left at their defaults - the OOM is provoked by the workload, not by tuning the allocator. The OOM
# canary lets the server survive the kill.
node = cluster.add_instance(
    "node",
    main_configs=["configs/oom.xml"],
    mem_limit="8g\n        memswap_limit: 8g",
    stay_alive=True,
)

# Per-query memory limit used by the AST fuzzer (ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml).
FUZZER_QUERY_SETTINGS = {"max_memory_usage": 10_000_000_000}

NUM_FUZZ_TABLES = 32
NUM_QUERY_WORKERS = 24

# Bound every worker query. The integration client waits up to 600s (DEFAULT_QUERY_TIMEOUT) for a query
# before it gives up, so after `stop` is set - or once the server is OOM-killed or otherwise wedged - a
# worker blocked in `INSERT`/`OPTIMIZE` would keep its thread alive far past the join below. Since the
# threads would otherwise be non-daemon, the pytest process would then wait for them at exit and hang the
# manual run for minutes, in exactly the wedged/OOMed case this module is meant to inspect.
# `connect_timeout`/`receive_timeout` bound the query server-side; the client-side `timeout` is a hard
# backstop that kills a stuck `clickhouse client`. A timeout is expected under OOM once the workload has
# made progress, and is then swallowed so the caller re-checks `stop` on the next iteration; before the
# first successful worker query it is treated as a workload failure (see `run_worker_query`). Mirrors
# manual_cgroup_oom_repro.py.
WORKER_QUERY_TIMEOUT = 20
WORKER_QUERY_SETTINGS = {"connect_timeout": 5, "receive_timeout": 10}

# The workload fails closed, mirroring manual_cgroup_oom_repro.py: `query_and_get_answer_with_error`
# only raises on the timeout backstop and otherwise returns an `(answer, error)` tuple even when the
# client exits non-zero, so a workload broken from the start (connection refused, a SQL error, an early
# `MEMORY_LIMIT_EXCEEDED`) would otherwise loop on failed queries for the whole wait window and be
# reported as a generic timeout. A worker that sees an error - or a query timeout - before any worker
# query has succeeded records the real failure and aborts the run; once `workload_ok` is set, errors
# and timeouts are expected and tolerated - queries start dying with memory errors, timeouts and lost
# connections as the cgroup fills and the canary is killed.
stop = threading.Event()
workload_ok = threading.Event()  # set on the first fully successful worker query
workload_failures = []  # pre-success failures as (sql, error); append is atomic, only [0] is read


def run_worker_query(sql, settings=None):
    if stop.is_set():
        return
    merged = dict(WORKER_QUERY_SETTINGS)
    if settings:
        merged.update(settings)
    try:
        _, error = node.query_and_get_answer_with_error(
            sql, timeout=WORKER_QUERY_TIMEOUT, settings=merged
        )
    except QueryTimeoutExceedException:
        # Expected under memory pressure or after the OOM kill - but only once the workload has made
        # progress. Before any worker query has succeeded, a timeout is just another way the workload
        # never got off the ground (a wedged or half-started server), and swallowing it would let
        # every worker time out for the whole wait window and hide behind the generic
        # `wait_for_log_line` timeout - so it follows the same fail-closed rule as the error path
        # below. After `workload_ok`, the caller re-checks `stop`.
        if not workload_ok.is_set():
            workload_failures.append((sql, f"timed out after {WORKER_QUERY_TIMEOUT}s"))
            stop.set()
        return
    if not error:
        workload_ok.set()
    elif not workload_ok.is_set():
        # Fail closed: the workload is broken, not OOMed - record the real error and abort all workers.
        workload_failures.append((sql, error))
        stop.set()


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown(ignore_fatal=True)


def crash_log_oom_count():
    node.query("SYSTEM FLUSH LOGS crash_log")
    return int(
        node.query(
            "SELECT count() FROM system.crash_log "
            "WHERE signal = 9 AND signal_description LIKE '%OOM Canary%'"
        ).strip()
    )


def test_fuzzer_scenario_triggers_kernel_oom_and_server_survives():
    # Replay the exact scenario from the AST-fuzzer OOM
    # (https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=107389&sha=ec12cb3ce0a49a403226cb0668b092f02a2fa3f6&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29).
    # The fuzzer.log shows the targeted/old_compatibility fuzzer seeding from the bitmap-join test
    # 01552_impl_aggfunc_cloneresize.sql (test_bm / test_bm_join, groupBitmapState + bitmapOrCardinality)
    # and the PR's arrayResize test (04327), then mutating them into:
    #   * bitmap self-joins with non-equi conditions (test_bm RIGHT JOIN test_bm ON lessOrEquals(...));
    #   * arrayResize(..., N) with huge N (up to 2^31) over num_10k;
    #   * ~97 fuzzed MergeTree table variants (num_10k__fuzz_N, test_bm__fuzz_N) being written and merged.
    # No single query exceeded its 10 GiB limit; the server died from the AGGREGATE of this diverse,
    # concurrent load plus background merges and wide system-log writes, with the allocator's resident
    # memory drifting above the tracker until the kernel OOM killer fired. The kernel OOM is expected
    # only occasionally here, which is acceptable.
    if node.is_built_with_sanitizer():
        pytest.skip("Sanitizer builds change memory usage and timing, making the OOM unreliable")

    oom_count_before = crash_log_oom_count()

    # Seed tables, exactly as in 01552_impl_aggfunc_cloneresize.sql and the fuzzer's num_10k.
    node.query("DROP TABLE IF EXISTS num_10k SYNC")
    node.query("CREATE TABLE num_10k (number UInt64) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO num_10k SELECT number FROM numbers(10000)")
    node.query("DROP TABLE IF EXISTS test_bm SYNC")
    node.query("CREATE TABLE test_bm (dim UInt64, id UInt64) ENGINE = MergeTree ORDER BY (dim, id)")
    node.query("INSERT INTO test_bm VALUES (1,1),(2,2),(3,3),(4,4)")
    node.query("DROP TABLE IF EXISTS test_bm_join SYNC")
    node.query("CREATE TABLE test_bm_join (dim UInt64, id UInt64) ENGINE = MergeTree ORDER BY (dim, id)")
    for t in range(NUM_FUZZ_TABLES):
        node.query(f"DROP TABLE IF EXISTS num_10k__fuzz_{t} SYNC")
        node.query(f"CREATE TABLE num_10k__fuzz_{t} (number UInt64) ENGINE = MergeTree ORDER BY tuple()")

    # The explosive bitmap self-join mutation, taken verbatim from fuzzer.log.
    BITMAP_QUERY = (
        "SELECT dim, sum(idnum) FROM test_bm_join RIGHT JOIN (SELECT dim, bitmapOrCardinality(ids, ids2) AS idnum "
        "FROM (SELECT dim, groupBitmapState(toUInt64(id)) AS ids FROM test_bm WHERE dim > 2 GROUP BY dim) AS A "
        "ALL RIGHT JOIN (SELECT dim, groupBitmapState(toUInt64(id)) AS ids2 FROM test_bm "
        "RIGHT JOIN test_bm AS alias65 ON lessOrEquals(alias65.id, id) WHERE dim < 2 GROUP BY dim) AS B "
        "USING (dim)) AS C USING (dim) GROUP BY dim"
    )

    def query_worker(seed):
        rng = random.Random(seed)
        while not stop.is_set():
            kind = rng.randint(0, 3)
            if kind == 0:
                # arrayResize(..., N) over num_10k with varied huge N - the PR's function, the fuzzer's seed.
                n = rng.choice([200_000, 800_000, 2_000_000, 6_000_000])
                sql = f"SELECT sum(length(arrayResize(range(number % 100), {n}))) FROM num_10k SETTINGS max_block_size = {rng.choice([64, 1024, 8192])}"
            elif kind == 1:
                sql = BITMAP_QUERY
            elif kind == 2:
                # num_10k aggregations, as in the log's tail.
                sql = "SELECT * FROM (SELECT sum(number) FROM num_10k UNION ALL SELECT sum(number) FROM num_10k) LIMIT 1"
            else:
                # groupArray of large strings - another varied large aggregate state.
                sql = f"SELECT length(groupArray(repeat('x', {rng.choice([100, 1009, 4099])}))) FROM num_10k"
            run_worker_query(sql, settings=FUZZER_QUERY_SETTINGS)

    def fuzz_table_churn(t):
        # Write and merge the fuzzed table variants - parts, indexes and merges accumulate, as the ~97
        # fuzz tables did in the run.
        rng = random.Random(5000 + t)
        while not stop.is_set():
            run_worker_query(
                f"INSERT INTO num_10k__fuzz_{t} SELECT number FROM numbers({rng.randint(1000, 10000)})"
            )
            run_worker_query(f"OPTIMIZE TABLE num_10k__fuzz_{t} FINAL")
            run_worker_query(f"TRUNCATE TABLE num_10k__fuzz_{t}")

    # Daemon threads so a worker that is somehow still blocked cannot keep the pytest process alive at
    # exit (a non-daemon thread would be joined implicitly, hanging the run).
    threads = (
        [threading.Thread(target=query_worker, args=(s,), daemon=True) for s in range(NUM_QUERY_WORKERS)]
        + [threading.Thread(target=fuzz_table_churn, args=(t,), daemon=True) for t in range(NUM_FUZZ_TABLES)]
    )
    for th in threads:
        th.start()

    try:
        # Aggregate memory drifts over the cgroup, the kernel kills the canary, and the server runs its
        # OOM response. A cancelled merge is rescheduled, so there is no stable end state; we wait for the
        # response to reach the merge-cancellation step. The wait is chunked so that a workload aborted by
        # a pre-success worker failure stops within one chunk and reports the real error below, instead of
        # burning the whole window and reporting a generic timeout. `wait_for_log_line` raises when its
        # chunk elapses without a match; the look-behind re-scan makes the chunks lose no log lines.
        deadline = time.monotonic() + 600
        while True:
            try:
                node.wait_for_log_line("Cancelled all running merges", timeout=30)
                break
            except Exception:
                if stop.is_set() or time.monotonic() >= deadline:
                    raise
    finally:
        stop.set()
        # The join window comfortably exceeds the per-call worker timeout above, so a worker mid-query
        # when `stop` was set still finishes here rather than lingering.
        for th in threads:
            th.join(timeout=30)
        stuck = [th for th in threads if th.is_alive()]
        if stuck:
            print(
                f"WARNING: {len(stuck)} worker(s) still running after join "
                "(a query did not return within its timeout)",
                file=sys.stderr,
            )
        # Fail closed on a workload that never got off the ground: surface the first real worker error
        # instead of the `wait_for_log_line` timeout it would otherwise hide behind.
        if workload_failures and not workload_ok.is_set():
            failed_sql, error = workload_failures[0]
            pytest.fail(
                f"A worker query failed before any worker query succeeded - the workload is broken, "
                f"not OOMed. First failure: {failed_sql!r}: {error}"
            )

    # The kernel OOM killer really fired: the canary died with cgroup OOM evidence, recorded in
    # system.crash_log.
    assert crash_log_oom_count() > oom_count_before

    # The server survived the OOM and is still serving queries.
    assert node.query("SELECT 1").strip() == "1"

    node.query("DROP TABLE IF EXISTS num_10k SYNC")
    node.query("DROP TABLE IF EXISTS test_bm SYNC")
    node.query("DROP TABLE IF EXISTS test_bm_join SYNC")
    for t in range(NUM_FUZZ_TABLES):
        node.query(f"DROP TABLE IF EXISTS num_10k__fuzz_{t} SYNC")
