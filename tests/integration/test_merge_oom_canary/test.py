import random
import threading

import pytest

from helpers.cluster import ClickHouseCluster

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

    stop = threading.Event()

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
            node.query_and_get_answer_with_error(sql, settings=FUZZER_QUERY_SETTINGS)

    def fuzz_table_churn(t):
        # Write and merge the fuzzed table variants - parts, indexes and merges accumulate, as the ~97
        # fuzz tables did in the run.
        rng = random.Random(5000 + t)
        while not stop.is_set():
            node.query_and_get_answer_with_error(
                f"INSERT INTO num_10k__fuzz_{t} SELECT number FROM numbers({rng.randint(1000, 10000)})"
            )
            node.query_and_get_answer_with_error(f"OPTIMIZE TABLE num_10k__fuzz_{t} FINAL")
            node.query_and_get_answer_with_error(f"TRUNCATE TABLE num_10k__fuzz_{t}")

    threads = (
        [threading.Thread(target=query_worker, args=(s,)) for s in range(NUM_QUERY_WORKERS)]
        + [threading.Thread(target=fuzz_table_churn, args=(t,)) for t in range(NUM_FUZZ_TABLES)]
    )
    for th in threads:
        th.start()

    try:
        # Aggregate memory drifts over the cgroup, the kernel kills the canary, and the server runs its
        # OOM response. A cancelled merge is rescheduled, so there is no stable end state; we wait for the
        # response to reach the merge-cancellation step.
        node.wait_for_log_line("Cancelled all running merges", timeout=600)
    finally:
        stop.set()
        for th in threads:
            th.join(timeout=120)

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
