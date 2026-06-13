import random
import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Large memory-limited container with swap disabled (memswap_limit == mem_limit) so the cgroup limit is
# a hard memory.max. ClickHouse memory limits are left at their defaults (max_server_memory_usage_to_ram
# _ratio = 0.9, default merge budget) - nothing removed or relaxed, exactly as in the AST fuzzer. The
# OOM canary lets the server survive the kill (kernel kills the canary first; the server cancels merges
# and queries and relaunches it).
node = cluster.add_instance(
    "node",
    main_configs=["configs/oom.xml"],
    mem_limit="8g\n        memswap_limit: 8g",
    stay_alive=True,
)

NUM_WIDE_WORKERS = 12
NUM_ARRAY_WORKERS = 12


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


def test_fuzzer_like_load_triggers_kernel_oom_and_server_survives():
    # Reproduce the AST-fuzzer OOM. That OOM was not a single huge allocation: it was an aggregate of a
    # diverse, sustained, concurrent workload pushing total memory to the limit, with the allocator's
    # resident memory drifting above the tracker's logical accounting (retained/fragmented pages) until
    # the kernel OOM killer fired. We recreate the two dominant consumers from the fuzzer's trace:
    #
    #   * writing/merging VERY WIDE parts (a clone of system.metric_log, ~1.8k columns): each part write
    #     opens one write buffer PER COLUMN at once - this is the MergedBlockOutputStream / initSkipIndices
    #     allocation that crossed the limit in the fuzzer (MergeTreeSink::consume -> writeTempPart).
    #   * big-array queries (arrayResize / groupArray of large arrays) - the targeted fuzzer's seed.
    #
    # Run concurrently and sustained in a large container with the limits at their defaults, the way the
    # fuzzer did. The kernel OOM is expected to fire only occasionally, which is acceptable.
    if node.is_built_with_sanitizer():
        pytest.skip("Sanitizer builds change memory usage and timing, making the OOM unreliable")

    oom_count_before = crash_log_oom_count()

    # A wide table with the shape of system.metric_log (~1.8k numeric columns). Generated directly
    # rather than cloned from system.metric_log, which the integration node does not enable.
    NUM_COLUMNS = 1800
    columns = ", ".join(f"c{i} Int64" for i in range(NUM_COLUMNS))
    for w in range(NUM_WIDE_WORKERS):
        node.query(f"DROP TABLE IF EXISTS wide_{w} SYNC")
        node.query(
            f"CREATE TABLE wide_{w} (event_time DateTime, {columns}) "
            "ENGINE = MergeTree ORDER BY tuple() "
            # Keep buffers full-size so each of the ~1.8k column streams really allocates its write buffer.
            "SETTINGS min_bytes_for_wide_part = 0, min_columns_to_activate_adaptive_write_buffer = 100000"
        )

    stop = threading.Event()

    def wide_churn(w):
        # Each insert is one part; writing it opens ~1.8k column write buffers at once. Merging several
        # such parts opens them again. Reset periodically so the merges keep recurring.
        rng = random.Random(w)
        while not stop.is_set():
            for _ in range(rng.randint(2, 4)):
                node.query_and_get_answer_with_error(
                    f"INSERT INTO wide_{w} (event_time) SELECT now() FROM numbers({rng.randint(1, 64)})"
                )
            node.query_and_get_answer_with_error(f"OPTIMIZE TABLE wide_{w} FINAL")
            node.query_and_get_answer_with_error(f"TRUNCATE TABLE wide_{w}")

    def array_churn(a):
        # Large transient arrays of randomly varied size - the targeted fuzzer's seed - allocate and free,
        # feeding the allocator fragmentation that makes resident memory exceed the tracked amount.
        rng = random.Random(1000 + a)
        while not stop.is_set():
            n = rng.choice([2_000_000, 5_000_000, 9_000_000, 14_000_000])
            node.query_and_get_answer_with_error(
                f"SELECT sum(length(arrayResize(range(number % 1000), {n}))) FROM numbers(8) SETTINGS max_block_size = 1"
            )

    threads = (
        [threading.Thread(target=wide_churn, args=(w,)) for w in range(NUM_WIDE_WORKERS)]
        + [threading.Thread(target=array_churn, args=(a,)) for a in range(NUM_ARRAY_WORKERS)]
    )
    for th in threads:
        th.start()

    try:
        # Total memory ratchets up under the diverse sustained load until resident memory crosses the
        # cgroup, the kernel kills the canary, and the server runs its OOM response. A cancelled merge is
        # rescheduled, so there is no stable end state to assert on; instead we wait for the response to
        # reach the merge-cancellation step.
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

    for w in range(NUM_WIDE_WORKERS):
        node.query(f"DROP TABLE IF EXISTS wide_{w} SYNC")
