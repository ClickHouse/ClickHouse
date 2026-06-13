import random
import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A tight cgroup memory limit. The ClickHouse memory limits stay ENABLED at their defaults (0.9 of the
# cgroup), exactly as in functional tests and the AST fuzzer. The test shows these limits do not prevent
# a real kernel OOM: a long run of size-varied merges builds up allocator fragmentation/retention, so
# resident memory drifts above the tracker's logical accounting and over the cgroup before the tracker -
# which sees only its lower count - can shed it. The OOM canary lets the server survive that kill.
# Disable swap (memswap_limit == mem_limit): otherwise Docker allows up to 2x mem_limit of swap, and the
# container swaps instead of OOMing. The framework only templates the `mem_limit:` line into the compose
# file, so we append the `memswap_limit:` line (matching its 8-space indentation) through the same field.
node = cluster.add_instance(
    "node",
    main_configs=["configs/oom.xml"],
    mem_limit="8g\n        memswap_limit: 8g",
    stay_alive=True,
)

NUM_WORKERS = 24


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


def test_sustained_merges_trigger_kernel_oom_and_server_survives():
    # The ClickHouse memory tracker bounds *logical* (tracked) memory, but the kernel OOM killer acts on
    # *resident* memory (RSS). What killed the fuzzer's server was the gap between the two: under a long
    # run of size-varied allocate/free churn, the allocator cannot reuse a freed region of one size for a
    # request of another, so it maps fresh pages and retains the old ones - RSS drifts above the tracker's
    # accounting and over the cgroup limit while the tracker, seeing only its lower count, never throws.
    #
    # We reproduce that here with sustained `groupArrayState` merges of randomly varied sizes (rows, array
    # length, number of keys). Each round on its own fits under the limit, but the fragmentation they
    # accumulate ratchets resident memory up until it crosses the 8 GiB cgroup and the kernel OOM killer
    # fires. The limits stay enabled at their defaults - no merge-budget or tracker tweak, exactly as in
    # the fuzzer.
    if node.is_built_with_sanitizer():
        pytest.skip("Sanitizer builds change memory usage and timing, making the OOM unreliable")

    oom_count_before = crash_log_oom_count()

    for w in range(NUM_WORKERS):
        node.query(f"DROP TABLE IF EXISTS test_merge_oom_{w} SYNC")
        node.query(
            f"""
            CREATE TABLE test_merge_oom_{w} (id UInt8, fat_state AggregateFunction(groupArray, String))
            ENGINE = AggregatingMergeTree ORDER BY id
            """
        )

    stop = threading.Event()

    def churn(w):
        rng = random.Random(w)
        while not stop.is_set():
            # Vary the allocation footprint each round so freed regions cannot be reused, forcing the
            # allocator to keep mapping fresh pages (fragmentation -> retained, resident memory).
            rows = rng.choice([100, 200, 300, 400, 550, 700])
            arrlen = rng.choice([1500, 3000, 5000, 8000, 12000, 17000])
            keys = rng.choice([1, 2, 3, 5])
            for _ in range(rng.randint(2, 4)):
                node.query_and_get_answer_with_error(
                    f"INSERT INTO test_merge_oom_{w} "
                    f"SELECT number % {keys} AS id, "
                    f"       arrayReduce('groupArrayState', arrayMap(x -> randomPrintableASCII(100), range({arrlen}))) AS fat_state "
                    f"FROM numbers({rows})"
                )
            # Merge the varied parts (the merge faults its combined state, then frees it), then reset the
            # table so the next round starts fresh and the per-round merge stays under the limit.
            node.query_and_get_answer_with_error(f"OPTIMIZE TABLE test_merge_oom_{w} FINAL")
            node.query_and_get_answer_with_error(f"TRUNCATE TABLE test_merge_oom_{w}")

    threads = [threading.Thread(target=churn, args=(w,)) for w in range(NUM_WORKERS)]
    for th in threads:
        th.start()

    try:
        # Resident memory ratchets up across rounds until it crosses the cgroup, the kernel kills the
        # canary, and the server runs its OOM response. A cancelled merge is rescheduled, so there is no
        # stable end state to assert on; instead we wait for the response to reach the merge-cancellation
        # step.
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

    for w in range(NUM_WORKERS):
        node.query(f"DROP TABLE IF EXISTS test_merge_oom_{w} SYNC")
