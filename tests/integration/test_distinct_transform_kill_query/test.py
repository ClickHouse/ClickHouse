import concurrent.futures
import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_kill_query_failpoint_test(query, failpoint_name, expected_cancel_marker, query_id=None):
    """Kill the query while a specific `DistinctTransform`/`DistinctSetFilter` poll site is paused
    and prove that the transform stopped at that exact site.

    Every `isCancelled()` bail-out logs its own distinct `Cancelled ...` message (the server log
    line carries the query id, so `grep_in_log(query_id)` sees it). `QUERY_WAS_CANCELLED` alone
    only proves that the query eventually ended; asserting the message of the poll site this test
    targets makes the test fail if that mid-chunk check is dropped and the loop hashes the rest of
    the chunk until a later bail-out (which logs a different message), and the bounded waits make
    it fail instead of hanging when the loop never stops at all.

    Sequence: arm the failpoint before starting the query, wait (bounded) until the query parks at
    a paused 4096-row boundary inside the loop, `KILL QUERY`, then release the failpoint. The
    cancellation check right behind the failpoint observes the kill and bails with the site message.
    """
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint_name}")

    thread_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query, query_id=query_id,
            )
            assert "DB::Exception: Query was cancelled" in error
        except Exception as e:
            thread_error[0] = e

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        wait_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {failpoint_name} PAUSE",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
        if not done:
            pool.shutdown(wait=False, cancel_futures=True)
            assert False, f"Failpoint {failpoint_name} not triggered within 60 s"
        # Propagate an exception from the wait (query error/timeout) instead of falling through
        # to KILL QUERY with the query unsynchronized.
        wait_future.result()
        pool.shutdown(wait=False)

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")
    finally:
        # Releasing the failpoint unblocks the parked query thread: with the mid-loop check it
        # returns from the paused row instead of resuming the loop.
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint_name}")

    # Bounded: a regression that keeps hashing the chunk fails here instead of hanging the
    # integration shard.
    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "killed query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    assert expected_cancel_marker in cancel_log, (expected_cancel_marker, cancel_log)


def test_hashmap_kill_query(started_cluster):
    """A plain multi-type key takes the bulk hash loop of `buildDistinctFilter` without a mask;
    its only mid-chunk poll is the 4096-row boundary inside the loop."""
    query = """SELECT DISTINCT number % 10000000
FROM numbers(10000)
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1"""
    run_kill_query_failpoint_test(
        query, "distinct_transform_pause", "Cancelled while hashing rows without a mask"
    )


def create_lc_test_table():
    node1.query(
        "CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory"
    )
    node1.query("TRUNCATE TABLE lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")


def test_lc_masked_hash_kill_query(started_cluster):
    """A single `LowCardinality` key builds the dictionary-index mask first (its own poll is not
    armed here), then the masked hash loop polls at its 4096-row boundary."""
    create_lc_test_table()
    try:
        query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1"""
        run_kill_query_failpoint_test(
            query,
            "distinct_transform_pause",
            "Cancelled while hashing rows with a mask",
        )
    finally:
        node1.query("DROP TABLE IF EXISTS lc_test")


def test_lc_mask_build_kill_query(started_cluster):
    """The `DistinctLowCardinalityFilter::buildMask` scan of the dictionary indexes is polled on
    its own; here that scan is the site that must observe the kill."""
    create_lc_test_table()
    try:
        query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1"""
        run_kill_query_failpoint_test(
            query,
            "distinct_transform_lc_pause",
            "Cancelled while building the LowCardinality mask",
        )
    finally:
        node1.query("DROP TABLE IF EXISTS lc_test")


def test_null_map_scan_kill_query(started_cluster):
    """`CreatingSetsStep` wires a `DistinctTransform` with `skip_null_keys = true` into the set
    fill when the key is `Nullable` and `transform_null_in = 0` (see `CreatingSetsStep`). Its
    null-map prepass scans the whole chunk before any hashing, so the prepass poll must observe
    the kill."""
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt (k Nullable(UInt64)) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple()"
    )
    node1.query("TRUNCATE TABLE null_keys_mt")
    node1.query(
        "INSERT INTO null_keys_mt SELECT if(number % 9 = 0, NULL, number % 100) FROM numbers(1000000)"
    )
    try:
        query = """SELECT count() FROM numbers(1000000)
WHERE number IN (SELECT k FROM null_keys_mt)
FORMAT Null
SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1"""
        run_kill_query_failpoint_test(
            query,
            "distinct_transform_null_pause",
            "Cancelled while scanning the NULL map",
        )
    finally:
        node1.query("DROP TABLE IF EXISTS null_keys_mt")


def test_lc_null_mark_kill_query(started_cluster):
    """Same set fill, but with a `LowCardinality(Nullable)` key: `extractNestedColumnsAndNullMap`
    does not unwrap it, so the plain null-map scan is skipped and `markLowCardinalityNullRows`
    does the scanning - its poll must observe the kill."""
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_lc (k LowCardinality(Nullable(UInt64))) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple() "
        "SETTINGS allow_suspicious_low_cardinality_types=1"
    )
    node1.query("TRUNCATE TABLE null_keys_mt_lc")
    node1.query(
        "INSERT INTO null_keys_mt_lc SELECT if(number % 9 = 0, NULL, number % 100) FROM numbers(1000000)"
    )
    try:
        query = """SELECT count() FROM numbers(1000000)
WHERE (number, number) IN (SELECT k, k FROM null_keys_mt_lc)
FORMAT Null
SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1"""
        run_kill_query_failpoint_test(
            query,
            "distinct_transform_null_pause",
            "Cancelled while marking LowCardinality NULL rows",
        )
    finally:
        node1.query("DROP TABLE IF EXISTS null_keys_mt_lc")
