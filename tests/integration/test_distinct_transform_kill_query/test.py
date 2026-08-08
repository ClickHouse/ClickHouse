import concurrent.futures
import pytest
import time
import uuid
import threading
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


HASHMAP_FAULT_NAME = "distinct_transform_pause"
LC_FAULT_NAME = "distinct_transform_lc_pause"


def run_kill_query_failpoint_test(query, fault_name, query_id=None):
    """A `KILL QUERY` sent while a single chunk is being processed must stop the transform at the
    paused row, not after it has hashed the rest of the chunk.

    The transform pauses at the first 4096-row boundary inside the loop (the `PAUSEABLE_ONCE`
    failpoint auto-disables after that one pause). After the kill the failpoint is re-armed and
    the loop is resumed. Intra-loop polling (the PR) makes the loop return at the paused row, so
    the re-armed failpoint at the next 4096-row boundary is never hit and a second
    `SYSTEM WAIT FAILPOINT ... PAUSE` times out. If the mid-loop cancellation check were removed,
    the loop would keep hashing rows, hit the re-armed failpoint at the next boundary, and the
    second WAIT would return -- so this test would fail then instead of false-passing.
    """
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")

    thread_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                query_id=query_id,
            )
            assert "DB::Exception: Query was cancelled" in error
        except Exception as e:
            thread_error[0] = e

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        def wait_failpoint(timeout=60):
            wait_future = pool.submit(
                node1.query,
                f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
            )
            done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
            if not done:
                assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
            ## An exception in SYSTEM WAIT FAILPOINT ... PAUSE would leave the query running
            ## unsynchronized (done is still non-empty); re-raise it so the test fails loudly.
            wait_future.result()

        try:
            wait_failpoint()

            node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

            ## Re-arm the one-shot failpoint, then resume the loop. With the fix the loop
            ## returns at the paused row; without it the loop hits the next boundary and pauses
            ## again, which the second WAIT below observes.
            node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")
            node1.query(f"SYSTEM NOTIFY FAILPOINT {fault_name}")

            second_pause_future = pool.submit(
                node1.query,
                f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
            )
            done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
            if done:
                second_pause_future.result()
                assert False, "loop reached the re-armed failpoint: intra-loop cancellation is broken"
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")

    ## Bound the join so a regression that keeps the query running fails cleanly here instead of
    ## hanging the integration shard (same pattern as the soft-timeout helper).
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


def test_hashmap_kill_query(started_cluster):
    query = """SELECT DISTINCT number % 10000000
FROM numbers(10000)
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        HASHMAP_FAULT_NAME,
    )


def test_lc_kill_query(started_cluster):
    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        LC_FAULT_NAME,
    )


def run_soft_timeout_failpoint_test(query, fault_name, query_id=None, hold_seconds=30):
    """A soft `max_execution_time` with `timeout_overflow_mode = 'break'` must stop the query
    cleanly (no exception) when the deadline passes while the DISTINCT transform is paused at a
    failpoint inside a single `transform` call. The transform is held past the deadline, then the
    failpoint is released; the soft-timeout latch stops the inner loop and the query finishes.

    `interactive_delay` is passed as a client connection setting (`--interactive_delay=0`), not in
    the query `SETTINGS` clause: it is applied in `TCPHandler` before the query settings are parsed.
    Zeroing it makes the pull loop block so that neither its `checkTimeLimitSoft()` polling (every
    `interactive_delay` ms) nor the `CancellationChecker` (a no-op in 'break' mode) can stop the
    query — the transform's own soft-timeout latch becomes the only stopper, which is the behavior
    under test.

    The last two checks prove the latch fired mid-loop rather than the query merely finishing cleanly.
    After the deadline has passed and the failpoint is re-armed, the latch makes the loop return at
    the paused row, so the re-armed failpoint at the next 4096-row boundary is never hit and a second
    `SYSTEM WAIT FAILPOINT ... PAUSE` times out. If the latch were removed, the loop would keep
    hashing the remaining rows, hit the re-armed failpoint at the next boundary, and the second WAIT
    would return -- so this test would fail then instead of false-passing.
    """
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")

    thread_error = [None]
    query_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                settings={"interactive_delay": 0},
                query_id=query_id,
            )
            query_error[0] = error
        except Exception as e:
            thread_error[0] = e

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        def wait_failpoint(timeout=60):
            wait_future = pool.submit(
                node1.query,
                f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
            )
            done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
            if not done:
                assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
            ## An exception in SYSTEM WAIT FAILPOINT ... PAUSE would leave the query running
            ## unsynchronized (done is still non-empty); re-raise it so the test fails loudly.
            wait_future.result()

        try:
            wait_failpoint()

            ## Hold the transform paused past the max_execution_time deadline, then re-arm the
            ## one-shot failpoint and release it. With the latch the loop returns at the paused
            ## row; without it the loop hits the next boundary and pauses again, which the second
            ## WAIT below observes.
            time.sleep(hold_seconds)
            node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")
            node1.query(f"SYSTEM NOTIFY FAILPOINT {fault_name}")

            second_pause_future = pool.submit(
                node1.query,
                f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
            )
            done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
            if done:
                second_pause_future.result()
                assert False, "loop reached the re-armed failpoint: soft-timeout latch is broken"
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the soft timeout"
    if thread_error[0] is not None:
        raise thread_error[0]

    ## In break mode the soft timeout must not surface any error to the client.
    assert query_error[0] == "", f"break-mode soft timeout raised an error: {query_error[0]}"

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0


def test_hashmap_soft_timeout(started_cluster):
    query = """SELECT DISTINCT number % 10000000
FROM numbers(10000)
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='break', max_rows_to_read=0"""

    run_soft_timeout_failpoint_test(
        query,
        HASHMAP_FAULT_NAME,
    )


def test_lc_soft_timeout(started_cluster):
    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='break', max_rows_to_read=0"""

    run_soft_timeout_failpoint_test(
        query,
        LC_FAULT_NAME,
    )


def test_lc_soft_timeout_then_kill(started_cluster):
    """A `KILL QUERY` sent after the soft timeout has latched during the LowCardinality scan must
    stop `buildFilter` without hashing the already-committed processed prefix.

    The LC scan is paused at row 8192 (a deterministic two-hop pause; `SYSTEM NOTIFY FAILPOINT`
    keeps the wait channel alive between hops, and re-`ENABLE` re-arms the one-shot failpoint).
    Holding it past `max_execution_time` makes the scan return a partial mask with
    `processed_rows = 8192`, so `buildFilter` runs with `processed_prefix = 8192`. `buildFilter` is
    then paused at row 4096, strictly below the processed prefix, and the query is killed. Because
    `isCancelled()` is checked unconditionally, `buildFilter` returns right there. A regression that
    lets the processed prefix suppress the cancellation check would keep hashing rows [4096, 8192)
    and pause again at row 8192, which the second `SYSTEM WAIT FAILPOINT ... PAUSE` observes.

    The query must run with `--interactive_delay=0` (client connection setting): otherwise the pull
    loop's `checkTimeLimitSoft()` polling cancels the pipeline at the deadline during the hold and
    `buildFilter` never runs, so the scenario is not exercised.
    """
    query_id = str(uuid.uuid4())

    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='break', max_rows_to_read=0"""

    node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")

    thread_error = [None]
    query_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query, settings={"interactive_delay": 0}, query_id=query_id
            )
            query_error[0] = error
        except Exception as e:
            thread_error[0] = e

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        ## First LC hop: the scan pauses at row 4096. Re-arm the one-shot failpoint and resume;
        ## the scan then pauses again at row 8192.
        wait_failpoint(LC_FAULT_NAME)
        node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")
        wait_failpoint(LC_FAULT_NAME)

        ## Hold past max_execution_time so the soft timeout latches, then let the LC scan resume:
        ## it returns at row 8192, so buildFilter runs with processed_prefix = 8192. Arm the
        ## buildFilter failpoint in the same step.
        time.sleep(30)
        node1.query(f"SYSTEM ENABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")

        ## buildFilter pauses at row 4096, below the processed prefix.
        wait_failpoint(HASHMAP_FAULT_NAME)

        ## Kill while buildFilter is hashing rows below the processed prefix.
        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

        ## Re-arm the buildFilter failpoint and resume. With the fix buildFilter returns at
        ## row 4096 because isCancelled() is set, so the second WAIT below times out. If the
        ## processed prefix suppressed the cancellation check, buildFilter would keep hashing
        ## and pause again at row 8192, making the second WAIT return.
        node1.query(f"SYSTEM ENABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {HASHMAP_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {HASHMAP_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
        if done:
            second_pause_future.result()
            assert False, "buildFilter kept hashing the processed prefix after the kill"
    finally:
        ## DISABLE both failpoints: this also unblocks any still-running WAIT ... PAUSE query.
        node1.query(f"SYSTEM DISABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the kill"
    if thread_error[0] is not None:
        raise thread_error[0]

    assert "Query was cancelled" in query_error[0], f"expected cancellation, got: {query_error[0]}"

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0
