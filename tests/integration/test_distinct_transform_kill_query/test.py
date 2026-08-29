import concurrent.futures
import os
import pytest
import signal
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
NULL_FAULT_NAME = "distinct_transform_null_pause"
FILTER_FAULT_NAME = "distinct_transform_filter_pause"
EXECUTOR_TIMEOUT_FAULT_NAME = "distinct_transform_soft_timeout_executor"


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


def test_lc_soft_timeout_executor_cancel(started_cluster):
    """A break-mode `max_execution_time` observed by the executor poll loop instead of by the
    transform must still preserve the committed chunk prefix.

    The query runs with the default `interactive_delay`, so TCPHandler drives the pipeline via
    `PullingAsyncPipelineExecutor` and its `pull()` polls `checkTimeLimitSoft()` every
    `interactive_delay` ms. When the deadline expires while the LC scan is paused at the failpoint,
    that poll cancels the whole pipeline with `CancelledByTimeout`: `is_cancelled` is set on every
    processor while `cancel_reason` stays `UNDEFINED`. The old code short-circuited on
    `isCancelled()` before latching the soft timeout and then `chunk.clear()`ed the chunk, dropping
    the already-committed [0, 4096) prefix. The fix treats the executor-side cancel as a soft
    timeout, so the partial mask is kept, `buildFilter` runs with `processed_prefix = 4096`, and it
    pauses at the re-armed `distinct_transform_pause` at row 4096. Without the fix `buildFilter`
    never runs and the re-armed failpoint is never hit, so the discriminator WAIT below times out.
    """
    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='break', max_rows_to_read=0"""

    query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")

    thread_error = [None]
    query_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                ## Deliberately no `interactive_delay` override, so the TCPHandler pull loop's
                ## `checkTimeLimitSoft()` polling stays enabled and cancels the pipeline by timeout
                ## while the transform is held at the failpoint.
                query_id=query_id,
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
        ## The LC scan pauses at row 4096 with rows [0, 4096) already committed.
        wait_failpoint(LC_FAULT_NAME)

        ## Hold past max_execution_time so the deadline expires mid-hold. In break mode the
        ## `CancellationChecker` only calls `checkTimeLimit()` (which for `overflow_mode = BREAK` returns
        ## false and sets no cancel reason), so it never hard-cancels; the executor's `checkTimeLimitSoft`
        ## is the sole observer and cancels with `CancelledByTimeout` (`cancel_reason` stays UNDEFINED).
        ## The test is therefore deterministic: the re-armed failpoint below can only be reached via the
        ## executor-side soft-timeout path.
        time.sleep(8)

        ## Arm the buildFilter failpoint, then let the LC scan resume. `is_cancelled` is set, so the
        ## scan returns a partial mask for [0, 4096). With the fix the chunk is kept (the executor-
        ## side cancel is a soft timeout), `buildFilter` runs with `processed_prefix = 4096` and
        ## pauses at the re-armed `distinct_transform_pause` at row 4096. Without the fix the chunk
        ## is cleared and `buildFilter` never runs, so this WAIT times out.
        node1.query(f"SYSTEM ENABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")

        build_filter_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {HASHMAP_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([build_filter_pause_future], timeout=10)
        if not done:
            assert False, "buildFilter did not run on the committed prefix after the executor-side cancel"
        build_filter_pause_future.result()

        ## Let `buildFilter` finish: it breaks at the paused row (soft timeout latched) and the
        ## query completes break-mode successfully.
        node1.query(f"SYSTEM NOTIFY FAILPOINT {HASHMAP_FAULT_NAME}")
    finally:
        ## DISABLE both failpoints: this also unblocks any still-running WAIT ... PAUSE query.
        node1.query(f"SYSTEM DISABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the executor-side soft timeout"
    if thread_error[0] is not None:
        raise thread_error[0]

    ## In break mode the soft timeout must not surface any error to the client.
    assert query_error[0] == "", f"break-mode soft timeout raised an error: {query_error[0]}"

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0


def test_lc_soft_timeout_executor_throw(started_cluster):
    """A 'throw' mode `max_execution_time` observed by the executor poll loop must raise
    TIMEOUT_EXCEEDED, not preserve a break-style prefix.

    Same setup as `test_lc_soft_timeout_executor_cancel` but with `timeout_overflow_mode='throw'`.
    `PullingAsyncPipelineExecutor::pull()` polls `checkTimeLimitSoft()` on every poll regardless of
    `timeout_overflow_mode`, and `QueryStatus::checkTimeLimitSoft()` always uses `OverflowMode::BREAK`,
    so the pipeline is cancelled with `CancelledByTimeout` (`cancel_reason` stays `UNDEFINED`) even in
    throw mode. With the fix `isCancelledBySoftTimeout()` is gated on the query timeout mode and returns
    false for throw mode, so `buildFilter` (and the LC scan) reach `timeoutShouldThrow()` and raise
    `TIMEOUT_EXCEEDED` via `checkTimeLimit()`. The old code treated the executor-side cancel as a soft
    break timeout and silently preserved/returned a partial prefix instead of erroring.
    """
    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='throw', max_rows_to_read=0"""

    query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")
    node1.query(f"SYSTEM ENABLE FAILPOINT {EXECUTOR_TIMEOUT_FAULT_NAME}")

    thread_error = [None]
    query_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                ## Deliberately no `interactive_delay` override, so the TCPHandler pull loop's
                ## `checkTimeLimitSoft()` polling stays enabled and cancels the pipeline by timeout
                ## while the transform is held at the failpoint.
                query_id=query_id,
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
        ## The LC scan pauses at row 4096 with rows [0, 4096) already committed.
        wait_failpoint(LC_FAULT_NAME)

        ## Hold past max_execution_time so the deadline expires mid-hold. With the default
        ## interactive_delay the pull loop observes it via checkTimeLimitSoft() and cancels the
        ## pipeline with CancelledByTimeout (`cancel_reason` stays UNDEFINED), then blocks the
        ## pipeline thread at the failpoint until we notify it.
        time.sleep(8)

        ## Do NOT arm the hashmap failpoint: in throw mode `buildFilter` must throw at the
        ## `isCancelled()` check (`timeoutShouldThrow` -> `checkTimeLimit()`) before it could pause
        ## there, so the query fails with TIMEOUT_EXCEEDED rather than pausing or returning a partial
        ## result. Without the fix the executor-side cancel is wrongly treated as a soft break timeout
        ## and the query returns a partial prefix without error.
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")

        ## The `CancellationChecker` can also deliver a `TIMEOUT_EXCEEDED` (it cancels the query at the
        ## same deadline), so the assertion above alone does not prove the *executor-side* throw path.
        ## The dedicated failpoint fires only inside this transform's `timeoutShouldThrow()` branch,
        ## i.e. once the pipeline thread resumes from the hold and the executor poll loop reports the
        ## timeout. If the checker had won, this branch would still be reached (the failpoint wait is
        ## not interruptible by `cancelQuery`), proving the executor-side raise regardless of who noticed
        ## the deadline first. A thrown `TIMEOUT_EXCEEDED` from this branch is the contract under test.
        wait_failpoint(EXECUTOR_TIMEOUT_FAULT_NAME)
        node1.query(f"SYSTEM NOTIFY FAILPOINT {EXECUTOR_TIMEOUT_FAULT_NAME}")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {EXECUTOR_TIMEOUT_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the throw-mode timeout"
    if thread_error[0] is not None:
        raise thread_error[0]

    ## In throw mode the timeout must surface as TIMEOUT_EXCEEDED to the client.
    assert "DB::Exception: Timeout exceeded" in query_error[0], (
        f"throw-mode timeout did not raise TIMEOUT_EXCEEDED: {query_error[0]!r}"
    )

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0


def test_null_keys_kill_query(started_cluster):
    """A `KILL QUERY` during the `skip_null_keys` prefilter of a `CreatingSetsStep` set build must
    stop the transform at the paused row, not after the whole chunk is scanned.

    `CreatingSetsStep` wires a `DistinctTransform` with `skip_null_keys = true` into the set build when
    the key is `Nullable`/`LowCardinality(Nullable)` and `transform_null_in = 0` (see
    CreatingSetsStep.cpp). That transform scans the whole chunk in `transform()`'s null-map prefilter
    before any of the hash-loop polls, so a cancellation arriving there used to wait for a full chunk.
    The prefilter now polls every 4096 rows; this test pauses it at the failpoint, kills the query, and
    asserts the kill is honored (the re-armed failpoint must NOT be hit again).
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt (k Nullable(UInt64)) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple()"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt")
    node1.query(
        "INSERT INTO null_keys_mt SELECT if(number % 9 = 0, NULL, number % 100) FROM numbers(1000000)"
    )

    query = """SELECT count() FROM numbers(1000000)
WHERE number IN (SELECT k FROM null_keys_mt)
FORMAT Null
SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1, max_rows_to_read=0"""

    query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        ## The null-map prefilter pauses at row 4096 while building the IN set.
        wait_failpoint(NULL_FAULT_NAME)

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

        ## Re-arm the one-shot failpoint, then resume the loop. With the fix the loop returns at the
        ## paused row on `isCancelled()`; without it the loop keeps scanning (it reaches the re-armed
        ## failpoint at the next boundary, which the second WAIT below observes).
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
        if done:
            second_pause_future.result()
            assert False, "loop reached the re-armed failpoint: intra-loop cancellation is broken"
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "killed query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0


def test_lc_null_keys_kill_query(started_cluster):
    """Same as `test_null_keys_kill_query`, but the set key is `LowCardinality(Nullable(UInt64))`, so
    the `skip_null_keys` prefilter also runs `markLowCardinalityNullRows` (the `LowCardinality(Nullable)`
    path) in addition to the null-map scan. Cancellation must be honored there too, not after the whole
    chunk is marked.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_lc (k LowCardinality(Nullable(UInt64))) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple() "
        "SETTINGS allow_suspicious_low_cardinality_types=1"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt_lc")
    node1.query(
        "INSERT INTO null_keys_mt_lc SELECT if(number % 9 = 0, NULL, number % 100) FROM numbers(1000000)"
    )

    query = """SELECT count() FROM numbers(1000000)
WHERE (number, number) IN (SELECT k, k FROM null_keys_mt_lc)
FORMAT Null
SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1, max_rows_to_read=0"""

    query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        wait_failpoint(NULL_FAULT_NAME)

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
        if done:
            second_pause_future.result()
            assert False, "loop reached the re-armed failpoint: intra-loop cancellation is broken"
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "killed query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0


def test_lc_null_keys_break_prefix(started_cluster):
    """Break-mode soft timeout must be honored inside the `skip_null_keys` null-marking prepass of a
    `LowCardinality(Nullable)` set build, preserving the *whole* multi-slice committed prefix through the
    downstream `buildLowCardinalityMask`.

    Regression for the Blocker where a soft timeout latched upstream (inside `markLowCardinalityNullRows`)
    caused `buildLowCardinalityMask` to drop the tail of the committed prefix at its own 4096-row boundary.
    The test pauses the prepass at the first boundary, resumes to commit a *second* 4096-row slice (so the
    committed prefix spans two slices), then latches the real soft timeout and releases the prepass. With the
    fix the prepass stops at the paused row and `buildLowCardinalityMask` scans the whole pre-latched prefix;
    without it the scan drops `[4096, begin)` of the committed prefix.

    The preserved prefix cannot be observed from the client (an interrupted break-mode query returns no rows),
    so the test asserts the downstream contract directly: after the prepass breaks, `buildLowCardinalityMask`
    must scan the *whole* committed 8192-row prefix. It pauses on `distinct_transform_lc_pause` at row 4096, and
    a re-armed pause must be reached again at row 8192; if the prefix were truncated downstream to 4096 only the
    first pause would occur. The test also asserts the upstream control flow (prepass pauses on the second
    boundary, never a third) and that break mode finishes with no error.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_lc (k LowCardinality(Nullable(UInt64))) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple() "
        "SETTINGS allow_suspicious_low_cardinality_types=1"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt_lc")
    ## Partition 0 large enough to reach the second 4096-row sub-range (and hit the failpoint); partitions
    ## 1/2/3 stay below the boundary so only partition 0 can pause. No NULLs, so the null-filter keeps the
    ## whole committed 8192-row prefix and `buildLowCardinalityMask` scans it in full.
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(0) FROM numbers(1000000)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(1) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(2) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(3) FROM numbers(100)")

    query = """SELECT count() FROM numbers(1000000)
 WHERE number IN (SELECT k FROM null_keys_mt_lc)
 FORMAT Null
 SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1,
          max_execution_time=5, timeout_overflow_mode='break'"""

    query_id = str(uuid.uuid4())
    ## Arm both failpoints up front: the prepass pauses on `null_pause`, and the pre-latched
    ## `buildLowCardinalityMask` (which runs after the prepass breaks) must already find `lc_pause`
    ## armed when it reaches its first 4096-row boundary.
    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
    node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        ## First 4096-row boundary: the null-marking prepass pauses after committing the first slice.
        wait_failpoint(NULL_FAULT_NAME)
        ## Re-arm and resume so the prepass commits a *second* 4096-row slice and pauses again at the
        ## second boundary. This is the multi-slice prefix the upstream commit must preserve wholesale.
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=15)
        if not done:
            assert False, "prepass did not reach the second 4096-row boundary: multi-slice prefix path not exercised"
        second_pause_future.result()
        ## Hold past the deadline so the soft timeout latches while two slices are already committed, then
        ## release. With the fix the prepass stops at the paused row (committed 2-slice prefix preserved
        ## through `buildLowCardinalityMask`, which must scan the whole pre-latched prefix); without it the
        ## scan drops the tail of the committed prefix at its own 4096-boundary.
        time.sleep(6)
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        ## With the fix the prepass breaks at the paused row and never pauses a third time.
        third_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([third_pause_future], timeout=10)
        if done:
            third_pause_future.result()
            assert False, "prepass reached a third boundary: soft-timeout latch is broken"

        ## Downstream observation: `buildLowCardinalityMask` must scan the *whole* committed 8192-row prefix,
        ## not drop `[4096, 8192)`. It pauses on `lc_pause` at row 4096; re-arm and assert it reaches a second
        ## pause at row 8192 (the full prefix). If the prefix were truncated downstream, only one pause occurs.
        wait_failpoint(LC_FAULT_NAME)
        node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")
        second_downstream_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {LC_FAULT_NAME} PAUSE")
        done, _ = concurrent.futures.wait([second_downstream_future], timeout=10)
        if not done:
            assert False, "buildLowCardinalityMask dropped the committed prefix: second LC pause not reached"
        second_downstream_future.result()
        ## Release the second pause so the query can finish (break mode returns 0 rows to the client).
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {LC_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the soft timeout"
    if thread_error[0] is not None:
        raise thread_error[0]
    ## Break mode must not surface an error to the client.
    assert query_error[0] == "", f"break-mode soft timeout raised an error: {query_error[0]}"


def test_lc_null_keys_break_then_kill(started_cluster):
    """A BREAK-mode soft timeout latched upstream in the `skip_null_keys` prepass must not suppress a later
    `KILL QUERY` while the resumed pre-latched `buildLowCardinalityMask` is mid-scan.

    The reviewer flagged that once `pre_latched` is true the per-boundary `isCancelled()` poll could be skipped
    together with the soft-timeout truncation branch, so a kill landing after the timeout would only be noticed
    after the whole committed prefix is rescanned. The KILL check (`isCancelled() && !isCancelledBySoftTimeout()`)
    is intentionally *not* guarded by `pre_latched` and runs at every 4096-row boundary; this test proves it by
    latching the break timeout in the prepass (via `distinct_transform_null_pause`, committing a 2-slice prefix),
    letting the pre-latched `buildLowCardinalityMask` resume and pause on `distinct_transform_lc_pause`, issuing
    `KILL QUERY`, and asserting the resumed scan aborts before reaching its second LC pause.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_lc (k LowCardinality(Nullable(UInt64))) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple() "
        "SETTINGS allow_suspicious_low_cardinality_types=1"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt_lc")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(0) FROM numbers(1000000)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(1) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(2) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(3) FROM numbers(100)")

    query = """SELECT count() FROM numbers(1000000)
 WHERE number IN (SELECT k FROM null_keys_mt_lc)
 FORMAT Null
 SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1,
          max_execution_time=5, timeout_overflow_mode='break'"""

    query_id = str(uuid.uuid4())
    ## Arm both failpoints up front: the prepass pauses on `null_pause`, and the pre-latched
    ## `buildLowCardinalityMask` (which runs after the prepass breaks) must already find `lc_pause`
    ## armed when it reaches its first 4096-row boundary.
    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
    node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        ## Latch the BREAK-mode soft timeout inside the `skip_null_keys` prepass, committing a 2-slice prefix.
        wait_failpoint(NULL_FAULT_NAME)
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")
        second_pause_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE")
        done, _ = concurrent.futures.wait([second_pause_future], timeout=15)
        if not done:
            assert False, "prepass did not reach the second boundary: prefix path not exercised"
        second_pause_future.result()
        time.sleep(6)  # past the 5s deadline -> break timeout latches upstream
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        ## The resumed `buildLowCardinalityMask` runs pre-latched and pauses on the already-armed LC failpoint.
        wait_failpoint(LC_FAULT_NAME)

        ## Kill while the pre-latched scan is paused; the resumed scan must honor it before its next boundary.
        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

        node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")

        second_lc_pause_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {LC_FAULT_NAME} PAUSE")
        done, _ = concurrent.futures.wait([second_lc_pause_future], timeout=10)
        if done:
            second_lc_pause_future.result()
            assert False, "pre-latched buildLowCardinalityMask reached a second LC pause: KILL was not honored"
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {LC_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "killed query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]
    assert "Query was cancelled" in query_error[0], f"expected KILL to cancel the query, got: {query_error[0]}"


def test_lc_null_keys_break_then_client_cancel(started_cluster):
    """A real client-initiated `Cancel` (Ctrl+C) arriving after a BREAK-mode soft timeout has latched must be
    honored as a hard cancellation, not misclassified as a soft timeout that lets the resumed pre-latched
    `buildLowCardinalityMask` keep running.

    A TCP `Cancel` does not, by itself, set `QueryStatus::cancel_reason` on the server (unlike `KILL QUERY`), so
    before the fix `isCancelledBySoftTimeout()` returned true for it (break mode + `cancel_reason == UNDEFINED` +
    latched timeout) and the resumed scan was treated as a harmless soft timeout. The fix makes
    `TCPHandler::processCancel` propagate `CANCELLED_BY_USER` for the remote `Cancel`, so the hard-cancel check
    (`isCancelled() && !isCancelledBySoftTimeout()`) fires and the query is aborted. This test proves the client
    `Cancel` is actually honored end-to-end: it latches the break timeout in the prepass (via
    `distinct_transform_null_pause`, committing a 2-slice prefix), lets the pre-latched `buildLowCardinalityMask`
    resume and pause on `distinct_transform_lc_pause`, sends a real client `Cancel` (SIGINT to the
    framework-managed `clickhouse-client` process, which delivers a Cancel packet on the query's own TCP
    connection), and asserts the query ends with a clean cancel (EOF, no exception text) and records
    `cancel_reason = CANCELLED_BY_USER`, rather than being silently downgraded to a soft timeout. The downstream
    abort (hard-cancel check firing at the next boundary) is what makes the cancellation take effect before the
    scan resumes; without the fix the query would instead be kept alive as a soft timeout.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_lc (k LowCardinality(Nullable(UInt64))) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple() "
        "SETTINGS allow_suspicious_low_cardinality_types=1"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt_lc")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(0) FROM numbers(1000000)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(1) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(2) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_lc SELECT toNullable(3) FROM numbers(100)")

    query = """SELECT count() FROM numbers(1000000)
     WHERE number IN (SELECT k FROM null_keys_mt_lc)
     FORMAT Null
     SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1,
              max_execution_time=5, timeout_overflow_mode='break'"""

    query_id = str(uuid.uuid4())

    thread_error = [None]
    query_error = [None]

    ## Arm both failpoints BEFORE spawning the client: the prepass pauses on `null_pause`, and the
    ## pre-latched `buildLowCardinalityMask` (which runs after the prepass breaks) must already find `lc_pause`
    ## armed when it reaches its first 4096-row boundary. Enabling them first removes the race where the
    ## query could reach or pass the synchronization points before the failpoints existed.
    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
    node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")

    ## `get_query_request` spawns the framework-managed `clickhouse-client` subprocess and returns the
    ## `CommandRequest` whose `.process` is the live `Popen`; we keep it so we can signal the exact client
    ## (delivering a real TCP Cancel) without hunting for its PID.
    req = node1.get_query_request(
        query,
        settings={"interactive_delay": 0},
        query_id=query_id,
    )

    def execute_query():
        try:
            _, error = req.get_answer_and_error()
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
        ## Latch the BREAK-mode soft timeout inside the `skip_null_keys` prepass, committing a 2-slice prefix.
        wait_failpoint(NULL_FAULT_NAME)
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")
        second_pause_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE")
        done, _ = concurrent.futures.wait([second_pause_future], timeout=15)
        if not done:
            assert False, "prepass did not reach the second boundary: prefix path not exercised"
        second_pause_future.result()
        time.sleep(6)  # past the 5s deadline -> break timeout latches upstream
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        ## The resumed `buildLowCardinalityMask` runs pre-latched and pauses on the already-armed LC failpoint.
        wait_failpoint(LC_FAULT_NAME)

        ## Send a real client Cancel (Ctrl+C) on the query's own TCP connection. Unlike KILL QUERY, a TCP
        ## `Cancel` does not, by itself, set `cancel_reason = CANCELLED_BY_USER` on the server: before the fix
        ## it was misclassified as a soft timeout and the resumed `buildLowCardinalityMask` kept rescanning the
        ## committed prefix instead of aborting. The fix makes `TCPHandler::processCancel` propagate
        ## `CANCELLED_BY_USER`, so a client Cancel is honored as a hard cancellation and the query is aborted
        ## rather than silently downgraded to a soft timeout. The abort is reported with
        ## `QUERY_WAS_CANCELLED_BY_CLIENT` (735), which is the clean-cancel code: the server sends EOF instead of
        ## an exception packet, so the client sees no error output (the pre-propagation client contract).
        ## The client is the framework-managed `clickhouse-client` subprocess we spawned via `get_query_request`,
        ## so `req.process.pid` is its exact PID; SIGINT makes the client deliver a Cancel packet on the query's
        ## own connection. We resume the paused scan and let the server honor the Cancel: the observable contract
        ## is that the query is cancelled (the downstream transform must not keep running as a soft timeout).
        os.kill(req.process.pid, signal.SIGINT)

        node1.query(f"SYSTEM ENABLE FAILPOINT {LC_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {LC_FAULT_NAME}")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {LC_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "client-canceled query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]

    ## The server reports a client `Cancel` with `QUERY_WAS_CANCELLED_BY_CLIENT` (735), which is the clean-cancel
    ## code: no exception packet is sent, so the client exits with code 0 and empty stderr. This is exactly the
    ## pre-propagation client contract, so make sure the fix did not leak a raw `QUERY_WAS_CANCELLED` (394) to the
    ## client (which would print an error and exit with code 138).
    assert query_error[0] == "", f"client Cancel must be a clean cancel with no error output, got: {query_error[0]!r}"

    ## The cancel_reason the server records is the real discriminator: a TCP `Cancel` does not set it by
    ## itself, so without the fix it stays `UNDEFINED` and the query is silently downgraded to a soft timeout;
    ## the fix makes `TCPHandler::processCancel` propagate `CANCELLED_BY_USER`. `system.query_log` now exposes
    ## `cancel_reason`, so we assert it directly.
    node1.query("SYSTEM FLUSH LOGS")
    cancel_reason = node1.query(
        f"SELECT cancel_reason FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'ExceptionWhileProcessing' "
        f"FORMAT TSV"
    ).strip()
    assert cancel_reason == "CANCELLED_BY_USER", (
        f"expected client Cancel to set cancel_reason='CANCELLED_BY_USER', got: {cancel_reason!r}"
    )


def test_mixed_null_keys_break_prefix(started_cluster):
    """Break-mode soft timeout must preserve the committed source-row prefix across *multiple* nullable-key
    prepasses: when the timeout latches inside the first prepass, the remaining `LowCardinality(Nullable(...))`
    key columns must not re-zero the keep mask from row 0 (which previously reset `committed_source_rows` and
    dropped the already-committed prefix for multi-key `IN` set builds).

    Uses a two-column key (one plain `Nullable` and one `LowCardinality(Nullable)`) so the prepass runs the LC
    null-marking loop for the `LowCardinality` column while the plain `Nullable` column's null-map scan commits the
    prefix. The preserved prefix cannot be observed from the client (an interrupted break-mode query returns no
    rows), so the test asserts the downstream contract directly: after the prepass breaks, `buildFilter` (the
    downstream stage for this mixed key) must reach its first `distinct_transform_pause` at row 4096, proving the
    committed prefix survived downstream (the re-zero regression would shrink the chunk to 0 rows and pause nowhere).
    The test also asserts the upstream control flow (prepass pauses on the first boundary, never a third) and that
    break mode finishes with no error.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_mixed "
        "(k1 Nullable(UInt64), k2 LowCardinality(Nullable(String))) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k1 IS NULL, toUInt64(0), k1) AS UInt64) % 4) ORDER BY tuple() "
        "SETTINGS allow_suspicious_low_cardinality_types=1"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt_mixed")
    ## Spread rows across all four partitions (so the set build runs with >1 stream and the
    ## `DistinctTransform`/`skip_null_keys` prepass is actually engaged) and inject NULLs into the plain-nullable
    ## `k1` so the combined null map is non-zero: the plain-nullable null-marking prepass (DistinctTransform.cpp:541)
    ## runs first and commits the 8192-row prefix; the second `LowCardinality(Nullable)` key column's prepass must
    ## preserve the already-committed source-row boundary (it previously re-zeroed the keep mask from row 0). The
    ## downstream stage here is `buildFilter` (the mixed plain+`LowCardinality` key does not take the
    ## `buildLowCardinalityMask` fast path); null-filtering keeps its surviving row count below 8192.
    node1.query(
        "INSERT INTO null_keys_mt_mixed "
        "SELECT if(number % 7 = 0, NULL, number % 4), toNullable(toString(number % 1000)) FROM numbers(1000000)"
    )

    query = """SELECT count() FROM numbers(1000000)
 WHERE (number, number) IN (SELECT k1, k2 FROM null_keys_mt_mixed)
 FORMAT Null
 SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1,
          max_execution_time=5, timeout_overflow_mode='break'"""

    query_id = str(uuid.uuid4())
    ## Arm both failpoints up front: the prepass pauses on `null_pause`, and the pre-latched `buildFilter`
    ## (the downstream stage for this mixed plain+`LowCardinality` key, which runs after the prepass breaks)
    ## must already find `distinct_transform_pause` armed when it reaches its first 4096-row boundary.
    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
    node1.query(f"SYSTEM ENABLE FAILPOINT {HASHMAP_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        ## First 4096-row boundary: the null-marking prepass pauses after committing the first slice.
        wait_failpoint(NULL_FAULT_NAME)
        ## Re-arm and resume so the prepass commits a *second* 4096-row slice and pauses again at the
        ## second boundary. This is the multi-slice prefix the upstream commit must preserve wholesale,
        ## here across two `LowCardinality(Nullable)` key columns.
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=15)
        if not done:
            assert False, "prepass did not reach the second 4096-row boundary: multi-slice prefix path not exercised"
        second_pause_future.result()
        ## Hold past the deadline so the soft timeout latches while two slices are already committed, then
        ## release. With the fix the prepass stops at the paused row and every subsequent nullable-key
        ## column preserves the committed source-row boundary; the second column must not re-zero the mask
        ## from row 0 (which would drop the prefix).
        time.sleep(6)
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        ## With the fix the prepass breaks at the paused row and never pauses a third time.
        third_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([third_pause_future], timeout=10)
        if done:
            third_pause_future.result()
            assert False, "prepass reached a third boundary: soft-timeout latch is broken"

        ## Downstream observation: `buildFilter` (the downstream stage for this mixed key) must reach its first
        ## `distinct_transform_pause` at row 4096. The committed-source-row re-zero regression (round 18) makes
        ## the materialization truncate the chunk to 0 rows, so `buildFilter` would receive 0 rows and pause
        ## nowhere; a pause here proves the committed prefix survived into downstream processing. (The full
        ## 8192-row prefix contract is checked by `test_lc_null_keys_break_prefix`; here null-filtering keeps the
        ## surviving row count below 8192, so a single pause at 4096 is the observable contract.)
        wait_failpoint(HASHMAP_FAULT_NAME)
        node1.query(f"SYSTEM NOTIFY FAILPOINT {HASHMAP_FAULT_NAME}")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the soft timeout"
    if thread_error[0] is not None:
        raise thread_error[0]
    ## Break mode must not surface an error to the client.
    assert query_error[0] == "", f"break-mode soft timeout raised an error: {query_error[0]}"


def test_null_keys_break_prefix(started_cluster):
    """Break-mode soft timeout must preserve the whole committed prefix for a plain `Nullable` (non-LowCardinality)
    `IN (subquery)` set build. This is the path where `processed_prefix` is set from the null-marking prepass's
    committed prefix rather than from `buildLowCardinalityMask`.

    Mirror of `test_lc_null_keys_break_prefix` but on a plain `Nullable` key, so the `LowCardinality` fast path is
    skipped and the `processed_prefix` fix for the plain path is exercised.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt_plain (k Nullable(UInt64)) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple()"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt_plain")
    ## Partition 0 large enough to reach the second 4096-row sub-range (and therefore the failpoint); the
    ## `skip_null_keys` null-map scan (DistinctTransform.cpp:541, `!memoryIsZero` branch) is the stage that
    ## observes the timeout, so real NULLs are required for the `distinct_transform_null_pause` failpoint to
    ## fire. The break commits ~3 slices (>= 12288 source rows); the null-filtered keep count still exceeds 8192,
    ## so `buildFilter` reaches its second `distinct_transform_pause` at row 8192.
    node1.query("INSERT INTO null_keys_mt_plain SELECT if(number % 9 = 0, NULL, number % 100) FROM numbers(1000000)")
    node1.query("INSERT INTO null_keys_mt_plain SELECT toNullable(1) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_plain SELECT toNullable(2) FROM numbers(100)")
    node1.query("INSERT INTO null_keys_mt_plain SELECT toNullable(3) FROM numbers(100)")

    query = """SELECT count() FROM numbers(1000000)
 WHERE number IN (SELECT k FROM null_keys_mt_plain)
 FORMAT Null
 SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1,
          max_execution_time=5, timeout_overflow_mode='break'"""

    query_id = str(uuid.uuid4())
    ## Arm both failpoints up front: the prepass pauses on `null_pause`, and the pre-latched `buildFilter`
    ## (which runs after the prepass breaks) must already find `distinct_transform_pause` armed when it reaches
    ## its first 4096-row boundary.
    node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
    node1.query(f"SYSTEM ENABLE FAILPOINT {HASHMAP_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        ## First 4096-row boundary: the null-marking prepass pauses after committing the first slice.
        wait_failpoint(NULL_FAULT_NAME)
        ## Re-arm and resume so the prepass commits a second 4096-row slice (multi-slice committed prefix),
        ## then latch the real soft timeout and release; `processed_prefix` must preserve the whole prefix.
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=15)
        if not done:
            assert False, "prepass did not reach the second 4096-row boundary: multi-slice prefix path not exercised"
        second_pause_future.result()

        ## Hold past the deadline so the soft timeout latches while two slices are committed, then release.
        time.sleep(6)
        node1.query(f"SYSTEM ENABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {NULL_FAULT_NAME}")

        ## With the fix the prepass breaks at the paused row and never pauses a third time.
        third_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {NULL_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([third_pause_future], timeout=10)
        if done:
            third_pause_future.result()
            assert False, "prepass reached a third boundary: soft-timeout latch is broken"

        ## Downstream observation: `buildFilter` (the plain-nullable downstream stage) must reach its first
        ## `distinct_transform_pause` at row 4096. The `processed_prefix == 0` regression makes `buildFilter`
        ## return at row 0 (before the `i > 0` pause guard) so no pause occurs; a pause here proves the
        ## committed prefix survived into downstream processing. (The full 8192-row prefix contract is checked
        ## by `test_lc_null_keys_break_prefix`; here null-filtering keeps the surviving row count below 8192,
        ## so a single pause at 4096 is the observable contract.)
        wait_failpoint(HASHMAP_FAULT_NAME)
        node1.query(f"SYSTEM NOTIFY FAILPOINT {HASHMAP_FAULT_NAME}")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {NULL_FAULT_NAME}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {HASHMAP_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the soft timeout"
    if thread_error[0] is not None:
        raise thread_error[0]
    ## Break mode must not surface an error to the client.
    assert query_error[0] == "", f"break-mode soft timeout raised an error: {query_error[0]}"


def test_null_keys_filter_kill(started_cluster):
    """A `KILL QUERY` arriving during the monolithic `column->filter(keep, ...)` pass of the
    `skip_null_keys` prepass must be honored promptly, not after the whole chunk is materialized.

    The query uses a two-column `IN` key so the filter pass iterates two projected columns and pauses at
    the failpoint twice; with the fix the pass returns at the first column on `isCancelled()`, so the
    re-armed failpoint at the second column is never reached.
    """
    node1.query(
        "CREATE TABLE IF NOT EXISTS null_keys_mt (k Nullable(UInt64)) "
        "ENGINE = MergeTree PARTITION BY (CAST(if(k IS NULL, toUInt64(0), k) AS UInt64) % 4) ORDER BY tuple()"
    )
    node1.query("TRUNCATE TABLE IF EXISTS null_keys_mt")
    node1.query(
        "INSERT INTO null_keys_mt SELECT if(number % 9 = 0, NULL, number % 100) FROM numbers(1000000)"
    )

    query = """SELECT count() FROM numbers(1000000)
WHERE (number, number) IN (SELECT k, k FROM null_keys_mt)
FORMAT Null
SETTINGS transform_null_in=0, max_threads=4, force_creating_set_partitions_independently=1, max_rows_to_read=0"""

    query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {FILTER_FAULT_NAME}")

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

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def wait_failpoint(fault_name, timeout=60):
        wait_future = pool.submit(node1.query, f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")
        done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
        if not done:
            assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
        wait_future.result()

    try:
        wait_failpoint(FILTER_FAULT_NAME)

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

        ## Re-arm the one-shot failpoint, then resume the pass. With the fix the pass returns at the
        ## paused column on `isCancelled()`; without it the pass keeps filtering columns (it reaches the
        ## re-armed failpoint at the next column, which the second WAIT below observes).
        node1.query(f"SYSTEM ENABLE FAILPOINT {FILTER_FAULT_NAME}")
        node1.query(f"SYSTEM NOTIFY FAILPOINT {FILTER_FAULT_NAME}")

        second_pause_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {FILTER_FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
        if done:
            second_pause_future.result()
            assert False, "filter pass reached the re-armed failpoint: intra-pass cancellation is broken"
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {FILTER_FAULT_NAME}")
        pool.shutdown(wait=False, cancel_futures=True)

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "killed query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0
