from concurrent.futures import ThreadPoolExecutor, TimeoutError
from uuid import uuid4

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/async_insert.xml"])

FAILPOINT = "async_insert_flush_pause_in_executor"
ADMISSION_FAILPOINT = "async_insert_pause_before_schedule"
SETTINGS = {
    "async_insert": 1,
    "wait_for_async_insert": 0,
    "async_insert_max_data_size": 2,
    "async_insert_use_adaptive_busy_timeout": 0,
    "async_insert_busy_timeout_min_ms": 3600000,
    "async_insert_busy_timeout_max_ms": 3600000,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def metric(name):
    return int(node.query(f"SELECT value FROM system.metrics WHERE metric = '{name}'"))


@pytest.fixture
def tables():
    for table in ("saturated", "unrelated"):
        node.query(f"CREATE TABLE {table} (x UInt64) ENGINE = MergeTree ORDER BY x")
    try:
        yield
    finally:
        node.query("SYSTEM FLUSH ASYNC INSERT QUEUE", timeout=30)
        for table in ("saturated", "unrelated"):
            node.query(f"DROP TABLE {table}")


@pytest.mark.parametrize("wait_for_async_insert", [0, 1])
@pytest.mark.parametrize(
    "flush_query",
    ["SYSTEM FLUSH ASYNC INSERT QUEUE", "SYSTEM FLUSH ASYNC INSERT QUEUE unrelated"],
)
def test_saturated_pool_does_not_lock_queue_shard(
    tables, wait_for_async_insert, flush_query
):
    # One worker also means one queue shard: both tables necessarily share its mutex.
    waiter_id = f"pool_waiter_{uuid4().hex}"
    flush_id = f"explicit_flush_{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

            # Exactly two bytes reach the size threshold without the oversized-input
            # fallback. HTTP keeps parsing in the server, where the failpoint runs.
            node.http_query(
                "INSERT INTO saturated FORMAT TSV",
                data="1\n",
                params=SETTINGS,
                timeout=30,
            )
            node.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=30)
            assert metric("AsynchronousInsertThreadsScheduled") == 1

            waiter = executor.submit(
                node.http_query,
                "INSERT INTO saturated FORMAT TSV",
                data="2\n",
                params={
                    **SETTINGS,
                    "wait_for_async_insert": wait_for_async_insert,
                    "query_id": waiter_id,
                },
                timeout=120,
            )
            # This is emitted with the shard lock held, just before the blocking
            # submission. Waiting for it prevents the unrelated insert racing ahead.
            node.wait_for_log_line(
                rf"\{{{waiter_id}\}}.*Scheduling async insert processing job because enough bytes accumulated",
                timeout=30,
            )

            # This insert only buffers. On the original code it times out waiting
            # for the mutex held by the producer trying to submit the second flush.
            assert (
                node.http_query(
                    "INSERT INTO unrelated FORMAT TSV",
                    data="3\n",
                    params={**SETTINGS, "async_insert_max_data_size": 1024},
                    timeout=30,
                )
                == ""
            )
            assert not waiter.done()
            assert metric("AsynchronousInsertThreadsScheduled") == 1
            assert metric("PendingAsyncInsert") == 3

            flush = executor.submit(
                node.query, flush_query, query_id=flush_id, timeout=120
            )
            node.wait_for_log_line(
                rf"\{{{flush_id}\}}.*Requested to flush asynchronous insert queue",
                timeout=30,
            )
            assert not flush.done()

            node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
            assert waiter.result(timeout=30) == ""
            assert flush.result(timeout=30) == ""
            node.query("SYSTEM FLUSH ASYNC INSERT QUEUE")

            assert node.query("SELECT x FROM saturated ORDER BY x") == "1\n2\n"
            assert node.query("SELECT x FROM unrelated") == "3\n"
            assert metric("PendingAsyncInsert") == 0
            assert metric("AsynchronousInsertThreadsScheduled") == 0
        finally:
            # Release the worker even when the regression times out, before joining
            # HTTP requests that need it to make progress.
            node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")


@pytest.mark.parametrize("trigger", ["size", "deadline"])
@pytest.mark.parametrize("bad_data", [False, True])
def test_forced_flush_waits_for_batch_before_pool_admission(tables, trigger, bad_data):
    settings = {**SETTINGS, "async_insert_max_data_size": 4}
    if trigger == "deadline":
        settings["async_insert_busy_timeout_min_ms"] = 1000
        settings["async_insert_busy_timeout_max_ms"] = 1000

    flush_id = f"drain_handoff_{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            node.query(f"SYSTEM ENABLE FAILPOINT {ADMISSION_FAILPOINT}")

            # The first request is acknowledged while still buffered. A subsequent
            # producer or the deadline worker removes its batch from the shard queue.
            node.http_query(
                "INSERT INTO saturated FORMAT TSV",
                data="x\n" if bad_data else "1\n",
                params=settings,
                timeout=30,
            )
            producer = None
            if trigger == "size":
                producer = executor.submit(
                    node.http_query,
                    "INSERT INTO saturated FORMAT TSV",
                    data="x\n" if bad_data else "2\n",
                    params=settings,
                    timeout=120,
                )
            node.query(f"SYSTEM WAIT FAILPOINT {ADMISSION_FAILPOINT} PAUSE", timeout=30)
            assert metric("AsynchronousInsertThreadsScheduled") == 0

            flush = executor.submit(
                node.query,
                "SYSTEM FLUSH ASYNC INSERT QUEUE",
                query_id=flush_id,
                timeout=120,
            )
            node.wait_for_log_line(
                rf"\{{{flush_id}\}}.*Will wait for finishing of 0 flushing jobs",
                timeout=30,
            )
            # An empty pool must not let the flush overlook the acknowledged row
            # in the batch paused just before admission.
            with pytest.raises(TimeoutError):
                flush.result(timeout=1)

            # Waiting for that batch must also leave the shard mutex available.
            node.http_query(
                "INSERT INTO unrelated FORMAT TSV",
                data="3\n",
                params={**SETTINGS, "async_insert_max_data_size": 1024},
                timeout=30,
            )

            node.query(f"SYSTEM DISABLE FAILPOINT {ADMISSION_FAILPOINT}")
            if producer is not None:
                assert producer.result(timeout=30) == ""
            assert flush.result(timeout=30) == ""
            expected = "" if bad_data else "1\n2\n" if trigger == "size" else "1\n"
            assert node.query("SELECT x FROM saturated ORDER BY x") == expected
            node.query("SYSTEM FLUSH ASYNC INSERT QUEUE")
            assert node.query("SELECT x FROM unrelated") == "3\n"
            assert metric("PendingAsyncInsert") == 0
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {ADMISSION_FAILPOINT}")
