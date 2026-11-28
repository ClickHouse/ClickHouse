import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Hold the data in the queue: it stays below the size limit and the busy timeout does not
# elapse, so only the flush under test empties the queue.
KEEP_IN_QUEUE = """
    async_insert = 1,
    wait_for_async_insert = 0,
    async_insert_use_adaptive_busy_timeout = 0,
    async_insert_busy_timeout_min_ms = 600000,
    async_insert_busy_timeout_max_ms = 600000,
    async_insert_max_data_size = 1000000000
"""

# 'test_flush_by_data_size' below needs an inlined payload that is far enough from both
# ends of its size limit for the exact framing of the inlined data not to matter.
ROWS_PER_PUSH = 100


def payload(first_value):
    """Values of a fixed width, so that every push inlines the same number of bytes."""
    return ",".join(f"({first_value + i})" for i in range(ROWS_PER_PUSH))


# Half a payload of headroom either way: one payload stays under the limit, two go over it.
FLUSH_AT_BYTES = len(payload(1000)) * 3 // 2


def queue_metrics():
    """The pending queue as 'system.metrics' reports it: (entries, bytes).

    The server belongs to this test alone, so both values are exactly what this test
    put into the queue. That is why every assertion below can be exact.
    """
    size, size_in_bytes = (
        node.query(
            """
            SELECT
                (SELECT value FROM system.metrics WHERE metric = 'AsynchronousInsertQueueSize'),
                (SELECT value FROM system.metrics WHERE metric = 'AsynchronousInsertQueueBytes')
            """
        )
        .strip()
        .split("\t")
    )
    return int(size), int(size_in_bytes)


def queue_contents():
    """The pending queue as 'system.asynchronous_inserts' reports it: (entries, bytes)."""
    count, total_bytes = (
        node.query(
            "SELECT count(), sum(total_bytes) FROM system.asynchronous_inserts"
        )
        .strip()
        .split("\t")
    )
    return int(count), int(total_bytes)


def fill_queue(table):
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a")

    assert queue_metrics() == (0, 0), "the queue must be empty before the test"

    for i in range(3):
        node.query(f"INSERT INTO {table} SETTINGS {KEEP_IN_QUEUE} VALUES ({i})")

    entries, size_in_bytes = queue_metrics()
    assert entries == 1, "the three queries share one queue entry"
    assert size_in_bytes > 0
    # The metrics and the queue itself must agree while the data is pending.
    assert (entries, size_in_bytes) == queue_contents()

    return size_in_bytes


def assert_queue_is_empty(table, rows):
    assert queue_metrics() == (0, 0)
    assert queue_contents() == (0, 0)
    assert node.query(f"SELECT count() FROM {table}").strip() == str(rows)
    node.query(f"DROP TABLE {table}")


def test_flush_by_system_query():
    """'SYSTEM FLUSH ASYNC INSERT QUEUE <table>' used to leave the entries counted."""
    fill_queue("flush_by_system_query")
    node.query("SYSTEM FLUSH ASYNC INSERT QUEUE flush_by_system_query")
    assert_queue_is_empty("flush_by_system_query", 3)


def test_flush_all_by_system_query():
    """The same command without a table list takes another path, 'flushAll'."""
    fill_queue("flush_all_by_system_query")
    node.query("SYSTEM FLUSH ASYNC INSERT QUEUE")
    assert_queue_is_empty("flush_all_by_system_query", 3)


def test_flush_by_busy_timeout():
    """The deadline thread discounted the entries itself, so it must not do it twice."""
    table = "flush_by_busy_timeout"
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a")

    assert queue_metrics() == (0, 0), "the queue must be empty before the test"

    # 'wait_for_async_insert' returns once the flush is done, so nothing here sleeps.
    node.query(
        f"""INSERT INTO {table} SETTINGS async_insert = 1, wait_for_async_insert = 1,
            async_insert_use_adaptive_busy_timeout = 0,
            async_insert_busy_timeout_min_ms = 50,
            async_insert_busy_timeout_max_ms = 50 VALUES (1)"""
    )

    assert_queue_is_empty(table, 1)


def test_flush_by_data_size():
    """A push that takes the queue over the size limit flushes it at once.

    'async_insert_max_data_size' is also the cap of the 'LimitReadBuffer' that reads the
    inlined data, so a single push can never cross the threshold: a query whose data does
    not fit under the cap returns 'TOO_MUCH_DATA' and is executed synchronously, without
    ever reaching the queue. Two pushes are needed - the first stays under the limit, the
    second takes their shared entry over it.
    """
    table = "flush_by_data_size"
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a")

    assert queue_metrics() == (0, 0), "the queue must be empty before the test"

    # Both pushes have to agree on every setting, because the settings are part of the
    # queue key: pushes that disagree land in different entries and never accumulate.
    # That is also why neither of them can wait for its own flush - the first one would
    # block until the busy timeout above elapses.
    def push(first_value):
        node.query(
            f"""INSERT INTO {table} SETTINGS async_insert = 1, wait_for_async_insert = 0,
                async_insert_use_adaptive_busy_timeout = 0,
                async_insert_busy_timeout_min_ms = 600000,
                async_insert_busy_timeout_max_ms = 600000,
                async_insert_max_data_size = {FLUSH_AT_BYTES}
                VALUES {payload(first_value)}"""
        )

    push(1000)

    # Without this the test would pass just as well if the push never reached the queue,
    # which is what a limit below the size of the inlined data makes it do.
    entries, size_in_bytes = queue_metrics()
    assert entries == 1, "the first push must be held in the queue"
    assert 0 < size_in_bytes < FLUSH_AT_BYTES
    assert (entries, size_in_bytes) == queue_contents()

    push(2000)

    # 'scheduleDataProcessingJob' discounts the metrics, and the push removes the entry
    # from the queue, both before the data is handed to the pool - so both are already
    # back to zero here. Only the rows themselves follow asynchronously.
    assert queue_metrics() == (0, 0)
    assert queue_contents() == (0, 0)
    assert_eq_with_retry(node, f"SELECT count() FROM {table}", str(2 * ROWS_PER_PUSH))

    node.query(f"DROP TABLE {table}")
