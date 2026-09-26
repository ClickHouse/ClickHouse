import pytest

from helpers.cluster import ClickHouseCluster

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
    """A push that reaches the size limit flushes at once, without entering the queue."""
    table = "flush_by_data_size"
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a")

    assert queue_metrics() == (0, 0), "the queue must be empty before the test"

    node.query(
        f"""INSERT INTO {table} SETTINGS async_insert = 1, wait_for_async_insert = 1,
            async_insert_use_adaptive_busy_timeout = 0,
            async_insert_busy_timeout_min_ms = 600000,
            async_insert_busy_timeout_max_ms = 600000,
            async_insert_max_data_size = 1 VALUES (1)"""
    )

    assert_queue_is_empty(table, 1)
