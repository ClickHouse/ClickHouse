import threading

import pytest

from helpers.cluster import ClickHouseCluster

# `max_async_insert_parsing_thread_pool_size` can be changed without a restart. Shrinking it to 0 while a
# flush has already scheduled its parsing slices on the pool must not hang the flush: the pool keeps at
# least one thread, so the scheduled slices are still run, and the batch is inserted.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/parsing_pool.xml"])

CONFIG_PATH = "/etc/clickhouse-server/config.d/parsing_pool.xml"
FAIL_POINT = "async_insert_parse_pause_before_next_entry"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def pool_size():
    return int(
        node.query(
            "SELECT value FROM system.server_settings WHERE name = 'max_async_insert_parsing_thread_pool_size'"
        )
    )


def test_shrink_pool_to_zero_during_flush(started_cluster):
    node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")
    assert pool_size() == 4

    # The settings are part of the key of the asynchronous insert queue, so all the inserts are collected
    # into one batch, which is flushed explicitly. With four parse threads the six entries are split into
    # four slices, three of which are scheduled on the pool.
    params = {
        "async_insert": 1,
        "wait_for_async_insert": 0,
        "async_insert_use_adaptive_busy_timeout": 0,
        "async_insert_busy_timeout_ms": 600000,
        "async_insert_max_data_size": 1000000000,
        "async_insert_max_query_number": 1000000,
        "async_insert_parse_threads": 4,
    }
    for i in range(6):
        node.http_query(
            "INSERT INTO t FORMAT JSONEachRow", data='{"x": %d}' % i, params=params
        )

    # A slice pauses at the fail point before every entry but its first one, so the flush parks after
    # every slice has parsed its first entry, with all the slices scheduled on the pool.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAIL_POINT}")
    flush = threading.Thread(
        target=lambda: node.query("SYSTEM FLUSH ASYNC INSERT QUEUE t")
    )
    flush.start()
    node.query(f"SYSTEM WAIT FAILPOINT {FAIL_POINT} PAUSE")

    # Shrink the pool to nothing while the flush is in progress. The server keeps one thread.
    node.replace_in_config(
        CONFIG_PATH,
        "<max_async_insert_parsing_thread_pool_size>4<",
        "<max_async_insert_parsing_thread_pool_size>0<",
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert pool_size() == 1

    node.query(f"SYSTEM DISABLE FAILPOINT {FAIL_POINT}")
    flush.join(timeout=60)
    assert not flush.is_alive(), "the flush hung after the parsing pool was shrunk"

    assert node.query("SELECT count(), sum(x) FROM t") == "6\t15\n"

    # The next flush is parsed with the pool at its minimum size, and still uses every slice.
    for i in range(6):
        node.http_query(
            "INSERT INTO t FORMAT JSONEachRow", data='{"x": %d}' % i, params=params
        )
    node.query("SYSTEM FLUSH ASYNC INSERT QUEUE t")
    assert node.query("SELECT count(), sum(x) FROM t") == "12\t30\n"

    node.query("SYSTEM FLUSH LOGS asynchronous_insert_log")
    assert (
        node.query(
            "SELECT count(), countDistinct(flush_query_id) FROM system.asynchronous_insert_log WHERE table = 't' AND status = 'Ok'"
        )
        == "12\t2\n"
    )
