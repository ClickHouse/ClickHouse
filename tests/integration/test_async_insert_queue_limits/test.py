import random
import string
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_failed_async_inserts(started_cluster):
    node.query(
        "CREATE TABLE queue_limit_async_insert (id UInt32, s String) ENGINE = MergeTree ORDER BY tuple()"
    )

    # each entry is 51 bytes, limit is set to 250 bytes
    def do_insert():
        node.query(
            f"INSERT INTO queue_limit_async_insert VALUES (42, '{''.join(random.choice(string.ascii_lowercase) for i in range(43))}')",
            settings={
                "async_insert": 1,
                "async_insert_busy_timeout_ms": 1000000000,
                "async_insert_busy_timeout_max_ms": 1000000000,
                "async_insert_busy_timeout_min_ms": 1000000000,
                "async_insert_cleanup_timeout_ms": 1000000000,
                "async_insert_deduplicate": 0,
                "async_insert_max_data_size": 1000000000000,
                "async_insert_max_query_number": 1000000000000,
                "wait_for_async_insert": 0,
            },
        )

    for _ in range(5):
        do_insert()

    assert (
        node.query(
            """
        SELECT SUM(ProfileEvents['AsyncInsertQueueFlushesOnLimit'])
          FROM system.query_log
    """
        )
        == "0\n"
    )

    do_insert()

    assert (
        node.query(
            """
        SELECT SUM(ProfileEvents['AsyncInsertQueueFlushesOnLimit'])
          FROM system.query_log
    """
        )
        == "1\n"
    )

    node.query("DROP TABLE IF EXISTS queue_limit_async_insert SYNC")
