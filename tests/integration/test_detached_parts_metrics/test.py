import time
import pytest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_until(call_back, time_to_sleep=0.5, timeout=60):
    assert callable(call_back)
    start_time = time.time()
    deadline = time.time() + timeout
    while not call_back() and time.time() < deadline:
        time.sleep(time_to_sleep)
    assert call_back(), "Elapsed {}".format(time.time() - start_time)


def is_different(a, b):
    def wrap():
        res_a = a() if callable(a) else a
        res_b = b() if callable(b) else b
        return res_a != res_b

    return wrap


def test_event_time_microseconds_field(started_cluster):
    cluster.start()
    query_create = """
    CREATE TABLE t
    (
       id Int64,
       event_time Date
    )
    Engine=MergeTree()
    PARTITION BY toYYYYMMDD(event_time)
    ORDER BY id;
    """
    node1.query(query_create)

    # gives us 2 partitions with 3 parts in total
    node1.query("INSERT INTO t VALUES (1, toDate('2022-09-01'));")
    node1.query("INSERT INTO t VALUES (2, toDate('2022-08-29'));")
    node1.query("INSERT INTO t VALUES (3, toDate('2022-09-01'));")

    query_number_detached_parts_in_async_metric = """
    SELECT value
    FROM system.asynchronous_metrics
    WHERE metric LIKE 'NumberOfDetachedParts';
    """
    query_number_detached_by_user_parts_in_async_metric = """
    SELECT value
    FROM system.asynchronous_metrics
    WHERE metric LIKE 'NumberOfDetachedPartsByUser';
    """
    query_count_active_parts = """
    SELECT count(*) FROM system.parts WHERE table = 't' AND active
    """
    query_count_detached_parts = """
    SELECT count(*) FROM system.detached_parts WHERE table = 't'
    """

    query_one_partition_name = """
    SELECT name FROM system.parts WHERE table = 't' AND active AND partition = '20220829'
    """
    partition_name = node1.query(query_one_partition_name).strip()

    assert 0 == int(node1.query(query_count_detached_parts))
    assert 3 == int(node1.query(query_count_active_parts))
    assert 0 == int(node1.query(query_number_detached_parts_in_async_metric))
    assert 0 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # detach some parts and wait until asynchronous metrics notice it
    node1.query("ALTER TABLE t DETACH PARTITION '20220901';")

    assert 2 == int(node1.query(query_count_detached_parts))
    assert 1 == int(node1.query(query_count_active_parts))

    wait_until(
        is_different(
            0, lambda: int(node1.query(query_number_detached_parts_in_async_metric))
        )
    )
    assert 2 == int(node1.query(query_number_detached_parts_in_async_metric))
    assert 2 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # detach the rest parts and wait until asynchronous metrics notice it
    node1.query("ALTER TABLE t DETACH PARTITION ALL")

    assert 3 == int(node1.query(query_count_detached_parts))
    assert 0 == int(node1.query(query_count_active_parts))

    wait_until(
        is_different(
            2, lambda: int(node1.query(query_number_detached_parts_in_async_metric))
        )
    )
    assert 3 == int(node1.query(query_number_detached_parts_in_async_metric))
    assert 3 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # inject some data directly and wait until asynchronous metrics notice it
    node1.exec_in_container(
        [
            "bash",
            "-c",
            "mkdir /var/lib/clickhouse/data/default/t/detached/unexpected_all_0_0_0",
        ],
        privileged=True,
    )

    assert 4 == int(node1.query(query_count_detached_parts))
    assert 0 == int(node1.query(query_count_active_parts))

    wait_until(
        is_different(
            3, lambda: int(node1.query(query_number_detached_parts_in_async_metric))
        )
    )
    assert 4 == int(node1.query(query_number_detached_parts_in_async_metric))
    assert 3 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))

    # drop some data directly and wait asynchronous metrics notice it
    node1.exec_in_container(
        [
            "bash",
            "-c",
            "rm -rf /var/lib/clickhouse/data/default/t/detached/{}".format(
                partition_name
            ),
        ],
        privileged=True,
    )

    assert 3 == int(node1.query(query_count_detached_parts))
    assert 0 == int(node1.query(query_count_active_parts))

    wait_until(
        is_different(
            4, lambda: int(node1.query(query_number_detached_parts_in_async_metric))
        )
    )
    assert 3 == int(node1.query(query_number_detached_parts_in_async_metric))
    assert 2 == int(node1.query(query_number_detached_by_user_parts_in_async_metric))
