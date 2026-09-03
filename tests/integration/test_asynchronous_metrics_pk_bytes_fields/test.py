import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/asynchronous_metrics_update_period_s.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def greater(a, b):
    return b > a


def lesser(a, b):
    return b < a


def query_until_condition(a, b, condition, retries=20, timeout=60, delay=0.5):
    """

    :param a: could be an input lambda that returns an int or just an int
    :param b: could be an input lambda that returns an int or just an int
    :param condition: lambda that returns a boolean after comparing a and b
    :param retries: number of times to retry until the condition is met
    :param timeout: time in seconds after which stop retrying
    :param delay: time in seconds between each retry
    :return: values of a and b (value post evaluation if lambda)
    """
    retries_done = 0
    start_time = time.time()
    while True:
        res1 = a() if callable(a) else a
        res2 = b() if callable(b) else b
        if condition(res1, res2):
            return res1, res2
        retries_done += 1
        if retries_done >= retries or (time.time() - start_time) > timeout:
            return res1, res2
        time.sleep(delay)


def test_total_pk_bytes_in_memory_fields(started_cluster):
    query_create = """CREATE TABLE test_pk_bytes
    (
       a UInt64,
       b UInt64
    )
    Engine=MergeTree()
    ORDER BY a SETTINGS index_granularity=1"""
    node.query(query_create)

    query_pk_bytes = "SELECT value FROM system.asynchronous_metrics WHERE metric = 'TotalPrimaryKeyBytesInMemory';"
    query_pk_bytes_allocated = """SELECT value FROM system.asynchronous_metrics 
                                  WHERE metric = 'TotalPrimaryKeyBytesInMemoryAllocated';"""

    # query for metrics before inserting anything into the table
    pk_bytes_before = int(node.query(query_pk_bytes).strip())
    pk_bytes_allocated_before = int(node.query(query_pk_bytes_allocated).strip())

    # insert data into the table and select
    node.query(
        """INSERT INTO test_pk_bytes SELECT number + 20, number * 20 from numbers(1000000)"""
    )

    node.query("""SELECT * FROM test_pk_bytes where a > 1000000""")

    # functions to query primary key bytes used and allocated in memory
    def res_pk_bytes():
        return int(node.query(query_pk_bytes).strip())

    def res_pk_bytes_allocated():
        return int(node.query(query_pk_bytes_allocated).strip())

    # query again after data insertion (make a reasonable amount of retries)
    # metrics should be greater after inserting data
    pk_bytes_before, pk_bytes_after = query_until_condition(
        pk_bytes_before, res_pk_bytes, condition=greater
    )
    assert pk_bytes_after > pk_bytes_before

    pk_bytes_allocated_before, pk_bytes_allocated_after = query_until_condition(
        pk_bytes_allocated_before, res_pk_bytes_allocated, condition=greater
    )
    assert pk_bytes_allocated_after > pk_bytes_allocated_before

    # insert some more data
    node.query(
        """INSERT INTO test_pk_bytes SELECT number + 100, number * 200 from numbers(1000000)"""
    )
    node.query("""SELECT * FROM test_pk_bytes""")

    # query again and compare the metrics.
    # metrics should be greater after inserting more data
    pk_bytes_after, pk_bytes_after_2 = query_until_condition(
        pk_bytes_after, res_pk_bytes, condition=greater
    )
    assert pk_bytes_after_2 > pk_bytes_after

    pk_bytes_allocated_after, pk_bytes_allocated_after_2 = query_until_condition(
        pk_bytes_allocated_after, res_pk_bytes_allocated, condition=greater
    )
    assert pk_bytes_allocated_after_2 > pk_bytes_allocated_after

    # drop all the data
    node.query("TRUNCATE table test_pk_bytes;")

    # query again and compare the metrics.
    # metrics should be lesser after dropping some data
    before_drop, after_drop = query_until_condition(
        pk_bytes_after_2, res_pk_bytes, condition=lesser
    )
    assert before_drop > after_drop

    before_drop, after_drop = query_until_condition(
        pk_bytes_allocated_after_2, res_pk_bytes_allocated, condition=lesser
    )
    assert before_drop > after_drop

    # finally drop the table
    node.query("DROP table test_pk_bytes;")
