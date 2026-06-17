import concurrent.futures

import pytest

from helpers.cluster import ClickHouseCluster

MAX_THREADS = 60

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    macros={"cluster": "test-cluster", "replica": "node1"},
    main_configs=["configs/settings.xml"],
    with_zookeeper=True,
)


def prepare_cluster():
    node1.query("DROP TABLE IF EXISTS test_threads_busy SYNC")
    node1.query(
        """
        CREATE TABLE test_threads_busy(d Date, i Int64,  s String) ENGINE=MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY d
        """
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def do_slow_select():
    # Do a bunch of slow queries that use a large number of threads to saturate max_thread_pool_size
    # explicitly set max_threads as otherwise it's relative to the number of CPU cores
    query = (
        "SELECT d, i, s, sleepEachRow(3) from test_threads_busy SETTINGS max_threads=40"
    )
    node1.query(query)


def test_query_exception_on_thread_pool_full(started_cluster):
    prepare_cluster()
    # Generate some sample data so sleepEachRow in do_slow_select works
    node1.query(
        f"INSERT INTO test_threads_busy VALUES ('2024-01-01', 1, 'thread-test')"
    )

    futures = []
    errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for _ in range(MAX_THREADS):
            futures.append(executor.submit(do_slow_select))

        for f in futures:
            try:
                f.result()
            except Exception as err:
                errors.append(str(err))
    assert len(errors) > 0, "Should be 'Cannot schedule a task' exceptions"
    assert all(
        "Cannot schedule a task" in err for err in errors
    ), "Query threads are stuck, or returned an unexpected error"
