import concurrent.futures
from threading import Event, Thread

import pytest

from helpers.cluster import ClickHouseCluster

MAX_THREADS = 10

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/shard_retry_config.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/concurrency_limits.xml"],
)


def prepare_cluster():
    for idx, node in enumerate([node1, node2]):
        node.query("DROP TABLE IF EXISTS local_table")
        node.query("DROP TABLE IF EXISTS distributed_table")
        node.query(
            """
            CREATE TABLE local_table(i Int64, d DateTime) ENGINE=MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY d
            """
        )
        node.query(
            """CREATE TABLE distributed_table(i Int64) ENGINE = Distributed(test_cluster, default, local_table, Rand());"""
        )
        # Generate some sample data so sleepEachRow in do_slow_select works
        node.query("INSERT INTO local_table VALUES ({},)".format(idx + 1))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_leaf_queries_retried(started_cluster):

    prepare_cluster()
    stop_event = Event()

    def _do_slow_select():
        while not stop_event.isSet():
            node1.query("SELECT i, sleepEachRow(3) from local_table")

    def _distributed_query():
        return node1.query("SELECT max(i), sleep(3) FROM distributed_table")

    slow_thread = Thread(target=_do_slow_select)
    slow_thread.start()

    futures = []
    errors = []
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for _ in range(MAX_THREADS):
            futures.append(executor.submit(_distributed_query))

        for f in futures:
            try:
                results.append(f.result())
            except Exception as err:
                errors.append(str(err))

    stop_event.set()
    slow_thread.join()

    concurrency_errors = sum(
        1 for err in errors if "TOO_MANY_SIMULTANEOUS_QUERIES" in err
    )
    other_errors = [err for err in errors if "TOO_MANY_SIMULTANEOUS_QUERIES" not in err]

    assert len(other_errors) == 0, f"Got unexpected query errors: {other_errors}"
    assert (
        concurrency_errors == 0
    ), f"Should be no 'TOO_MANY_SIMULTANEOUS_QUERIES'. Got {errors}"

    assert node1.contains_in_log(
        "StorageDistributed (distributed_table): try 1 of 6 failed due to error code 202"
    )
    assert len(results) > 0

    assert results[0] == "2\t0\n"

    node1.query("SYSTEM FLUSH LOGS")
    assert (
        int(
            node1.query(
                "SELECT max(ProfileEvents['DistributedTryCount']) FROM system.query_log"
            )
        )
        > 1
    )
