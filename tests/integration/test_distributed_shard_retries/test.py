import concurrent.futures
from threading import Event, Thread
import uuid

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

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
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml", "configs/concurrency_limits.xml"],
)


def prepare_cluster():
    for idx, node in enumerate([node1, node2, node3, node4]):
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

    query_ids = []

    def _distributed_query():
        query_id = str(uuid.uuid4())
        query_ids.append(query_id)
        return node1.query(
            "SELECT max(i), sleep(3) FROM distributed_table", query_id=query_id
        )

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

    assert len(results) > 0

    result_value = int(results[0].split("\t")[0])
    assert result_value >= 2, f"Expected max(i) >= 2, got {result_value}"
    assert result_value <= 4, f"Expected max(i) <= 4, got {result_value}"

    node1.query("SYSTEM FLUSH LOGS")

    # Check that retries happened for at least one of the distributed queries
    query_ids_str = "','".join(query_ids)
    retry_count = int(
        node1.query(
            f"""
            SELECT max(ProfileEvents['DistributedTryCount']) 
            FROM system.query_log 
            WHERE query_id IN ('{query_ids_str}')
              AND type = 'QueryFinish'
            """
        ).strip()
        or "0"
    )

    assert (
        retry_count >= 1
    ), f"Expected DistributedTryCount >= 1 (at least one retry happened), got {retry_count}"


def test_leaf_queries_not_retried_after_receiving_data(started_cluster):
    """
    Test that queries are NOT retried if we've already received data from a replica.
    Retrying after receiving data could lead to incorrect results (duplicate data).
    """
    prepare_cluster()

    # Enable failpoint that will throw an exception after receiving data
    node1.query(
        "SYSTEM ENABLE FAILPOINT remote_query_executor_exception_after_receiving_data"
    )

    query_id = str(uuid.uuid4())

    with pytest.raises(
        QueryRuntimeException,
        match="Injected TOO_MANY_SIMULTANEOUS_QUERIES error after receiving data",
    ):
        node1.query("SELECT max(i) FROM distributed_table", query_id=query_id)

    node1.query(
        "SYSTEM DISABLE FAILPOINT remote_query_executor_exception_after_receiving_data"
    )

    # Check that no retries happened (DistributedTryCount should be 0)
    node1.query("SYSTEM FLUSH LOGS")
    retry_count = int(
        node1.query(
            f"SELECT coalesce(max(ProfileEvents['DistributedTryCount']), 0) FROM system.query_log WHERE query_id = '{query_id}'"
        )
    )

    assert (
        retry_count == 0
    ), f"Expected no retries (DistributedTryCount = 0) after receiving data, but got {retry_count}"
