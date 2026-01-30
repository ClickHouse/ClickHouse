import uuid
import pytest
from helpers.cluster import ClickHouseCluster, QueryRuntimeException

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
            CREATE TABLE local_table(i Int64) ENGINE=MergeTree ORDER BY i
            """
        )
        node.query(
            """CREATE TABLE distributed_table(i Int64) ENGINE = Distributed(test_cluster, default, local_table, Rand());"""
        )
        node.query("INSERT INTO local_table SELECT number FROM numbers(1000)")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_leaf_queries_retried(started_cluster):
    """
    Enable failpoints that mean leaf nodes return concurrency errors,
    then validate that the query still succeeds and we see retries.
    """
    prepare_cluster()

    node1.query("SYSTEM ENABLE FAILPOINT remote_query_executor_exception_retryable")
    node3.query("SYSTEM ENABLE FAILPOINT remote_query_executor_exception_retryable")

    query_id = str(uuid.uuid4())
    try:
        result = node1.query(
            "SELECT max(i) FROM distributed_table SETTINGS load_balancing='in_order'",
            query_id=query_id,
        )
    finally:
        node1.query(
            "SYSTEM DISABLE FAILPOINT remote_query_executor_exception_retryable"
        )
        node3.query(
            "SYSTEM DISABLE FAILPOINT remote_query_executor_exception_retryable"
        )

    result_value = int(result.split("\t")[0])
    assert result_value == 999, f"Expected max(i) == 999, got {result_value}"

    node1.query("SYSTEM FLUSH LOGS")
    retry_count = int(
        node1.query(
            f"SELECT coalesce(max(ProfileEvents['DistributedTryCount']), 0) FROM system.query_log WHERE query_id = '{query_id}'"
        )
    )

    assert retry_count >= 1, f"Expected at least one retry, got {retry_count}"


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
