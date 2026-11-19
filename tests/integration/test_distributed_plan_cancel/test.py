"""
Test that worker task failures in distributed plan queries are propagated
to the initiator with the correct error message instead of QUERY_WAS_CANCELLED.

Reproduces https://github.com/ClickHouse/clickhouse-private/issues/40546
(error-propagation part).
"""

import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/stateless_worker.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/stateless_worker.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


DISTRIBUTED_SETTINGS = (
    "make_distributed_plan = 1, "
    "enable_parallel_replicas = 0, "
    "distributed_plan_default_shuffle_join_bucket_count = 2, "
    "distributed_plan_default_reader_bucket_count = 2, "
    "distributed_plan_max_rows_to_broadcast = 0, "
    "distributed_plan_fallback_to_local_execution = 0"
)


def test_worker_error_not_masked(started_cluster):
    """A worker task error must surface to the initiator with the real error, not QUERY_WAS_CANCELLED.

    The initiator sets max_rows_to_read = 5, which is propagated to the workers; each worker reads
    more than 5 rows and fails with TOO_MANY_ROWS, and the initiator must report that. This also
    covers settings propagation: without it the workers would not enforce the limit and the query
    would silently succeed.
    """
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE IF NOT EXISTS test_err_mask (id UInt64) "
            "ENGINE = MergeTree() ORDER BY id"
        )
        node.query("INSERT INTO test_err_mask SELECT number FROM numbers(10000)")

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query(
            f"""
            SELECT sum(id)
            FROM test_err_mask
            SETTINGS {DISTRIBUTED_SETTINGS},
                     max_rows_to_read = 5
            """,
            timeout=30,
        )

    error_msg = str(exc_info.value)
    logging.info(f"Query failed with: {error_msg}")

    # The error should be the worker's limit failure, not QUERY_WAS_CANCELLED.
    assert "QUERY_WAS_CANCELLED" not in error_msg, (
        f"Initiator masked the worker error with QUERY_WAS_CANCELLED: {error_msg}"
    )
    assert (
        "TOO_MANY_ROWS" in error_msg
        or "max_rows_to_read" in error_msg
        or "Limit for rows" in error_msg
    ), f"Expected the propagated max_rows_to_read limit to be enforced on the worker, got: {error_msg}"

    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS test_err_mask")


def test_worker_refuses_queries_beyond_max_queries(started_cluster):
    """A worker runs the tasks of at most `stateless_worker_server.max_queries` queries (2 here): the control connection of
    a third query is refused as busy and that query fails, while the two running queries finish."""
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE IF NOT EXISTS test_max_queries (k UInt64) "
            "ENGINE = MergeTree() ORDER BY k"
        )
        node.query("INSERT INTO test_max_queries SELECT number FROM numbers(100000)")

    # Each query sleeps 10 s in all (5 s per reader); its tasks are on both workers.
    slow_query = f"""
        SELECT count() FROM test_max_queries WHERE sleepEachRow(0.0001) = 0
        SETTINGS {DISTRIBUTED_SETTINGS}, max_block_size = 1000, function_sleep_max_microseconds_per_block = 0
        """
    running = [
        node1.get_query_request(slow_query, query_id=f"test_max_queries_{i}", timeout=120)
        for i in range(2)
    ]
    for node in [node1, node2]:
        assert_eq_with_retry(
            node,
            "SELECT uniqExact(initial_query_id) FROM system.processes "
            "WHERE initial_query_id LIKE 'test_max_queries_%' AND query_id != initial_query_id",
            "2",
        )

    error = node1.query_and_get_error(
        f"SELECT count() FROM test_max_queries WHERE k % 2 = 0 SETTINGS {DISTRIBUTED_SETTINGS}"
    )
    assert "RECEIVED_ERROR_TOO_MANY_REQUESTS" in error, error
    assert "max_queries" in error, error

    for request in running:
        assert request.get_answer() == "100000\n"

    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS test_max_queries")
