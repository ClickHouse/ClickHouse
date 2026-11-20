"""
Test that worker task failures in distributed plan queries are propagated
to the initiator with the correct error message instead of QUERY_WAS_CANCELLED.

Reproduces https://github.com/ClickHouse/clickhouse-private/issues/40546
(error-propagation part).
"""

import logging
import time

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


# Each reader sleeps 50 s, so the query is still running when a node is killed.
SLOW_QUERY = (
    "SELECT count() FROM {table} WHERE sleepEachRow(0.001) = 0 "
    f"SETTINGS {DISTRIBUTED_SETTINGS}, max_block_size = 1000, function_sleep_max_microseconds_per_block = 0"
)


def create_table(table):
    for node in [node1, node2]:
        node.query(f"CREATE TABLE IF NOT EXISTS {table} (k UInt64) ENGINE = MergeTree() ORDER BY k")
        node.query(f"TRUNCATE TABLE {table}")
        node.query(f"INSERT INTO {table} SELECT number FROM numbers(100000)")


def drop_table(table):
    for node in [node1, node2]:
        node.query(f"DROP TABLE IF EXISTS {table}")


def wait_for_tasks(node, query_id):
    assert_eq_with_retry(
        node,
        f"SELECT count() > 0 FROM system.processes WHERE initial_query_id = '{query_id}' AND query_id != initial_query_id",
        "1",
    )


def get_value(node, query):
    return int(node.query(query).strip() or "0")


def test_worker_killed_fails_query(started_cluster):
    """A worker killed mid-query fails the query: its exchange streams break, and the initiator cannot connect its control
    connection to it again within `control_reattach_timeout_ms` (5 s here), whichever the query sees first."""
    table = "test_worker_killed"
    query_id = "test_worker_killed"
    create_table(table)

    request = node1.get_query_request(SLOW_QUERY.format(table=table), query_id=query_id, timeout=120)
    wait_for_tasks(node2, query_id)
    killed_at = time.monotonic()
    node2.stop_clickhouse(kill=True)
    try:
        error = request.get_error()
        failed_after = time.monotonic() - killed_at
    finally:
        node2.start_clickhouse()

    assert (
        "Lost the control connection to stateless worker node2" in error
        or "EXCHANGE_PEER_DISCONNECTED" in error
    ), error
    assert failed_after < 30, failed_after
    drop_table(table)


def test_initiator_killed_worker_reclaims_tasks(started_cluster):
    """A worker whose initiator is killed mid-query cancels and forgets the query's tasks and frees its slot, once no new
    control connection came within `control_reattach_timeout_ms` (5 s here)."""
    table = "test_initiator_killed"
    query_id = "test_initiator_killed"
    create_table(table)
    reclaimed_query = "SELECT value FROM system.events WHERE event = 'StatelessWorkerControlReclaimedTasks'"
    controllers_query = "SELECT value FROM system.metrics WHERE metric = 'StatelessWorkerQueryControllers'"
    reclaimed_before = get_value(node2, reclaimed_query)

    request = node1.get_query_request(SLOW_QUERY.format(table=table), query_id=query_id, timeout=120)
    wait_for_tasks(node2, query_id)
    assert get_value(node2, controllers_query) >= 1
    node1.stop_clickhouse(kill=True)
    try:
        request.get_answer_and_error()
        assert_eq_with_retry(node2, controllers_query, "0", retry_count=60)
        assert_eq_with_retry(
            node2,
            f"SELECT count() FROM system.processes WHERE initial_query_id = '{query_id}'",
            "0",
            retry_count=60,
        )
        assert get_value(node2, reclaimed_query) > reclaimed_before
    finally:
        node1.start_clickhouse()

    drop_table(table)


def test_wrong_interserver_credentials(started_cluster):
    """A worker refuses the control connection of an initiator with other interserver credentials."""
    config_path = "/etc/clickhouse-server/config.d/zz_interserver_credentials.xml"
    credentials = """<clickhouse>
    <interserver_http_credentials>
        <user>interserver</user>
        <password>{password}</password>
    </interserver_http_credentials>
</clickhouse>"""
    table = "test_wrong_credentials"
    create_table(table)
    try:
        for node, password in [(node1, "initiator"), (node2, "worker")]:
            node.replace_config(config_path, credentials.format(password=password))
            node.query("SYSTEM RELOAD CONFIG")

        error = node1.query_and_get_error(f"SELECT sum(k) FROM {table} SETTINGS {DISTRIBUTED_SETTINGS}")
        assert "AUTHENTICATION_FAILED" in error, error
        assert "Stateless worker node2:9223 refused the control connection" in error, error
    finally:
        for node in [node1, node2]:
            node.exec_in_container(["rm", "-f", config_path])
            node.query("SYSTEM RELOAD CONFIG")

    assert node1.query(f"SELECT sum(k) FROM {table} SETTINGS {DISTRIBUTED_SETTINGS}") == "4999950000\n"
    drop_table(table)
