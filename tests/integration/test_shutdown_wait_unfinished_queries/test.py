import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node_wait_queries = cluster.add_instance(
    "node_wait_queries",
    main_configs=["configs/config_wait.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)
node_kill_queries = cluster.add_instance(
    "node_kill_queries",
    main_configs=["configs/config_kill.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

global result


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def do_long_query(node, query_id):
    global result

    result = node.query_and_get_answer_with_error(
        "SELECT sleepEachRow(1) FROM system.numbers LIMIT 10",
        settings={"send_logs_level": "trace"},
        query_id=query_id,
    )


def test_shutdown_wait_unfinished_queries(start_cluster):
    global result

    query_id = uuid.uuid4().hex
    long_query = threading.Thread(
        target=do_long_query,
        args=(
            node_wait_queries,
            query_id,
        ),
    )
    long_query.start()

    assert_eq_with_retry(
        node_wait_queries,
        f"SELECT query_id FROM system.processes WHERE query_id = '{query_id}'",
        query_id,
    )
    node_wait_queries.stop_clickhouse(kill=False)

    long_query.join()

    assert result[0].count("0") == 10

    query_id = uuid.uuid4().hex
    long_query = threading.Thread(
        target=do_long_query,
        args=(
            node_kill_queries,
            query_id,
        ),
    )
    long_query.start()

    assert_eq_with_retry(
        node_kill_queries,
        f"SELECT query_id FROM system.processes WHERE query_id = '{query_id}'",
        query_id,
    )
    node_kill_queries.stop_clickhouse(kill=False)

    long_query.join()
    assert "QUERY_WAS_CANCELLED" in result[1]


def test_shutdown_kill_distributed_plan_query(start_cluster):
    """
    `killAllQueries` on shutdown must not hang on a query with a distributed query plan executed locally:
    each local task has its own process list entry, and cancelling the query joins the task threads,
    which destroy their entries while the initial query is being cancelled.
    """
    node = node_kill_queries
    if node.get_process_pid("clickhouse") is None:
        node.start_clickhouse()

    node.query("DROP TABLE IF EXISTS t_dp_shutdown SYNC")
    node.query(
        "CREATE TABLE t_dp_shutdown (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000"
    )
    node.query("INSERT INTO t_dp_shutdown SELECT number FROM numbers(100000)")

    query_id = uuid.uuid4().hex
    query_result = {}

    def run_query():
        # 1000 rows per block x 0.001 s = 1 s per block, about 100 s in total: the query is still running on shutdown.
        query_result["answer"] = node.query_and_get_answer_with_error(
            "SELECT x, count() FROM t_dp_shutdown WHERE NOT sleepEachRow(0.001) GROUP BY x FORMAT Null",
            settings={
                "make_distributed_plan": 1,
                "distributed_plan_execute_locally": 1,
                "enable_parallel_replicas": 0,
                "distributed_plan_default_shuffle_join_bucket_count": 3,
                "distributed_plan_default_reader_bucket_count": 3,
                "distributed_plan_max_rows_to_broadcast": 0,
                "max_block_size": 1000,
                "max_execution_time": 300,
            },
            query_id=query_id,
        )

    long_query = threading.Thread(target=run_query)
    long_query.start()

    # Shut down only once the local tasks, each a query of its own under the initial query id, are reading.
    assert_eq_with_retry(
        node,
        f"SELECT count() > 0 FROM system.processes WHERE initial_query_id = '{query_id}' AND query_id != '{query_id}' AND read_rows > 0",
        "1",
        retry_count=300,
        sleep_time=0.2,
    )

    # The server waits for the queries for `shutdown_wait_unfinished` = 30 seconds, and the helper waits as long
    # before it kills the server and sets `clickhouse_forced_stop`. A hang in `killAllQueries` ends up there.
    assert node.stop_clickhouse(kill=False)
    assert not node.clickhouse_forced_stop

    long_query.join()
    assert "QUERY_WAS_CANCELLED" in query_result["answer"][1]
