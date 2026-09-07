"""
`send_logs_level` for `make_distributed_plan=1` queries: log lines produced by
stateless-worker tasks must be forwarded to the initiator and reach the client.

https://github.com/ClickHouse/ClickHouse/issues/109452
"""

import re

import pytest

from helpers.cluster import ClickHouseCluster

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
        # Identical data on both nodes: worker tasks resolve the table in their own
        # catalog, so the reader buckets must partition the same content on any node.
        for node in [node1, node2]:
            node.query(
                "CREATE TABLE IF NOT EXISTS t_worker_logs (id UInt64) "
                "ENGINE = MergeTree ORDER BY id"
            )
            node.query(
                "INSERT INTO t_worker_logs SELECT number FROM numbers(1000000)"
            )
        yield cluster
    finally:
        cluster.shutdown()


DISTRIBUTED_SETTINGS = (
    "make_distributed_plan = 1, "
    "enable_parallel_replicas = 0, "
    "distributed_plan_default_shuffle_join_bucket_count = 2, "
    "distributed_plan_default_reader_bucket_count = 2, "
    "distributed_plan_max_rows_to_broadcast = 0"
)

# A dispatched worker task runs under current_query_id = '<initiator_uuid>::<stage_name>'
TASK_LOG_LINE = re.compile(r"\{[0-9a-f-]+::stage_[0-9_]+\}")
TASK_ERROR_LINE = re.compile(r"\{[0-9a-f-]+::stage_[0-9_]+\} <Error>")


def run_query_capturing_logs(query):
    """Logs requested with send_logs_level go to the client's stderr; capture both
    streams. `|| true` keeps a failing query (expected in the exception test) from
    failing the container exec itself."""
    return node1.exec_in_container(
        [
            "bash",
            "-c",
            f'clickhouse client --send_logs_level=trace --query "{query}" 2>&1 || true',
        ]
    )


def test_worker_logs_reach_client(started_cluster):
    out = run_query_capturing_logs(
        f"SELECT sum(id) FROM t_worker_logs SETTINGS {DISTRIBUTED_SETTINGS}"
    )
    assert "499999500000" in out, (
        "query did not return the expected result; test setup problem, "
        "not a log-forwarding failure: " + out[-2000:]
    )
    assert TASK_LOG_LINE.search(out), (
        "no log line from a dispatched worker task reached the client: " + out[-2000:]
    )


def test_worker_exception_context_reaches_client(started_cluster):
    out = run_query_capturing_logs(
        "SELECT sum(id + throwIf(id = 999999, 'boom on worker')) FROM t_worker_logs "
        f"SETTINGS {DISTRIBUTED_SETTINGS}"
    )
    assert "boom on worker" in out
    assert TASK_ERROR_LINE.search(out), (
        "worker exception context was not forwarded to the client: " + out[-2000:]
    )
