"""
OpenTelemetry trace propagation from the initiator of a distributed plan query
(`make_distributed_plan = 1`) to the stateless workers executing its tasks, across real hosts.

The initiator (node1) samples a trace and dispatches the tasks to node1 and node2 over HTTP with
W3C `traceparent` headers. node2 must record the `InterserverIOHTTPHandler` request spans and the
`DistributedPlanTask::execute` task spans under the initiator's trace id, and the request spans'
parents must be the `StatelessWorkerClient::sendTask` spans recorded on node1: that is the hop
the header carries.
"""

import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/config.d/stateless_worker.xml", "configs/config.d/enable_span_log.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/config.d/stateless_worker.xml", "configs/config.d/enable_span_log.xml"])

NODES = [node1, node2]
INITIATOR = node1

# Two reader buckets and two shuffle buckets over the two-node worker cluster: tasks are assigned
# round-robin starting from the first worker, so every stage puts a task on node2.
DISTRIBUTED_SETTINGS = ", ".join(
    [
        "make_distributed_plan = 1",
        "enable_parallel_replicas = 0",
        "distributed_plan_default_shuffle_join_bucket_count = 2",
        "distributed_plan_default_reader_bucket_count = 2",
        "distributed_plan_max_rows_to_broadcast = 0",
        "opentelemetry_start_trace_probability = 1",
    ]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _span_rows(node, trace_id, select_list, condition):
    """Rows of `system.opentelemetry_span_log` of one trace on `node`, flushed first.
    Returns a list of tab-split rows."""
    node.query("SYSTEM FLUSH LOGS opentelemetry_span_log")
    result = node.query(
        f"""
        SELECT {select_list}
        FROM system.opentelemetry_span_log
        WHERE finish_date >= yesterday() AND trace_id = '{trace_id}' AND ({condition})
        FORMAT TSV
        """
    )
    return [line.split("\t") for line in result.splitlines()]


def _wait_until(predicate, timeout_seconds=60):
    """Span logs are flushed by background threads on every node; poll until `predicate`
    returns a truthy value and return it."""
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(1)
    raise AssertionError(f"condition not met within {timeout_seconds}s, last value: {last!r}")


def test_worker_task_spans_join_initiator_trace(started_cluster):
    for node in NODES:
        node.query("CREATE TABLE t_dp_otel (x UInt64) ENGINE = MergeTree ORDER BY tuple()")
        # Workers are replicas of one shard and must hold the same data.
        node.query("INSERT INTO t_dp_otel SELECT number % 10 FROM numbers(10000)")

    query_id = f"dp_otel_{uuid.uuid4()}"
    INITIATOR.query(
        f"SELECT x, count() FROM t_dp_otel GROUP BY x FORMAT Null SETTINGS {DISTRIBUTED_SETTINGS}",
        query_id=query_id,
    )

    # The sampled trace id is known only from the initiator's `query` span.
    def initiator_trace_id():
        INITIATOR.query("SYSTEM FLUSH LOGS opentelemetry_span_log")
        return INITIATOR.query(
            f"""
            SELECT trace_id FROM system.opentelemetry_span_log
            WHERE finish_date >= yesterday() AND operation_name = 'query'
              AND attribute['clickhouse.query_id'] = '{query_id}'
            LIMIT 1
            """
        ).strip()

    trace_id = _wait_until(initiator_trace_id)

    # One CLIENT dispatch span per task on the initiator, at least one of them aimed at node2.
    def dispatch_span_ids():
        rows = _span_rows(
            INITIATOR,
            trace_id,
            "span_id, attribute['clickhouse.target_host']",
            f"""operation_name = 'StatelessWorkerClient::sendTask' AND kind = 'CLIENT'
                AND attribute['clickhouse.initial_query_id'] = '{query_id}'
                AND attribute['clickhouse.distributed.task_id'] != ''""",
        )
        if not any(target_host.startswith("node2") for _, target_host in rows):
            return None
        return {span_id for span_id, _ in rows}

    _wait_until(dispatch_span_ids)

    # The worker side of the same trace on node2: the start requests' spans must be parented by the
    # initiator's dispatch spans, which proves the context crossed the HTTP hop. Cancel and forget
    # requests carry the context too, but run on the initiator's tracker pool, so only start
    # requests are compared. Both logs are flushed by background threads, so poll until the whole
    # start-request set is covered.
    def start_requests_under_dispatch_spans():
        rows = _span_rows(
            node2,
            trace_id,
            "parent_span_id",
            """operation_name = 'InterserverIOHTTPHandler' AND kind = 'SERVER'
               AND attribute['clickhouse.uri'] LIKE '%operation=start%'""",
        )
        request_parent_ids = {parent_span_id for (parent_span_id,) in rows}
        if not request_parent_ids:
            return None
        dispatch_ids = dispatch_span_ids() or set()
        if not request_parent_ids <= dispatch_ids:
            return None
        return len(request_parent_ids)

    assert _wait_until(start_requests_under_dispatch_spans) >= 1

    # The task span on node2 carries the task identity and the finished status.
    def finished_task_spans():
        rows = _span_rows(
            node2,
            trace_id,
            "count()",
            f"""operation_name = 'DistributedPlanTask::execute' AND kind = 'SERVER'
                AND attribute['clickhouse.initial_query_id'] = '{query_id}'
                AND attribute['clickhouse.distributed.task_id'] != ''
                AND attribute['clickhouse.distributed.execute_locally'] = '0'
                AND attribute['clickhouse.query_status'] = 'QueryFinish'""",
        )
        return int(rows[0][0]) if rows else 0

    assert _wait_until(finished_task_spans) >= 1

    for node in NODES:
        node.query("DROP TABLE t_dp_otel")
