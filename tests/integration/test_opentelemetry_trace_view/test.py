"""traceView with the `cluster` argument merges the span logs of different nodes.

In a cluster every node writes the spans of its part of a distributed query to its own
`system.opentelemetry_span_log`. The stateless tests run every replica of their test clusters
on one node, so they cannot tell a `clusterAllReplicas` read of every log from a read of the
local log alone. Here the second shard is a real second node.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml", "configs/enable_span_log.xml"], with_zookeeper=False)
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml", "configs/enable_span_log.xml"], with_zookeeper=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cluster_argument_reads_the_span_log_of_every_node(started_cluster):
    query_id = f"trace_view_{uuid.uuid4().hex}"

    # A distributed query started on node1 with tracing: node2 executes the second shard and
    # writes its spans, with the same trace id, to its own span log.
    node1.query(
        "SELECT * FROM cluster('two_nodes', system, one) FORMAT Null",
        query_id=query_id,
        settings={"opentelemetry_start_trace_probability": 1, "prefer_localhost_replica": 0},
    )
    for node in (node1, node2):
        node.query("SYSTEM FLUSH LOGS opentelemetry_span_log")

    trace_id = node1.query(
        f"SELECT trace_id FROM system.opentelemetry_span_log"
        f" WHERE operation_name = 'query' AND attribute['clickhouse.query_id'] = '{query_id}' LIMIT 1"
    ).strip()
    assert trace_id, "the query span of the traced query is not in node1's span log"

    def span_ids(node, source):
        return set(
            node.query(f"SELECT DISTINCT span_id FROM {source} WHERE trace_id = '{trace_id}'").split()
        )

    local_ids = span_ids(node1, "system.opentelemetry_span_log")
    remote_ids = span_ids(node2, "system.opentelemetry_span_log")
    assert remote_ids, "node2 wrote no spans for the trace: the trace context did not propagate"
    assert remote_ids.isdisjoint(local_ids), "a span is in both logs: the two logs are not distinct"

    # Without `cluster`, only the local log is read; with it, the spans of both nodes appear.
    local_rows = int(node1.query(f"SELECT count() FROM traceView('{trace_id}')"))
    all_rows = int(node1.query(f"SELECT count() FROM traceView('{trace_id}', 40, 'two_nodes')"))
    assert local_rows == len(local_ids)
    assert all_rows == len(local_ids) + len(remote_ids)

    # The remote spans are attached under the local tree, not rendered as a separate forest:
    # node2's query span is a child of node1's fragment span, so it carries a tree connector.
    remote_query_span = node1.query(
        f"SELECT span FROM traceView('{trace_id}', 40, 'two_nodes')"
        f" WHERE attribute['clickhouse.query_id'] != '{query_id}' AND span LIKE '%query%' LIMIT 1"
    ).strip()
    assert remote_query_span, "the query span of node2 is not in the merged trace"
    assert "─ " in remote_query_span, f"the remote query span is not attached to the tree: {remote_query_span!r}"
