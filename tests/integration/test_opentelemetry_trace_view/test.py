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
# The span log is configured but never written: `system.opentelemetry_span_log` is created by the
# first flush of spans, and no traced query ever reaches this node.
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml", "configs/enable_span_log.xml"], with_zookeeper=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_traced_query_over_two_nodes():
    """A distributed query started on node1 with tracing: node2 executes the second shard and
    writes its spans, with the same trace id, to its own span log.

    Returns the query id, the trace id, and the span ids of the trace in the logs of node1 and node2.
    """
    query_id = f"trace_view_{uuid.uuid4().hex}"
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

    def span_ids(node):
        return set(
            node.query(
                f"SELECT DISTINCT span_id FROM system.opentelemetry_span_log WHERE trace_id = '{trace_id}'"
            ).split()
        )

    local_ids = span_ids(node1)
    remote_ids = span_ids(node2)
    assert remote_ids, "node2 wrote no spans for the trace: the trace context did not propagate"
    assert remote_ids.isdisjoint(local_ids), "a span is in both logs: the two logs are not distinct"
    return query_id, trace_id, local_ids, remote_ids


def test_cluster_argument_reads_the_span_log_of_every_node(started_cluster):
    query_id, trace_id, local_ids, remote_ids = run_traced_query_over_two_nodes()

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


def test_cluster_argument_skips_a_replica_without_a_span_log(started_cluster):
    # `clusterAllReplicas` reads every replica of the cluster, not only those that took part in
    # the trace: a fresh replica that never flushed a span has no span log table, and the read
    # must not fail on it with UNKNOWN_TABLE while the trace is on the other nodes.
    assert node3.query("EXISTS TABLE system.opentelemetry_span_log").strip() == "0", (
        "node3 has a span log: the test needs a node that never flushed a span"
    )

    query_id, trace_id, local_ids, remote_ids = run_traced_query_over_two_nodes()

    # The trace of the two nodes with a span log, read through a cluster that also has node3.
    all_rows = int(node1.query(f"SELECT count() FROM traceView('{trace_id}', 40, 'three_nodes')"))
    assert all_rows == len(local_ids) + len(remote_ids)
    by_query_id = int(node1.query(f"SELECT count() FROM traceView(query_id = '{query_id}', cluster = 'three_nodes')"))
    assert by_query_id == all_rows

    # Reading the trace did not create the span log on node3 as a side effect.
    assert node3.query("EXISTS TABLE system.opentelemetry_span_log").strip() == "0"

    # A cluster where no replica has a span log yet says so, instead of failing on a replica.
    error = node1.query_and_get_error(f"SELECT count() FROM traceView('{trace_id}', 40, 'fresh_node_only')")
    assert "No replica of cluster 'fresh_node_only' has the table system.opentelemetry_span_log yet" in error
    assert "UNKNOWN_TABLE" not in error


def test_missing_span_log_is_not_revealed_without_select(started_cluster):
    # Whether the span log exists is a fact about a guarded table: a caller without SELECT on it
    # is denied before the table is looked up, locally and on the replicas of a cluster, so the
    # "does not exist yet" hint never reaches them.
    assert node3.query("EXISTS TABLE system.opentelemetry_span_log").strip() == "0"
    node3.query("DROP USER IF EXISTS no_span_log_access")
    node3.query("CREATE USER no_span_log_access")
    node3.query("GRANT REMOTE ON *.* TO no_span_log_access")
    try:
        for query in (
            "SELECT count() FROM traceView('00000000-0000-0000-0000-000000000001')",
            "SELECT count() FROM traceView(query_id = 'no_such_query')",
            "SELECT count() FROM traceView('00000000-0000-0000-0000-000000000001', 40, 'fresh_node_only')",
        ):
            error = node3.query_and_get_error(query, user="no_span_log_access")
            assert "ACCESS_DENIED" in error, error
            assert "does not exist yet" not in error, error
    finally:
        node3.query("DROP USER no_span_log_access")
