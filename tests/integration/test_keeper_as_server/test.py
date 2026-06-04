import pytest
import requests
from dataclasses import dataclass
from typing import Optional
from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import time


@dataclass
class KeeperClusterRow:
    server_id: int
    host: str
    endpoint: str
    is_observer: bool
    priority: int
    is_leader: bool
    is_self: bool
    last_log_index: Optional[int]

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
)

cluster_node1 = cluster.add_instance(
    "cluster_node1",
    main_configs=["configs/cluster_keeper1.xml"],
    stay_alive=True,
    with_zookeeper=False,
)
cluster_node2 = cluster.add_instance(
    "cluster_node2",
    main_configs=["configs/cluster_keeper2.xml"],
    stay_alive=True,
    with_zookeeper=False,
)
cluster_node3 = cluster.add_instance(
    "cluster_node3",
    main_configs=["configs/cluster_keeper3.xml"],
    stay_alive=True,
    with_zookeeper=False,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_prometheus_keeper_metrics_only(start_cluster):
    keeper_only_async_metric_exists = int(
        node.query("SELECT count() > 0 FROM system.asynchronous_metrics WHERE metric = 'KeeperIsLeader'").strip()
    )
    assert keeper_only_async_metric_exists

    prometheus_handler_response = requests.get(
        f"http://{node.ip_address}:8001/metrics",
        timeout=5,
    )
    assert prometheus_handler_response.status_code == 200
    assert "ClickHouseAsyncMetrics_KeeperIsStandalone" in prometheus_handler_response.text
    assert "PolygonDictionaryThreads" not in prometheus_handler_response.text


def test_asynchronous_metric_log(start_cluster):
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM system.asynchronous_metric_log WHERE metric = 'KeeperIsLeader'",
        "1",
    )


def test_skip_alias_columns(start_cluster):
    node.query("SYSTEM FLUSH LOGS")
    # build_id is an ALIAS column on trace_log. With skip_alias_columns=true,
    # it should be absent from the table schema.
    error = node.query_and_get_error("SELECT build_id FROM system.trace_log LIMIT 0")
    assert "UNKNOWN_IDENTIFIER" in error


def _read_keeper_cluster_view(node):
    text = node.query(
        "SELECT server_id, host, endpoint, is_observer, priority, is_leader, is_self, last_log_index "
        "FROM system.keeper_cluster ORDER BY server_id FORMAT TSV"
    ).strip().split("\n")
    rows = []
    for line in text:
        f = line.split("\t")
        rows.append(KeeperClusterRow(
            server_id=int(f[0]),
            host=f[1],
            endpoint=f[2],
            is_observer=f[3] == "1",
            priority=int(f[4]),
            is_leader=f[5] == "1",
            is_self=f[6] == "1",
            last_log_index=None if f[7] == "\\N" else int(f[7]),
        ))
    return rows


def test_keeper_cluster_invariants(start_cluster):
    if cluster_node1.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers: slow queries can cause leader changes due to timeouts, making leader invariants flaky")

    for n in (cluster_node1, cluster_node2, cluster_node3):
        keeper_utils.wait_until_connected(cluster, n)

    views = {n.name: _read_keeper_cluster_view(n) for n in (cluster_node1, cluster_node2, cluster_node3)}

    for name, rows in views.items():
        assert len(rows) == 3, f"{name} saw {len(rows)} rows, expected 3"

    leader_ids = set()
    for name, rows in views.items():
        leaders = [r for r in rows if r.is_leader]
        assert len(leaders) == 1, f"{name} sees {len(leaders)} leaders, expected exactly 1"
        leader_ids.add(leaders[0].server_id)
    assert len(leader_ids) == 1, f"nodes disagree on leader server_id: {leader_ids}"
    leader_id = next(iter(leader_ids))
    assert leader_id in {1, 2}, f"observer node 3 must not be leader, got {leader_id}"

    expected_self_id = {"cluster_node1": 1, "cluster_node2": 2, "cluster_node3": 3}
    for name, rows in views.items():
        selves = [r for r in rows if r.is_self]
        assert len(selves) == 1, f"{name} sees {len(selves)} self rows, expected exactly 1"
        assert selves[0].server_id == expected_self_id[name], (
            f"{name} self row has server_id={selves[0].server_id}, expected {expected_self_id[name]}"
        )

    for name, rows in views.items():
        non_null = [r for r in rows if r.last_log_index is not None]
        assert len(non_null) == 1, f"{name} has {len(non_null)} non-null last_log_index rows, expected 1"
        assert non_null[0].is_self, f"{name} non-null last_log_index row is not is_self"
        assert non_null[0].last_log_index >= 1

    observer_by_id = {}
    for rows in views.values():
        for r in rows:
            observer_by_id.setdefault(r.server_id, set()).add(r.is_observer)
            assert len(observer_by_id[r.server_id]) == 1
    assert observer_by_id == {1: {False}, 2: {False}, 3: {True}}, "expected only node 3 to be observer"
