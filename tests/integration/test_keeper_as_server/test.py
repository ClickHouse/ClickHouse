import pytest
import requests
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import time

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
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
