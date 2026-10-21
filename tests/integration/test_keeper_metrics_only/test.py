import pytest
import requests
from helpers.cluster import ClickHouseCluster
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
