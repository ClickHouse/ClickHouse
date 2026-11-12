import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, keeper_config_dir="configs/")

# The instance is only needed to start the standalone Keeper nodes (zoo1, zoo2, zoo3).
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def scrape_metrics(headers):
    return requests.get(
        f"http://{cluster.get_instance_ip('zoo1')}:9363/metrics",
        headers=headers,
        timeout=10,
    )


def test_standalone_keeper_metrics_with_traceparent(started_cluster):
    started_cluster.wait_zookeeper_to_start()

    trace_id = uuid.uuid4().hex
    tracing_headers = {
        "traceparent": f"00-{trace_id}-5150000000000515-01",
        "tracestate": "some custom state",
    }

    for headers in ({}, tracing_headers):
        response = scrape_metrics(headers)
        assert response.status_code == 200, response.text
        assert "ClickHouseAsyncMetrics_KeeperIsStandalone" in response.text
