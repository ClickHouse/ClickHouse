import pytest
import requests
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_server_histogram_metrics_in_system_table(started_cluster):
    result = node.query(
        """
        SELECT DISTINCT
            metric
        FROM system.histogram_metrics
        WHERE metric LIKE '%keeper_%'
        """
    )

    expected_metrics = (
        'keeper_client_queue_duration_milliseconds',
        'keeper_client_roundtrip_duration_milliseconds',
        'keeper_response_time_ms',
        'keeper_server_preprocess_request_duration_milliseconds',
        'keeper_server_queue_duration_milliseconds',
        'keeper_server_send_duration_milliseconds',
    )
    for metric_name in expected_metrics:
        assert metric_name in result
