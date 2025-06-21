import os
import pytest

from helpers.cluster import ClickHouseCluster

# Tests the validation of asynchronous metrics settings:
# asynchronous_metrics_update_period_s & asynchronous_heavy_metrics_update_period_s

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.add_instance(
            "node_invalid_settings",
            main_configs=["configs/invalid_settings.xml"],
        )

        # The cluster should fail to start when trying to start the node with invalid settings
        with pytest.raises(Exception) as exc_info:
            cluster.start()

            assert "Settings asynchronous_metrics_update_period_s and asynchronous_heavy_metrics_update_period_s must not be zero" in str(exc_info.value)

        yield cluster
    finally:
        cluster.shutdown()


def test_invalid_settings(start_cluster):
    # Check that the error message is as expected
    logs = ""
    error_logs_file = os.path.join(
        cluster.instances_dir,
        "node_invalid_settings",
        "logs",
        "clickhouse-server.err.log",
    )
    with open(error_logs_file, "r") as f:
        logs = f.read()

    assert "Settings asynchronous_metrics_update_period_s and asynchronous_heavy_metrics_update_period_s must not be zero" in logs
