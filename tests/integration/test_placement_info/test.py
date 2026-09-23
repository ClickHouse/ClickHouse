import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

METADATA_SERVER_HOSTNAME = "node_imds"
METADATA_SERVER_PORT = 8080

cluster = ClickHouseCluster(__file__)
node_imds = cluster.add_instance(
    "node_imds",
    main_configs=["configs/imds_bootstrap.xml"],
    env_variables={
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": f"http://{METADATA_SERVER_HOSTNAME}:{METADATA_SERVER_PORT}",
    },
    stay_alive=True,
)
node_config_value = cluster.add_instance(
    "node_config_value",
    main_configs=["configs/config_value.xml"],
)
node_file_value = cluster.add_instance(
    "node_file_value",
    main_configs=["configs/file_value.xml"],
    stay_alive=True,
)
node_missing_value = cluster.add_instance(
    "node_missing_value",
    main_configs=["configs/missing_value.xml"],
)


def start_metadata_server(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "metadata_servers")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            (
                "simple_server.py",
                METADATA_SERVER_HOSTNAME,
                METADATA_SERVER_PORT,
            )
        ],
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        start_metadata_server(cluster)
        yield cluster
    finally:
        cluster.shutdown()


def test_placement_info_from_imds():
    with open(os.path.join(os.path.dirname(__file__), "configs/imds.xml"), "r") as f:
        node_imds.replace_config(
            "/etc/clickhouse-server/config.d/imds_bootstrap.xml", f.read()
        )
    node_imds.stop_clickhouse(kill=True)
    node_imds.start_clickhouse()

    node_imds.query("SYSTEM FLUSH LOGS")
    assert node_imds.contains_in_log(
        "CloudPlacementInfo: Loaded info: availability_zone: ci-test-1a"
    )


def test_placement_info_from_config():
    node_config_value.query("SYSTEM FLUSH LOGS")
    assert node_config_value.contains_in_log(
        "CloudPlacementInfo: Loaded info: availability_zone: ci-test-1b"
    )


def test_placement_info_from_file():
    node_file_value.exec_in_container(
        ["bash", "-c", "echo ci-test-1c > /tmp/node-zone"]
    )

    node_file_value.stop_clickhouse(kill=True)
    node_file_value.start_clickhouse()

    node_file_value.query("SYSTEM FLUSH LOGS")
    assert node_file_value.contains_in_log(
        "CloudPlacementInfo: Loaded info: availability_zone: ci-test-1c"
    )


def test_placement_info_missing_data():
    node_missing_value.query("SYSTEM FLUSH LOGS")
    assert node_missing_value.contains_in_log(
        "CloudPlacementInfo: Availability zone info not found"
    )
