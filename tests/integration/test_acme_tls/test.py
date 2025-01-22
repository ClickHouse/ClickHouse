import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node_acme",
    main_configs=["configs/config.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

def start_metadata_server(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            (
                "acme_server.py",
                "node_acme",
                "80",
            )
        ],
    )

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        start_metadata_server(cluster)
        yield cluster
    finally:
        cluster.shutdown()


def test_acme_authorization(started_cluster):
    for _ in range(60):
        time.sleep(1)

        curl_result = node.exec_in_container(
            ["bash", "-c", "curl -k -v 'https://127.0.0.1:3443' 2>&1 | grep issuer: || true"]
        )

        print(curl_result)
        if "O=ClickHouse; CN=Integration tests" in curl_result:
            return

    raise Exception("Failed to get expected certificate issuer")
