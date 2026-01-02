import os
import os.path as p
import time

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance(
    "server",
    base_config_dir="configs/server",
    main_configs=[
        "keys/server.crt",
        "keys/server.key",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_composable_protocol_without_global_ssl():
    # Test that the TLS port works with protocol-specific cert configuration only, without global openSSL config section required.
    client = Client(
        server.ip_address,
        9440,
        command=cluster.client_bin_path,
        secure=True,
        config=f"{SCRIPT_DIR}/configs/client/client.xml",
    )
    assert client.query("SELECT 1") == "1\n"
