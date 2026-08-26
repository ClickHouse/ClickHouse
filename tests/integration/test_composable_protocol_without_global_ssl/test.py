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
        "keys/server.pem",
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

    # Verify that system.certificates includes per-protocol CA certificates
    result = client.query(
        "SELECT count() = 2 AND countIf(path LIKE '%server.pem%') = 2 FROM system.certificates WHERE protocol = 'tcp_secure'"
    ).strip()
    assert result == "1"
