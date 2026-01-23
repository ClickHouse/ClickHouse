import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import Client
import os

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node", main_configs=["configs/ssl_config.xml"])

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_ip_block_message_ssl():
    # Connect to the secure port (9440).
    # Rejection happens after the SSL handshake completes, driven by <networks> in ssl_config.xml.
    
    # We need a client config that accepts invalid certificates (since self-signed)
    client_config = """
<clickhouse>
    <accept-invalid-certificate>1</accept-invalid-certificate>
</clickhouse>
"""
    # Write config to temp file
    config_path = os.path.join(os.path.dirname(__file__), "configs", "client_ssl_allow.xml")
    with open(config_path, "w") as f:
        f.write(client_config)

    client = Client(
        instance.ip_address,
        9440,
        command=cluster.client_bin_path,
        secure=True,
        config=config_path
    )
    
    # The client invocation should fail with the error message from the server
    # We expect "IP address not allowed" in the stderr/stdout which is captured in the exception
    try:
        with pytest.raises(Exception) as excinfo:
            client.query("SELECT 1")
        
        assert "IP address not allowed" in str(excinfo.value)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)

def test_ip_block_message_plain():
    # Connect to the insecure port (9000).
    client = Client(
        instance.ip_address,
        9000,
        command=cluster.client_bin_path,
        secure=False
    )
    
    # Should receive error message immediately
    with pytest.raises(Exception) as excinfo:
        client.query("SELECT 1")
        
    assert "IP address not allowed" in str(excinfo.value)
