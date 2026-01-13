import pytest
from helpers.cluster import ClickHouseCluster
import ssl
import socket

cluster = ClickHouseCluster(__file__)
# The config includes a <networks> restriction that blocks our current IP.
instance = cluster.add_instance("node", main_configs=["configs/ssl_config.xml"])

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def verify_ip_block_error(data):
    if not data:
        pytest.fail("Connection closed without receiving error message")

    error_msg = data.decode('utf-8', errors='replace')
    
    # We expect our specific error message
    assert "IP address not allowed" in error_msg
    assert "Code: 195" in error_msg


def test_ip_block_message_ssl():
    # Connect to the secure port (9440).
    # Rejection happens after the SSL handshake completes, driven by <networks> in ssl_config.xml.
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
    except OSError as e:
        pytest.fail(f"Failed to create socket: {e}")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    
    wrapped_socket = None
    
    try:
        s.connect((instance.ip_address, 9440))
        wrapped_socket = context.wrap_socket(s, server_hostname='localhost')
        
        # We expect our specific error message after the SSL handshake completes
        data = wrapped_socket.recv(1024)
        verify_ip_block_error(data)
        
    finally:
        if wrapped_socket:
            wrapped_socket.close()
        else:
            s.close()

def test_ip_block_message_plain():
    # Connect to the insecure port (9000).
    # Rejection happens immediately after TCP connection, before any ClickHouse protocol exchange.
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
    except OSError as e:
        pytest.fail(f"Failed to create socket: {e}")
    
    try:
        s.connect((instance.ip_address, 9000))
        
        # Should receive error message immediately
        data = s.recv(1024)
        verify_ip_block_error(data)
        
    finally:
        s.close()
