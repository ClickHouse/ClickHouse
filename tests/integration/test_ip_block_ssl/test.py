import pytest
from helpers.cluster import ClickHouseCluster
import ssl
import socket

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node", main_configs=["configs/ssl_config.xml"])

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        instance.query("CREATE USER blocked_user IDENTIFIED WITH no_password HOST IP '1.1.1.1'")
        yield cluster
    finally:
        cluster.shutdown()

def test_ip_block_message_ssl():
    # Connect to the secure port (usually 9440 for TCP secure)
    # The user 'blocked_user' is only allowed from 1.1.1.1, so we (local) should be blocked.
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Wrap in SSL using standard library, similar to how clients would
    # We use PROTOCOL_TLS_CLIENT to ensure we attempt a handshake
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    
    wrapped_socket = context.wrap_socket(s, server_hostname='localhost')
    
    try:
        wrapped_socket.connect((instance.ip_address, 9440))
        
        # Send Hello packet (ClientHello is handled by SSL wrap, this is ClickHouse Hello)
        # But wait, if we are blocked, the server might close immediately after handshake.
        # Let's try to read immediately.
        
        data = wrapped_socket.read(1024)
        error_msg = data.decode('utf-8', errors='ignore')
        
        # We expect our specific error message
        assert "IP address not allowed" in error_msg
        assert "Code: 195" in error_msg
        
    except ssl.SSLError as e:
        # If the fix works, we should NOT get a generic SSL error like EOF occurred in violation of protocol
        # We should have successfully established SSL, then read the error.
        # But if the socket is closed *cleanly* after writing, read() returns empty bytes or text.
        pytest.fail(f"SSL Handshake failed or connection broken without message: {e}")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")
    finally:
        wrapped_socket.close()
