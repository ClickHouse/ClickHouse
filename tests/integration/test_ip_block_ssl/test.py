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

def test_ip_block_message_ssl():
    # Connect to the secure port (9440).
    # Rejection should happen at the TCP stack level due to <networks> in ssl_config.xml.
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    
    wrapped_socket = context.wrap_socket(s, server_hostname='localhost')
    
    try:
        wrapped_socket.connect((instance.ip_address, 9440))
        
        # We expect our specific error message after the SSL handshake completes
        data = wrapped_socket.recv(1024)
        if not data:
            pytest.fail("Connection closed without receiving error message")
            
        error_msg = data.decode('utf-8', errors='ignore')
        
        # We expect our specific error message
        assert "IP address not allowed" in error_msg
        assert "Code: 195" in error_msg
        
    except ssl.SSLError as e:
        pytest.fail(f"SSL Handshake failed or connection broken without message: {e}")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred: {e}")
    finally:
        wrapped_socket.close()

def test_ip_block_message_plain():
    # Connect to the insecure port (9000).
    # Rejection should happen immediately before any handshake.
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    
    try:
        s.connect((instance.ip_address, 9000))
        
        # Should receive error message immediately
        data = s.recv(1024)
        if not data:
            pytest.fail("Connection closed without receiving error message")
            
        error_msg = data.decode('utf-8', errors='ignore')
        
        # We expect our specific error message
        assert "IP address not allowed" in error_msg
        assert "Code: 195" in error_msg
        
    except Exception as e:
        pytest.fail(f"Connection failed or no message received: {e}")
    finally:
        s.close()
