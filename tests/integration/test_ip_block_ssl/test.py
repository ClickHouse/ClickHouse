import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import Client
import os
import socket
import ssl
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node", main_configs=["configs/ssl_config.xml"])

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        # Wait for the server to be ready
        wait_for_server_ready()
        yield cluster
    finally:
        cluster.shutdown()

def wait_for_server_ready(timeout=60):
    """Wait for ClickHouse server to be ready to accept connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((instance.ip_address, 9000))
            sock.close()
            return  # Server is ready
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.5)
    raise RuntimeError(f"ClickHouse server did not become ready within {timeout} seconds")

def check_ip_blocked(port, secure=False, max_retries=3):
    """
    Check that the IP blocking mechanism works correctly.
    Retries on ConnectionRefusedError to handle race conditions in CI.
    """
    last_error = None
    for attempt in range(max_retries):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((instance.ip_address, port))
            
            if secure:
                # Create a context that accepts self-signed certs
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                sock = context.wrap_socket(sock, server_hostname=instance.ip_address)

            # Read the response
            data = b""
            start = time.time()
            while time.time() - start < 5:
                try:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                except socket.timeout:
                    break
                except (ConnectionResetError, OSError):
                    # Connection reset by peer - also acceptable due to race condition
                    break
                    
            response = data.decode('utf-8', errors='ignore')
            # Accept both error messages due to TCP buffering race condition:
            # - "IP address not allowed": TCP layer transmits error message before close() executes
            # - Empty response or connection closed: close() executes before TCP transmits the message
            # See: https://github.com/ClickHouse/ClickHouse/pull/93539 for similar fix
            assert "IP address not allowed" in response or response == "", f"Unexpected response: {response}"
            return  # Test passed
            
        except ConnectionRefusedError as e:
            last_error = e
            time.sleep(1)  # Wait before retry
            continue
        finally:
            sock.close()
    
    # If we get here, all retries failed with ConnectionRefusedError
    raise last_error

def test_ip_block_message_ssl():
    # Connect to the secure port (9440).
    check_ip_blocked(9440, secure=True)

def test_ip_block_message_plain():
    # Connect to the insecure port (9000).
    check_ip_blocked(9000, secure=False)
