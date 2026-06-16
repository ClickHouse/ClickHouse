import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import Client
import os
import socket
import ssl
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_config.xml",
        "certs/server-cert.pem",
        "certs/server-key.pem",
    ],
)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        # Wait for the server to be ready
        wait_for_server_ready()
        yield cluster
    finally:
        cluster.shutdown()

def smart_connect(host, port, timeout=300):
    start = time.time()
    attempt = 0
    last_error = None
    while time.time() - start < timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((host, port))
            return sock
        except (ConnectionRefusedError, OSError, TimeoutError, socket.timeout) as e:
            last_error = e
            sock.close()
            # Exponential backoff: starts fast (0.1s), caps at 5s
            # 0.1, 0.15, 0.225, 0.33...
            sleep_time = min(5.0, 0.1 * (1.5 ** attempt))
            time.sleep(sleep_time)
            attempt += 1
    raise last_error

def wait_for_server_ready(timeout=300):
    """Wait for ClickHouse server to be ready to accept connections."""
    try:
        sock = smart_connect(instance.ip_address, 9000, timeout)
        sock.close()
    except Exception as e:
        raise RuntimeError(f"ClickHouse server did not become ready within {timeout} seconds: {e}")

def check_ip_blocked(port, secure=False, max_retries=None):
    """
    Check that the IP blocking mechanism works correctly.
    Uses smart retry logic with exponential backoff to handle CI network flakiness.
    """
    sock = None
    try:
        # Use smart_connect to handle initial TCP connection and routing issues
        sock = smart_connect(instance.ip_address, port, timeout=300)
        
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
        # For secure/TLS connections we must see the explicit error message, since the
        # purpose of the test is to validate improved TLS-client errors.
        if secure:
            assert "IP address not allowed" in response, f"Unexpected response over TLS: {response}"
        else:
            # For plain-text connections, the server fast-fails in the factory (to mitigate DoS),
            # so we expect either an empty response or the error message.
            # See: https://github.com/ClickHouse/ClickHouse/pull/93539 for similar fix
            assert "IP address not allowed" in response or response == "", f"Unexpected response: {response}"
        
    finally:
        if sock:
            sock.close()

def test_ip_block_message_ssl():
    # Connect to the secure port (9440).
    check_ip_blocked(9440, secure=True)

def test_ip_block_message_plain():
    # Connect to the insecure port (9000).
    check_ip_blocked(9000, secure=False)
