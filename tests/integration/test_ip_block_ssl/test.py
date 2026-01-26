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

import socket
import ssl
import time

def check_ip_blocked(port, secure=False):
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

        # We don't need to send anything for the server to reject us if the IP is blocked.
        # But for SSL, the handshake (wrap_socket) happens first.
        # After handshake (or immediately for plain), the server should send the error message.
        
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
                
        response = data.decode('utf-8', errors='ignore')
        assert "IP address not allowed" in response, f"Unexpected response: {response}"
        
    finally:
        sock.close()

def test_ip_block_message_ssl():
    # Connect to the secure port (9440).
    check_ip_blocked(9440, secure=True)

def test_ip_block_message_plain():
    # Connect to the insecure port (9000).
    check_ip_blocked(9000, secure=False)
