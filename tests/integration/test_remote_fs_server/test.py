import socket
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

REMOTE_FS_PORT = 9012


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote(start_cluster):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        sleep(15)
        s.connect((node.ip_address, REMOTE_FS_PORT))
        s.send(b'\x00')
        data = s.recv(1024)
        assert data == b'\x00'

        s.send(b'\x01')
        data = s.recv(1024)
        assert data == b'\x01'
