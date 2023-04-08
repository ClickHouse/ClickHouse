import socket
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
)

REMOTE_FS_PORT = 9012
CLIENT_DISK_NAME = "client_disk"


def bytesToNum(b: bytes):
    x = 0
    for i in range(len(b)):
        x |= (b[i] & 0x7F) << (7 * i)
        if (b[i] & 0x80) == 0:
            return x
    return x


def strToBinary(s: str) -> bytes:
    sBytes = s.encode()
    lenBytes = bytearray()
    x = len(sBytes)
    while x:
        byte = x & 0x7F
        if x > 0x7F:
            byte |= 0x80
        lenBytes.append(byte)
        x >>= 7
    return lenBytes + sBytes


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_bad_disk_name(start_cluster):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        sleep(15)
        s.connect((node.ip_address, REMOTE_FS_PORT))
        s.send(b'\x00' + strToBinary("some_name"))
        print("sended", flush=True)
        data = s.recv(1024)
        assert bytesToNum(data[:2]) == 255
        strLen = data[2]
        assert data[3:3+strLen+1].decode() == "Unknown disk some_name"


def test_remote(start_cluster):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        sleep(15)
        s.connect((node.ip_address, REMOTE_FS_PORT))
        s.send(b'\x00' + strToBinary(CLIENT_DISK_NAME))
        print("sended", flush=True)
        data = s.recv(1024)
        assert data == b'\x00'

        s.send(b'\x01')
        data = s.recv(1024)
        assert data == b'\x01'
