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

HELLO = b'\x00'
PING = b'\x01'

EXISTS = b'\x05'
IS_FILE = b'\x06'
IS_DIRECTORY = b'\x07'

CREATE_DIRECTORY = b'\x09'
CREATE_DIRECTORIES = b'\x0A'

CREATE_FILE = b'\x0E'

ERROR = b'\xFF\x01' # 255


def isErrorResp(data: bytes) -> bool:
    return data[:2] == ERROR


def bytesToNum(b: bytes) -> int:
    x = 0
    for i in range(len(b)):
        x |= (b[i] & 0x7F) << (7 * i)
        if (b[i] & 0x80) == 0:
            return x
    return x


def numToBytes(x: int) -> bytes:
    b = bytearray()
    while x:
        byte = x & 0x7F
        if x > 0x7F:
            byte |= 0x80
        b.append(byte)
        x >>= 7
    return bytes(b)


def strToBinary(s: str) -> bytes:
    sBytes = s.encode()
    lenBytes = numToBytes(len(s))
    return lenBytes + sBytes


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function")
def conn(start_cluster):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        for i in range(5):
            try:
                s.connect((node.ip_address, REMOTE_FS_PORT))
                break
            except:
                if i == 4:
                    raise
                sleep(5)
        yield s


@pytest.fixture(scope="function")
def disk_conn(conn):
    conn.send(HELLO + strToBinary(CLIENT_DISK_NAME))
    data = conn.recv(1024)
    assert data == HELLO
    yield conn
        


def test_bad_disk_name(conn):
    conn.send(HELLO + strToBinary("some_name"))
    print("sended", flush=True)
    data = conn.recv(1024)
    assert isErrorResp(data)
    strLen = data[2]
    assert data[3:3+strLen+1].decode() == "Unknown disk some_name"


def test_ping(conn):
    conn.send(HELLO + strToBinary(CLIENT_DISK_NAME))
    data = conn.recv(1024)
    assert data == HELLO

    conn.send(PING)
    data = conn.recv(1024)
    assert data == PING


@pytest.mark.parametrize("command,path,is_dir,is_error", [
    (CREATE_FILE, "file_name", False, False),
    (CREATE_DIRECTORY, "dir_path", True, False),
    (CREATE_DIRECTORY, "dir/path", True, True),
    (CREATE_DIRECTORIES, "dir/path", True, False), 
])
def test_create_smth(disk_conn, command, path, is_dir, is_error):
    path = strToBinary(path)
    disk_conn.send(command + path)
    data = disk_conn.recv(1024)
    if is_error:
        assert isErrorResp(data)
        return
    assert data == command

    disk_conn.send(EXISTS + path)
    data = disk_conn.recv(1024)
    assert data[:1] == EXISTS
    assert data[1:] == b'1'

    disk_conn.send(IS_FILE + path)
    data = disk_conn.recv(1024)
    assert data[:1] == IS_FILE
    if is_dir:
        assert data[1:] == b'0'
    else:
        assert data[1:] == b'1'

    disk_conn.send(IS_DIRECTORY + path)
    data = disk_conn.recv(1024)
    assert data[:1] == IS_DIRECTORY
    if is_dir:
        assert data[1:] == b'1'
    else:
        assert data[1:] == b'0'

