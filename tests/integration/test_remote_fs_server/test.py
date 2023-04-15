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
GET_FILE_SIZE = b'\x08'

CREATE_DIRECTORY = b'\x09'
CREATE_DIRECTORIES = b'\x0A'

CREATE_FILE = b'\x0E' # 15

READ_FILE = b'\x14' # 20

WRITE_FILE = b'\x15' #21
END_WRITE_FILE = b'\x79' #121

DATA_PACKET = b'\x37' # 55

ERROR = b'\xFF\x01' # 255


RewriteMode = b'\x00'
AppendMode = b'\x01'

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
    if x == 0:
        return b'\x00'
    b = bytearray()
    while x:
        byte = x & 0x7F
        if x > 0x7F:
            byte |= 0x80
        b.append(byte)
        x >>= 7
    return bytes(b)


def bytesToStr(b: bytes) -> str:
    strLen = bytesToNum(b)
    return b[len(b) - strLen:].decode()


def strToBytes(s: str) -> bytes:
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
    conn.send(HELLO + strToBytes(CLIENT_DISK_NAME))
    data = conn.recv(1024)
    assert data == HELLO
    yield conn
        


# def test_bad_disk_name(conn):
#     conn.send(HELLO + strToBytes("some_name"))
#     print("sended", flush=True)
#     data = conn.recv(1024)
#     assert isErrorResp(data)
#     strLen = data[2]
#     assert data[3:3+strLen+1].decode() == "Unknown disk some_name"


# def test_ping(conn):
#     conn.send(HELLO + strToBytes(CLIENT_DISK_NAME))
#     data = conn.recv(1024)
#     assert data == HELLO

#     conn.send(PING)
#     data = conn.recv(1024)
#     assert data == PING


# @pytest.mark.parametrize("command,path,is_dir,is_error", [
#     (CREATE_FILE, "file_name", False, False),
#     (CREATE_DIRECTORY, "dir_path", True, False),
#     (CREATE_DIRECTORY, "dir/path", True, True),
#     (CREATE_DIRECTORIES, "dir/path", True, False), 
# ])
# def test_create_smth(disk_conn, command, path, is_dir, is_error):
#     path = strToBytes(path)
#     disk_conn.send(command + path)
#     data = disk_conn.recv(1024)
#     if is_error:
#         assert isErrorResp(data)
#         return
#     assert data == command

#     disk_conn.send(EXISTS + path)
#     data = disk_conn.recv(1024)
#     assert data[:1] == EXISTS
#     assert data[1:] == b'1'

#     disk_conn.send(IS_FILE + path)
#     data = disk_conn.recv(1024)
#     assert data[:1] == IS_FILE
#     if is_dir:
#         assert data[1:] == b'0'
#     else:
#         assert data[1:] == b'1'

#     disk_conn.send(IS_DIRECTORY + path)
#     data = disk_conn.recv(1024)
#     assert data[:1] == IS_DIRECTORY
#     if is_dir:
#         assert data[1:] == b'1'
#     else:
#         assert data[1:] == b'0'


# @pytest.mark.parametrize("mode,file_name", [
#     (RewriteMode, "test_file_0"),
#     (AppendMode, "test_file_1"), 
# ])
# def test_write_file(disk_conn, mode, file_name):
#     file_name = strToBytes(file_name)
#     disk_conn.send(CREATE_FILE + file_name)
#     data = disk_conn.recv(1024)

#     disk_conn.send(WRITE_FILE + file_name + b"\x0A" + mode)
#     data = disk_conn.recv(1024)
#     assert data == WRITE_FILE

#     disk_conn.send(DATA_PACKET + strToBytes('1234567890'))
#     data = disk_conn.recv(1024)
#     assert data == DATA_PACKET
#     disk_conn.send(DATA_PACKET + strToBytes('abcdefghij'))
#     data = disk_conn.recv(1024)
#     assert data == DATA_PACKET

#     disk_conn.send(END_WRITE_FILE)
#     data = disk_conn.recv(1024)
#     assert data == END_WRITE_FILE

#     disk_conn.send(GET_FILE_SIZE + file_name)
#     data = disk_conn.recv(1024)
#     assert data[:1] == GET_FILE_SIZE
#     assert bytesToNum(data[1:]) == 20


@pytest.mark.parametrize("offset,size", [
    (0, 20),
    (10, 10), 
])
def test_read_file(disk_conn, offset, size):
    test_data = '1234567890abcdefghij'
    file_name = strToBytes("file_name")
    disk_conn.send(WRITE_FILE + file_name + numToBytes(20) + RewriteMode)
    data = disk_conn.recv(1024)
    assert data == WRITE_FILE

    disk_conn.send(DATA_PACKET + strToBytes(test_data))
    data = disk_conn.recv(1024)
    assert data == DATA_PACKET

    disk_conn.send(END_WRITE_FILE)
    data = disk_conn.recv(1024)
    assert data == END_WRITE_FILE

    disk_conn.send(READ_FILE + file_name + numToBytes(offset) + numToBytes(size))
    data = disk_conn.recv(1024)
    assert data[:1] == READ_FILE
    assert bytesToStr(data[1:]) == test_data[offset:offset+size]