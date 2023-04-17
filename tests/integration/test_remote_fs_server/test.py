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

GET_TOTAL_SPACE = b'\x02'
GET_AVAILABLE_SPACE = b'\x03'

EXISTS = b'\x05'
IS_FILE = b'\x06'
IS_DIRECTORY = b'\x07'
GET_FILE_SIZE = b'\x08'

CREATE_DIRECTORY = b'\x09'
CREATE_DIRECTORIES = b'\x0A' # 10
CLEAR_DIRECTORY = b'\x0B' # 11
MOVE_DIRECTORY = b'\x0C' # 12

ITERATE_DIRECTORY = b'\x0D' # 13
END_ITERATE_DIRECTORY = b'\x71' # 113

CREATE_FILE = b'\x0E' # 14
MOVE_FILE = b'\x0F' # 15
REPLACE_FILE = b'\x10' # 16 

COPY = b'\x11' # 17
COPY_DIRECTORY_CONTENT = b'\x12' # 18

LIST_FILES = b'\x13' # 19
END_LIST_FILES = b'\x77' # 119

READ_FILE = b'\x14' # 20

WRITE_FILE = b'\x15' # 21
END_WRITE_FILE = b'\x79' # 121

REMOVE_FILE = b'\x16' # 22
REMOVE_FILE_IF_EXISTS = b'\x17' # 23
REMOVE_DIRECTORY = b'\x18' # 24
REMOVE_RECURSIVE = b'\x19' # 25

SET_LAST_MODIFIED = b'\x1A' # 26
GET_LAST_MODIFIED = b'\x1B' # 27
GET_LAST_CHANGED = b'\x1C' # 28
SET_READ_ONLY = b'\x1D' # 29
CREATE_HARD_LINK = b'\x1E' # 30
TRUNCATE_FILE = b'\x1F' # 31

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
    # Clear root dir
    conn.send(CLEAR_DIRECTORY + strToBytes(""))
    data = conn.recv(1024)
    assert data == CLEAR_DIRECTORY
        

def test_bad_disk_name(conn):
    conn.send(HELLO + strToBytes("some_name"))
    data = conn.recv(1024)
    assert isErrorResp(data)
    strLen = data[2]
    assert data[3:3+strLen+1].decode() == "Unknown disk some_name"


def test_ping(conn):
    conn.send(HELLO + strToBytes(CLIENT_DISK_NAME))
    data = conn.recv(1024)
    assert data == HELLO

    conn.send(PING)
    data = conn.recv(1024)
    assert data == PING


@pytest.mark.parametrize("command", [
    (GET_TOTAL_SPACE),
    (GET_AVAILABLE_SPACE),
])
def test_get_space_info(disk_conn, command):
    disk_conn.send(command)
    data = disk_conn.recv(1024)
    assert data[:1] == command
    assert bytesToNum(data[1:]) != 0


@pytest.mark.parametrize("command,path,is_dir,is_error", [
    (CREATE_FILE, "file_name", False, False),
    (CREATE_DIRECTORY, "dir_path", True, False),
    (CREATE_DIRECTORY, "dir/path", True, True),
    (CREATE_DIRECTORIES, "dir/path", True, False), 
])
def test_create_smth(disk_conn, command, path, is_dir, is_error):
    path = strToBytes(path)
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
        # Remove dir so test teardown can work
        disk_conn.send(REMOVE_DIRECTORY + path)
        data = disk_conn.recv(1024)
        assert data == REMOVE_DIRECTORY
    else:
        assert data[1:] == b'0'


def test_clear_directory(disk_conn):
    path = strToBytes("path_to/dir")
    disk_conn.send(CREATE_DIRECTORIES + path)
    data = disk_conn.recv(1024)
    assert data == CREATE_DIRECTORIES

    clear_path = strToBytes("path_to")
    disk_conn.send(CLEAR_DIRECTORY + clear_path)
    data = disk_conn.recv(1024)
    assert data == CLEAR_DIRECTORY

    disk_conn.send(EXISTS + path)
    data = disk_conn.recv(1024)
    assert data[:1] == EXISTS
    assert data[1:] == b'0'


def test_move_directory(disk_conn):
    path = strToBytes("path_to/dir")
    disk_conn.send(CREATE_DIRECTORIES + path)
    data = disk_conn.recv(1024)
    assert data == CREATE_DIRECTORIES

    new_path = strToBytes("dir")
    disk_conn.send(MOVE_DIRECTORY + path + new_path)
    data = disk_conn.recv(1024)
    assert data == MOVE_DIRECTORY

    for (check_path, exists) in [(path, b"0"), (new_path, b"1")]:
        disk_conn.send(EXISTS + check_path)
        data = disk_conn.recv(1024)
        assert data[:1] == EXISTS
        assert data[1:] == exists


def test_iterate_directory(disk_conn):
    file1 = strToBytes("file1")
    file2 = strToBytes("file2")
    for file in [file1, file2]:
        disk_conn.send(CREATE_FILE + file)
        data = disk_conn.recv(1024)
        assert data == CREATE_FILE
    
    disk_conn.send(ITERATE_DIRECTORY + strToBytes(""))
    data = disk_conn.recv(1024)
    entries_count = 0
    while data[:1] != END_ITERATE_DIRECTORY:
        assert data[:1] == DATA_PACKET
        data = data[1:]
        l = bytesToNum(data)
        data = data[1:]
        strBinary, data = data[:l], data[l:]
        assert strBinary.decode() != ""
        entries_count += 1
    assert entries_count == 2


def test_list_files(disk_conn):
    file1 = strToBytes("file1")
    file2 = strToBytes("file2")
    for file in [file1, file2]:
        disk_conn.send(CREATE_FILE + file)
        data = disk_conn.recv(1024)
        assert data == CREATE_FILE
    
    disk_conn.send(LIST_FILES + strToBytes(""))
    data = disk_conn.recv(1024)
    entries_count = 0
    while data[:1] != END_LIST_FILES:
        assert data[:1] == DATA_PACKET
        data = data[1:]
        l = bytesToNum(data)
        data = data[1:]
        strBinary, data = data[:l], data[l:]
        assert strBinary.decode() != ""
        entries_count += 1
    assert entries_count == 2


@pytest.mark.parametrize("command,path,is_error", [
    (MOVE_FILE, "file3", False),
    (REPLACE_FILE, "file3", False),
    (MOVE_FILE, "file2", True),
    (REPLACE_FILE, "file2", False), 
])
def test_move_replace_file(disk_conn, command, path, is_error):
    file1 = strToBytes("file1")
    file2 = strToBytes("file2")
    for file in [file1, file2]:
        disk_conn.send(CREATE_FILE + file)
        data = disk_conn.recv(1024)
        assert data == CREATE_FILE
    
    file3 = strToBytes(path)
    disk_conn.send(command + file1 + file3)
    data = disk_conn.recv(1024)
    if is_error:
        assert isErrorResp(data)
        return
    assert data == command

    for (check_path, exists) in [(file1, b"0"), (file3, b"1")]:
        disk_conn.send(EXISTS + check_path)
        data = disk_conn.recv(1024)
        assert data[:1] == EXISTS
        assert data[1:] == exists


@pytest.mark.parametrize("command,from_path,to_path", [
    (COPY, "dir1/file", "dir2/file"),
    # (COPY_DIRECTORY_CONTENT, "dir1", "dir2"), TODO: fix test
])
def test_copy(disk_conn, command, from_path, to_path):
    for dir in ["dir1", "dir2"]:
        disk_conn.send(CREATE_DIRECTORY + strToBytes(dir))
        data = disk_conn.recv(1024)
        assert data == CREATE_DIRECTORY
    
    file_path = strToBytes("dir1/file")
    disk_conn.send(CREATE_FILE + file_path)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE
    
    disk_conn.send(command + strToBytes(from_path) + strToBytes(to_path))
    data = disk_conn.recv(1024)
    assert data == command

    for file_path in ["dir1/file", "dir2/file"]:
        file_path = strToBytes(file_path)
        disk_conn.send(EXISTS + file_path)
        data = disk_conn.recv(1024)
        assert data[:1] == EXISTS
        assert data[1:] == b'1'
        # Remove file so test teardown cleared root dir
        disk_conn.send(REMOVE_FILE + file_path)
        data = disk_conn.recv(1024)
        assert data == REMOVE_FILE


@pytest.mark.parametrize("mode,file_name", [
    (RewriteMode, "test_file_0"),
    (AppendMode, "test_file_1"), 
])
def test_write_file(disk_conn, mode, file_name):
    file_name = strToBytes(file_name)
    disk_conn.send(CREATE_FILE + file_name)
    data = disk_conn.recv(1024)

    disk_conn.send(WRITE_FILE + file_name + b"\x0A" + mode)
    data = disk_conn.recv(1024)
    assert data == WRITE_FILE

    disk_conn.send(DATA_PACKET + strToBytes('1234567890'))
    data = disk_conn.recv(1024)
    assert data == DATA_PACKET
    disk_conn.send(DATA_PACKET + strToBytes('abcdefghij'))
    data = disk_conn.recv(1024)
    assert data == DATA_PACKET

    disk_conn.send(END_WRITE_FILE)
    data = disk_conn.recv(1024)
    assert data == END_WRITE_FILE

    disk_conn.send(GET_FILE_SIZE + file_name)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_FILE_SIZE
    assert bytesToNum(data[1:]) == 20


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

@pytest.mark.parametrize("command,path,is_error", [
    (REMOVE_FILE, "file", False),
    (REMOVE_FILE_IF_EXISTS, "file", False),
    (REMOVE_FILE, "file1", True),
    (REMOVE_FILE_IF_EXISTS, "file1", False),
    (REMOVE_RECURSIVE, "file", False), 
])
def test_remove_file(disk_conn, command, path, is_error):
    file = strToBytes("file")
    disk_conn.send(CREATE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE
    
    path = strToBytes(path)
    disk_conn.send(command + path)
    data = disk_conn.recv(1024)
    if is_error:
        assert isErrorResp(data)
        return
    assert data == command

    disk_conn.send(EXISTS + path)
    data = disk_conn.recv(1024)
    assert data[:1] == EXISTS
    assert data[1:] == b'0'


@pytest.mark.parametrize("command,path,is_error", [
    (REMOVE_DIRECTORY, "path_to/dir", False),
    (REMOVE_RECURSIVE, "path_to/dir", False), 
    (REMOVE_DIRECTORY, "path_to", True),
    (REMOVE_RECURSIVE, "path_to", False), 
])
def test_remove_directory(disk_conn, command, path, is_error):
    dirs = strToBytes("path_to/dir")
    disk_conn.send(CREATE_DIRECTORIES + dirs)
    data = disk_conn.recv(1024)
    assert data == CREATE_DIRECTORIES
    
    path = strToBytes(path)
    disk_conn.send(command + path)
    data = disk_conn.recv(1024)
    if is_error:
        assert isErrorResp(data)
        # Remove dir so test teardown cleared root dir
        disk_conn.send(REMOVE_DIRECTORY + dirs)
        data = disk_conn.recv(1024)
        assert data == REMOVE_DIRECTORY
        return
    assert data == command

    disk_conn.send(EXISTS + path)
    data = disk_conn.recv(1024)
    assert data[:1] == EXISTS
    assert data[1:] == b'0'


@pytest.mark.parametrize("modify", [
    (True),
    (False)
])
def test_get_last_modified(disk_conn, modify):
    file = strToBytes("file")
    disk_conn.send(CREATE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE
    
    disk_conn.send(GET_LAST_MODIFIED + file)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_LAST_MODIFIED
    last_modified = bytesToNum(data[1:])
    assert last_modified != 0

    sleep(1)
    if modify:
        disk_conn.send(WRITE_FILE + file + b"\x0A" + RewriteMode)
        data = disk_conn.recv(1024)
        assert data == WRITE_FILE

        disk_conn.send(DATA_PACKET + strToBytes('1234567890'))
        data = disk_conn.recv(1024)
        assert data == DATA_PACKET

        disk_conn.send(END_WRITE_FILE)
        data = disk_conn.recv(1024)
        assert data == END_WRITE_FILE
    
    disk_conn.send(GET_LAST_MODIFIED + file)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_LAST_MODIFIED
    new_last_modified = bytesToNum(data[1:])
    assert new_last_modified != 0

    if modify:
        assert new_last_modified > last_modified
    else:
        assert new_last_modified == last_modified


def test_set_last_modified(disk_conn):
    file = strToBytes("file")
    disk_conn.send(CREATE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE
    
    disk_conn.send(GET_LAST_MODIFIED + file)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_LAST_MODIFIED
    last_modified = bytesToNum(data[1:])
    assert last_modified != 0

    new_last_modified = last_modified + 5
    disk_conn.send(SET_LAST_MODIFIED + file + numToBytes(new_last_modified))
    data = disk_conn.recv(1024)
    assert data == SET_LAST_MODIFIED

    disk_conn.send(GET_LAST_MODIFIED + file)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_LAST_MODIFIED
    last_modified = bytesToNum(data[1:])
    assert last_modified != 0

    assert new_last_modified == last_modified


def test_create_hard_link(disk_conn):
    file = strToBytes("file")
    disk_conn.send(CREATE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE

    hard_link = strToBytes("hard_link")
    disk_conn.send(CREATE_HARD_LINK + file + hard_link)
    data = disk_conn.recv(1024)
    assert data == CREATE_HARD_LINK

    disk_conn.send(WRITE_FILE + file + b"\x0A" + RewriteMode)
    data = disk_conn.recv(1024)
    assert data == WRITE_FILE

    test_str = '1234567890'
    disk_conn.send(DATA_PACKET + strToBytes(test_str))
    data = disk_conn.recv(1024)
    assert data == DATA_PACKET

    disk_conn.send(END_WRITE_FILE)
    data = disk_conn.recv(1024)
    assert data == END_WRITE_FILE

    disk_conn.send(REMOVE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == REMOVE_FILE

    disk_conn.send(READ_FILE + hard_link + numToBytes(0) + numToBytes(10))
    data = disk_conn.recv(1024)
    assert data[:1] == READ_FILE
    assert bytesToStr(data[1:]) == test_str


@pytest.mark.parametrize("modify", [
    (True),
    (False)
])
def test_get_last_changed(disk_conn, modify):
    file = strToBytes("file")
    disk_conn.send(CREATE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE
    
    disk_conn.send(GET_LAST_CHANGED + file)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_LAST_CHANGED
    last_changed = bytesToNum(data[1:])
    assert last_changed != 0

    sleep(1)
    if modify:
        hard_link = strToBytes("hard_link")
        disk_conn.send(CREATE_HARD_LINK + file + hard_link)
        data = disk_conn.recv(1024)
        assert data == CREATE_HARD_LINK
    
    disk_conn.send(GET_LAST_CHANGED + file)
    data = disk_conn.recv(1024)
    assert data[:1] == GET_LAST_CHANGED
    new_last_changed = bytesToNum(data[1:])
    assert new_last_changed != 0

    if modify:
        assert new_last_changed > last_changed
    else:
        assert new_last_changed == last_changed


# TODO fix test 
# def test_set_read_only(disk_conn):
#     file = strToBytes("file")
#     disk_conn.send(CREATE_FILE + file)
#     data = disk_conn.recv(1024)
#     assert data == CREATE_FILE

#     disk_conn.send(SET_READ_ONLY + file)
#     data = disk_conn.recv(1024)
#     assert data == SET_READ_ONLY

#     disk_conn.send(WRITE_FILE + file + b"\x0A" + AppendMode)
#     data = disk_conn.recv(1024)
#     assert data == WRITE_FILE

#     test_str = '1234567890'
#     disk_conn.send(DATA_PACKET + strToBytes(test_str))
#     data = disk_conn.recv(1024)
#     assert data == DATA_PACKET

#     disk_conn.send(END_WRITE_FILE)
#     data = disk_conn.recv(1024)
#     assert isErrorResp(data)


def test_truncate(disk_conn):
    file = strToBytes("file")
    disk_conn.send(CREATE_FILE + file)
    data = disk_conn.recv(1024)
    assert data == CREATE_FILE

    disk_conn.send(WRITE_FILE + file + b"\x0A" + RewriteMode)
    data = disk_conn.recv(1024)
    assert data == WRITE_FILE

    test_str = '1234567890'
    disk_conn.send(DATA_PACKET + strToBytes(test_str))
    data = disk_conn.recv(1024)
    assert data == DATA_PACKET

    disk_conn.send(END_WRITE_FILE)
    data = disk_conn.recv(1024)
    assert data == END_WRITE_FILE

    disk_conn.send(TRUNCATE_FILE + file + numToBytes(5))
    data = disk_conn.recv(1024)
    assert data == TRUNCATE_FILE

    disk_conn.send(READ_FILE + file + numToBytes(0) + numToBytes(10))
    data = disk_conn.recv(1024)
    assert data[:1] == READ_FILE
    assert bytesToStr(data[1:]) == test_str[:5]