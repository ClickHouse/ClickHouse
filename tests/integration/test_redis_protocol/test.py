import socket
import time

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/redis.xml"])

REDIS_PORT = 9006


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        prepare_table()
        yield cluster
    finally:
        cluster.shutdown()


def prepare_table():
    node.query(
        """
        DROP TABLE IF EXISTS kv_baseline;
        CREATE TABLE kv_baseline
        (
            key String,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key;

        INSERT INTO kv_baseline VALUES
            ('key_1', 'value_1'),
            ('key_2', 'value_2'),
            ('key_3', 'value_3');

        DROP TABLE IF EXISTS kv_uint64;
        CREATE TABLE kv_uint64
        (
            key UInt64,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key;

        INSERT INTO kv_uint64 VALUES
            (0, 'value_0'),
            (65536, 'value_65536'),
            (131072, 'value_131072');

        DROP TABLE IF EXISTS kv_not_key_value;
        CREATE TABLE kv_not_key_value
        (
            key String,
            value String
        )
        ENGINE = MergeTree
        ORDER BY key;

        INSERT INTO kv_not_key_value VALUES
            ('key_1', 'value_1');

        DROP TABLE IF EXISTS kv_uint32_key;
        CREATE TABLE kv_uint32_key
        (
            key UInt32,
            value String
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key;

        INSERT INTO kv_uint32_key VALUES
            (1, 'value_1');

        DROP TABLE IF EXISTS kv_uint64_value;
        CREATE TABLE kv_uint64_value
        (
            key UInt64,
            value UInt64
        )
        ENGINE = EmbeddedRocksDB
        PRIMARY KEY key;

        INSERT INTO kv_uint64_value VALUES
            (1, 10);
        """
    )


def encode_command(*parts):
    result = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        if isinstance(part, str):
            part = part.encode()
        result.append(f"${len(part)}\r\n".encode())
        result.append(part)
        result.append(b"\r\n")
    return b"".join(result)


def read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise AssertionError("connection closed while reading RESP payload")
        data += chunk
    return data


def read_line(sock):
    data = b""
    while not data.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise AssertionError("connection closed while reading RESP line")
        data += chunk
    return data[:-2]


def read_response(sock):
    prefix = sock.recv(1)
    if not prefix:
        raise AssertionError("connection closed while reading RESP type")

    if prefix == b"+":
        return ("simple", read_line(sock))

    if prefix == b"-":
        return ("error", read_line(sock))

    if prefix == b"$":
        size = int(read_line(sock))
        if size == -1:
            return ("bulk", None)

        value = read_exact(sock, size)
        if read_line(sock) != b"":
            raise AssertionError("bulk string is not terminated by CRLF")
        return ("bulk", value)

    if prefix == b"*":
        size = int(read_line(sock))
        return ("array", [read_response(sock) for _ in range(size)])

    raise AssertionError(f"unexpected RESP prefix: {prefix!r}")


def connect_redis(timeout=5):
    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((node.ip_address, REDIS_PORT))
            return sock
        except OSError as error:
            last_error = error
            sock.close()
            time.sleep(0.1)

    raise AssertionError(f"Redis-compatible endpoint did not become available: {last_error}")


def command(sock, *parts):
    sock.sendall(encode_command(*parts))
    return read_response(sock)


def assert_error(response, expected):
    assert response == ("error", expected)


def assert_is_error(response):
    assert response[0] == "error"


def test_ping(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "PING") == ("simple", b"PONG")


def test_select_valid_db(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "0") == ("simple", b"OK")


def test_select_invalid_db(started_cluster):
    with connect_redis() as sock:
        assert_error(
            command(sock, "SELECT", "999"),
            b"ERR Redis DB 999 is not configured",
        )


def test_select_invalid_index(started_cluster):
    with connect_redis() as sock:
        assert_error(command(sock, "SELECT", "abc"), b"ERR invalid DB index")


def test_get_existing_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "0") == ("simple", b"OK")
        assert command(sock, "GET", "key_1") == ("bulk", b"value_1")


def test_get_missing_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "0") == ("simple", b"OK")
        assert command(sock, "GET", "missing_key") == ("bulk", None)


def test_get_wrong_arity(started_cluster):
    with connect_redis() as sock:
        assert_error(
            command(sock, "GET"),
            b"ERR wrong number of arguments for 'get' command",
        )
        assert_error(
            command(sock, "GET", "key_1", "key_2"),
            b"ERR wrong number of arguments for 'get' command",
        )


def test_get_before_select(started_cluster):
    with connect_redis() as sock:
        assert_error(command(sock, "GET", "key_1"), b"ERR no Redis DB selected")


def test_mget_existing_keys(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "0") == ("simple", b"OK")
        assert command(sock, "MGET", "key_1", "key_2", "key_3") == (
            "array",
            [
                ("bulk", b"value_1"),
                ("bulk", b"value_2"),
                ("bulk", b"value_3"),
            ],
        )


def test_mget_mixed_existing_missing(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "0") == ("simple", b"OK")
        assert command(sock, "MGET", "key_1", "missing_key", "key_3") == (
            "array",
            [
                ("bulk", b"value_1"),
                ("bulk", None),
                ("bulk", b"value_3"),
            ],
        )


def test_mget_wrong_arity(started_cluster):
    with connect_redis() as sock:
        assert_error(
            command(sock, "MGET"),
            b"ERR wrong number of arguments for 'mget' command",
        )


def test_mget_before_select(started_cluster):
    with connect_redis() as sock:
        assert_error(command(sock, "MGET", "key_1"), b"ERR no Redis DB selected")


def test_uint64_get_existing_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert command(sock, "GET", "0") == ("bulk", b"value_0")


def test_uint64_get_another_existing_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert command(sock, "GET", "65536") == ("bulk", b"value_65536")


def test_uint64_get_missing_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert command(sock, "GET", "999999") == ("bulk", None)


def test_uint64_mget_existing_keys(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert command(sock, "MGET", "0", "65536", "131072") == (
            "array",
            [
                ("bulk", b"value_0"),
                ("bulk", b"value_65536"),
                ("bulk", b"value_131072"),
            ],
        )


def test_uint64_mget_mixed_existing_missing(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert command(sock, "MGET", "0", "999999", "131072") == (
            "array",
            [
                ("bulk", b"value_0"),
                ("bulk", None),
                ("bulk", b"value_131072"),
            ],
        )


def test_uint64_get_invalid_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert_is_error(command(sock, "GET", "abc"))
        assert_is_error(command(sock, "GET", "-1"))
        assert_is_error(command(sock, "GET", "18446744073709551616"))


def test_uint64_mget_invalid_key(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert_is_error(command(sock, "MGET", "0", "abc"))
        assert_is_error(command(sock, "MGET", "0", "-1"))
        assert_is_error(command(sock, "MGET", "0", "18446744073709551616"))


def test_uint64_server_survives_invalid_key_errors(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert_is_error(command(sock, "GET", "abc"))
        assert command(sock, "PING") == ("simple", b"PONG")
        assert command(sock, "SELECT", "1") == ("simple", b"OK")
        assert command(sock, "GET", "0") == ("bulk", b"value_0")


def test_get_non_key_value_table_error(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "2") == ("simple", b"OK")
        assert_is_error(command(sock, "GET", "key_1"))
        assert command(sock, "PING") == ("simple", b"PONG")


def test_get_unsupported_key_type_error(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "3") == ("simple", b"OK")
        assert_is_error(command(sock, "GET", "1"))
        assert command(sock, "PING") == ("simple", b"PONG")


def test_get_unsupported_value_type_error(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "4") == ("simple", b"OK")
        assert_is_error(command(sock, "GET", "1"))
        assert command(sock, "PING") == ("simple", b"PONG")


def test_mget_non_key_value_table_error(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "2") == ("simple", b"OK")
        assert_is_error(command(sock, "MGET", "key_1", "key_2"))
        assert command(sock, "PING") == ("simple", b"PONG")


def test_mget_unsupported_key_type_error(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "3") == ("simple", b"OK")
        assert_is_error(command(sock, "MGET", "1", "2"))
        assert command(sock, "PING") == ("simple", b"PONG")


def test_mget_unsupported_value_type_error(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "SELECT", "4") == ("simple", b"OK")
        assert_is_error(command(sock, "MGET", "1", "2"))
        assert command(sock, "PING") == ("simple", b"PONG")


def test_quit(started_cluster):
    with connect_redis() as sock:
        assert command(sock, "QUIT") == ("simple", b"OK")
        assert sock.recv(1) == b""


def test_server_survives_errors(started_cluster):
    with connect_redis() as sock:
        assert_error(command(sock, "UNKNOWN"), b"ERR unknown command 'UNKNOWN'")
        assert_error(command(sock, "SELECT", "abc"), b"ERR invalid DB index")
        assert_error(
            command(sock, "GET"),
            b"ERR wrong number of arguments for 'get' command",
        )
        assert_error(
            command(sock, "MGET"),
            b"ERR wrong number of arguments for 'mget' command",
        )

    assert node.query("SELECT 1") == "1\n"

    with connect_redis() as sock:
        assert command(sock, "PING") == ("simple", b"PONG")
