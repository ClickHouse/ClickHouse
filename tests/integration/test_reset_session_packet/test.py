import os
import socket
import struct
import sys
import time

import pytest

from helpers.cluster import ClickHouseCluster

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "../../queries/0_stateless/helpers"
    ),
)
from native_session_client import (
    ConnectionClosed,
    NativeSessionClient,
    ServerError,
    expect_error,
    write_string,
    write_varuint,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"], user_configs=["configs/users.xml"]
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def connect(user="default", password=""):
    return NativeSessionClient(
        host=node.ip_address,
        port=9000,
        database="default",
        user=user,
        password=password,
    )


def test_authentication_expiry_survives_reset():
    # Use the server clock and leave enough time for login on sanitizer builds.
    deadline = int(node.query("SELECT toUnixTimestamp(now()) + 15").strip())
    expiry = node.query(f"SELECT toString(toDateTime({deadline}, 'UTC'))").strip()
    node.query(
        f"CREATE USER reset_expiring IDENTIFIED WITH plaintext_password BY 'short' VALID UNTIL '{expiry} UTC', plaintext_password BY 'lasting'"
    )
    try:
        with (
            connect("reset_expiring", "short") as expiring,
            connect("reset_expiring", "lasting") as lasting,
        ):
            expiring.reset_session()
            assert expiring.value("SELECT 'before expiry'") == "before expiry"
            # A reset must retain the original method's deadline, even if the administrator
            # extends it for future logins. Both reset and subsequent queries must enforce it.
            node.query(
                "ALTER USER reset_expiring VALID UNTIL '2999-01-01 00:00:00 UTC'"
            )
            expiring.reset_session()
            limit = time.monotonic() + 60
            while int(node.query("SELECT toUnixTimestamp(now())").strip()) <= deadline:
                assert time.monotonic() < limit, (
                    "server clock did not reach credential expiry"
                )
                time.sleep(0.1)
            expect_error("USER_EXPIRED", expiring.reset_session)
            with pytest.raises((ConnectionClosed, OSError)):
                expiring.ping()
            lasting.reset_session()
            assert lasting.value("SELECT 'still valid'") == "still valid"
        with connect("reset_expiring", "short") as fresh:
            fresh.reset_session()
    finally:
        node.query("DROP USER reset_expiring")


def test_session_limit_survives_reset():
    node.query("CREATE USER reset_one_session SETTINGS PROFILE reset_one_session")
    try:
        with connect("reset_one_session") as first:
            for _ in range(3):
                first.reset_session()
                second = connect("reset_one_session")
                try:
                    expect_error("USER_SESSION_LIMIT_EXCEEDED", second.connect)
                finally:
                    second.close()
                assert first.value("SELECT 'usable'") == "usable"
        # Socket close is processed asynchronously; wait for the tracker handle to be released.
        deadline = time.monotonic() + 10
        while True:
            try:
                with connect("reset_one_session") as fresh:
                    fresh.reset_session()
                break
            except ServerError as error:
                name = node.query(f"SELECT errorCodeToName({error.code})").strip()
                if (
                    name != "USER_SESSION_LIMIT_EXCEEDED"
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.1)
    finally:
        node.query("DROP USER reset_one_session")


@pytest.mark.parametrize("send_chunked", [False, True])
@pytest.mark.parametrize("receive_chunked", [False, True])
def test_reset_with_older_client_and_chunked_framing(send_chunked, receive_chunked):
    database = f"reset_framing_{int(send_chunked)}_{int(receive_chunked)}"
    node.query(f"CREATE DATABASE {database}")
    client = connect()
    client.sock = socket.create_connection((node.ip_address, 9000), timeout=10)
    try:
        hello = bytearray([0])
        write_string("reset framing test", hello)
        for value in (
            1,
            0,
            54470,
        ):  # Predates `ResetSession`, but supports chunk negotiation.
            write_varuint(value, hello)
        for value in (database, "default", ""):
            write_string(value, hello)
        client.sock.sendall(hello)
        assert client.read_varuint() == 0
        client.read_string()  # name
        client.read_varuint()  # major
        client.read_varuint()  # minor
        client.read_varuint()  # server revision
        client.read_string()  # timezone
        client.read_string()  # display name
        client.read_varuint()  # patch
        assert client.read_string().endswith("_optional")
        assert client.read_string().endswith("_optional")
        for _ in range(client.read_varuint()):
            client.read_string()  # password rule
            client.read_string()  # description
        client.read_bytes(8)  # interserver nonce
        addendum = bytearray()
        write_string("", addendum)  # quota key
        write_string("chunked" if send_chunked else "notchunked", addendum)
        write_string("chunked" if receive_chunked else "notchunked", addendum)
        client.sock.sendall(addendum)

        def exchange(packet, expected):
            payload = bytes([packet])
            if send_chunked:
                payload = struct.pack("<I", len(payload)) + payload + bytes(4)
            client.sock.sendall(payload)
            if receive_chunked:
                response = bytearray()
                while True:
                    size = client.read_uint(4)
                    if not size:
                        break
                    response.extend(client.read_bytes(size))
                assert response[0] == expected, response
                if expected != 2:
                    assert response == bytes([expected])
                else:
                    assert b"does not exist" in response
            else:
                assert client.read_varuint() == expected
                if expected == 2:
                    error = client.read_exception()
                    assert (
                        node.query(f"SELECT errorCodeToName({error.code})").strip()
                        == "UNKNOWN_DATABASE"
                    )

        for packet, reply in ((4, 4), (15, 5), (15, 5), (4, 4)):
            exchange(packet, reply)
        node.query(f"DROP DATABASE {database}")
        exchange(15, 2)
        exchange(
            4, 4
        )  # The exception must finish its chunk and keep the connection usable.
        node.query(f"CREATE DATABASE {database}")
        exchange(15, 5)
    finally:
        client.close()
        node.query(f"DROP DATABASE IF EXISTS {database}")
