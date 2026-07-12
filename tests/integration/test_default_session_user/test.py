import socket
import struct
import urllib.parse
import urllib.request

import pymysql
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml", "configs/cluster.xml"],
    user_configs=["configs/users.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml", "configs/cluster.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def execute_query_http(port, query, user=None, headers=None):
    url = f"http://{node1.ip_address}:{port}/?query={urllib.parse.quote(query)}"
    if user is not None:
        url += f"&user={urllib.parse.quote(user)}"
    request = urllib.request.Request(url, headers=headers or {})
    response = urllib.request.urlopen(request, timeout=10).read()
    return response.decode("utf-8")


def write_varuint(num):
    result = b""
    while True:
        byte = num & 0x7F
        num >>= 7
        if num:
            result += bytes([byte | 0x80])
        else:
            result += bytes([byte])
            return result


def write_string(text):
    data = text.encode("utf-8")
    return write_varuint(len(data)) + data


def native_hello(port, user, password=""):
    """Send a native protocol Hello packet and return the type of the first
    packet of the response: 0 is ServerHello (authentication succeeded),
    2 is an Exception."""
    with socket.create_connection((node1.ip_address, port), timeout=10) as sock:
        packet = write_varuint(0)  # Hello
        packet += write_string("test-client")
        packet += write_varuint(26)  # version major
        packet += write_varuint(7)  # version minor
        packet += write_varuint(54449)  # protocol version
        packet += write_string("")  # database
        packet += write_string(user)
        packet += write_string(password)
        sock.sendall(packet)
        response = sock.recv(1)
        assert len(response) == 1
        return response[0]


def postgres_login(port, user):
    """Send a PostgreSQL startup message and return True if the server replied
    with AuthenticationOk."""
    with socket.create_connection((node1.ip_address, port), timeout=10) as sock:
        parameters = b"user\x00" + user.encode("utf-8") + b"\x00\x00"
        body = struct.pack("!I", 196608) + parameters  # protocol version 3.0
        sock.sendall(struct.pack("!I", len(body) + 4) + body)
        response = sock.recv(9)
        # 'R' + Int32 length (8) + Int32 authentication type (0 is AuthenticationOk)
        return response == b"R\x00\x00\x00\x08\x00\x00\x00\x00"


def assert_login_success(user, interface):
    node1.query("SYSTEM FLUSH LOGS session_log")
    assert (
        node1.query(
            f"SELECT count() > 0 FROM system.session_log "
            f"WHERE type = 'LoginSuccess' AND user = '{user}' AND interface = '{interface}'"
        )
        == "1\n"
    )


def test_http_global_default_session_user():
    assert execute_query_http(8123, "SELECT currentUser()") == "global_default_user\n"


def test_http_per_protocol_default_session_user():
    assert execute_query_http(8124, "SELECT currentUser()") == "proto_http_user\n"

    # An explicitly empty user parameter also means the default session user.
    assert execute_query_http(8124, "SELECT currentUser()", user="") == "proto_http_user\n"

    # An explicitly specified user is not affected.
    assert (
        execute_query_http(8124, "SELECT currentUser()", user="explicit_user")
        == "explicit_user\n"
    )

    # An empty user name in Basic credentials also means the default session user.
    assert (
        execute_query_http(
            8124, "SELECT currentUser()", headers={"Authorization": "Basic Og=="}  # ":"
        )
        == "proto_http_user\n"
    )


def test_native_default_session_user():
    hello = 0
    exception = 2

    # The global default session user on the ordinary port.
    assert native_hello(9000, "") == hello
    # A nonexistent user still fails.
    assert native_hello(9000, "nonexistent_user") == exception
    # The per-protocol default session user.
    assert native_hello(9101, "") == hello
    # A protocol without its own default session user uses the global one.
    assert native_hello(9102, "") == hello
    # An endpoint's default session user is found through the `impl` reference.
    assert native_hello(9103, "") == hello
    # An explicitly specified user is not affected.
    assert native_hello(9101, "explicit_user") == hello
    # An empty default session user prohibits connections without a user name.
    assert native_hello(9104, "") == exception

    for user in [
        "global_default_user",
        "proto_tcp_user",
        "proto_endpoint_user",
        "explicit_user",
    ]:
        assert_login_success(user, "TCP")


def test_mysql_default_session_user():
    connection = pymysql.connect(
        user="", password="", host=node1.ip_address, port=9106
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT currentUser()")
            assert cursor.fetchall() == (("proto_mysql_user",),)

    # An explicitly specified user is not affected.
    connection = pymysql.connect(
        user="explicit_user", password="", host=node1.ip_address, port=9106
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT currentUser()")
            assert cursor.fetchall() == (("explicit_user",),)


def test_postgres_default_session_user():
    assert postgres_login(9107, "")
    assert_login_success("proto_pg_user", "PostgreSQL")


def test_interserver_connections_do_not_use_default_session_user():
    # Interserver connections (the cluster has a secret) are authenticated by the
    # initial user, so remote queries must run as the initiating user, not as the
    # default session user.
    assert (
        node1.query(
            "SELECT hostName(), currentUser() FROM clusterAllReplicas('secret_cluster', system.one) ORDER BY hostName()"
        )
        == "node1\tdefault\nnode2\tdefault\n"
    )
