import base64
import json
import os
import secrets
import socket
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager

import grpc
import pyarrow.flight
import pymysql
import pytest

from helpers.cluster import ClickHouseCluster

script_dir = os.path.dirname(os.path.realpath(__file__))
grpc_protocol_pb2_dir = os.path.join(script_dir, "grpc_protocol_pb2")
if grpc_protocol_pb2_dir not in sys.path:
    sys.path.append(grpc_protocol_pb2_dir)
import clickhouse_grpc_pb2  # Execute pb2/generate.py from test_grpc_protocol to regenerate these modules.
import clickhouse_grpc_pb2_grpc

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
# A node with an empty global `default_session_user`: connections without a user name are
# rejected on every interface. Arrow Flight and gRPC are not composable protocols, so their
# reject mode can only be enabled globally, which needs a separate node.
node_reject = cluster.add_instance(
    "node_reject",
    main_configs=["configs/config_reject.xml"],
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


def recv_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            break
        data += chunk
    return data


def postgres_login(port, user, password=""):
    """Log in over the PostgreSQL wire protocol and return True if the server
    accepted the credentials. Users with a plaintext password (even an empty
    one) are asked for it with AuthenticationCleartextPassword first."""
    with socket.create_connection((node1.ip_address, port), timeout=10) as sock:
        parameters = b"user\x00" + user.encode("utf-8") + b"\x00\x00"
        body = struct.pack("!I", 196608) + parameters  # protocol version 3.0
        sock.sendall(struct.pack("!I", len(body) + 4) + body)

        # 'R' + Int32 length (8) + Int32 authentication type
        response = recv_exact(sock, 9)
        if response[:1] != b"R":
            return False
        auth_type = struct.unpack("!I", response[5:9])[0]
        if auth_type == 3:  # AuthenticationCleartextPassword
            password_bytes = password.encode("utf-8") + b"\x00"
            sock.sendall(b"p" + struct.pack("!I", len(password_bytes) + 4) + password_bytes)
            response = recv_exact(sock, 9)
            if response[:1] != b"R":
                return False
            auth_type = struct.unpack("!I", response[5:9])[0]
        return auth_type == 0  # AuthenticationOk


def login_success_count(user, interface):
    node1.query("SYSTEM FLUSH LOGS session_log")
    return int(node1.query(f"SELECT count() FROM system.session_log WHERE type = 'LoginSuccess' AND user = '{user}' AND interface = '{interface}'"))


@contextmanager
def assert_login_success(user, interface):
    """Assert that the wrapped action produced a new `LoginSuccess` row in
    `system.session_log`, comparing the count of matching rows before and after,
    so that a row left by an earlier login of the same user on the same interface
    cannot satisfy the assertion."""
    count_before = login_success_count(user, interface)
    yield
    assert login_success_count(user, interface) > count_before


def test_http_global_default_session_user():
    assert execute_query_http(8123, "SELECT currentUser()") == "global_default_user\n"


def test_http_per_protocol_default_session_user():
    assert execute_query_http(8124, "SELECT currentUser()") == "proto_http_user\n"

    # An explicitly empty user parameter also means the default session user.
    assert execute_query_http(8124, "SELECT currentUser()", user="") == "proto_http_user\n"

    # An explicitly specified user is not affected.
    assert execute_query_http(8124, "SELECT currentUser()", user="explicit_user") == "explicit_user\n"

    # An empty user name in Basic credentials also means the default session user.
    assert (
        execute_query_http(
            8124,
            "SELECT currentUser()",
            headers={"Authorization": "Basic Og=="},  # ":"
        )
        == "proto_http_user\n"
    )


def test_fixed_user_handler_with_anonymous_logins_disabled():
    # A handler with a fixed user (`handler.user` of an `http_handlers` rule)
    # authenticates as the configured user regardless of the request, so a request
    # without a user name is not resolved through the default session user: an empty
    # `default_session_user`, which prohibits anonymous logins, must not reject
    # fixed-user handlers.
    url = f"http://{node1.ip_address}:8129/fixed"
    with assert_login_success("fixed_handler_user", "HTTP"):
        response = urllib.request.urlopen(url, timeout=10).read()
    assert response == b"fixed_handler_user\n"

    # The prohibition still applies to the default handlers on the same endpoint.
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        execute_query_http(8129, "SELECT currentUser()")
    assert exc_info.value.code == 403


def grpc_query(query, user_name=None):
    channel = grpc.insecure_channel(f"{node1.ip_address}:9100")
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
        query_info = clickhouse_grpc_pb2.QueryInfo(query=query)
        if user_name is not None:
            query_info.user_name = user_name
        result = stub.ExecuteQuery(query_info)
        assert not result.HasField("exception"), result.exception.display_text
        return result.output.decode("utf-8")
    finally:
        channel.close()


def test_grpc_default_session_user():
    # gRPC is not a composable protocol, so only the global setting applies to it:
    # a query without a user name runs as the global default session user.
    with assert_login_success("global_default_user", "gRPC"):
        assert grpc_query("SELECT currentUser()") == "global_default_user\n"

    # An explicitly specified user is not affected.
    with assert_login_success("explicit_user", "gRPC"):
        assert grpc_query("SELECT currentUser()", user_name="explicit_user") == "explicit_user\n"


def arrowflight_query(node, query, authorization=None):
    """Run a query over Arrow Flight and return the single result value.
    `authorization` is the raw value of the `authorization` header,
    or None to send no credentials at all."""
    client = pyarrow.flight.FlightClient(f"grpc+tcp://{node.ip_address}:9110")
    try:
        options = None
        if authorization is not None:
            options = pyarrow.flight.FlightCallOptions(headers=[(b"authorization", authorization)])
        ticket = pyarrow.flight.Ticket(query.encode("utf-8"))
        table = client.do_get(ticket, options).read_all()
        return table.column(0)[0].as_py()
    finally:
        client.close()


def basic_authorization(user, password=""):
    return b"Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8"))


def test_arrowflight_default_session_user():
    # Arrow Flight is not a composable protocol, so only the global setting applies to it:
    # a call without an `authorization` header runs as the global default session user.
    assert arrowflight_query(node1, "SELECT currentUser()") == "global_default_user"

    # Basic credentials with an empty user name mean the default session user as well.
    assert arrowflight_query(node1, "SELECT currentUser()", basic_authorization("")) == "global_default_user"

    # An explicitly specified user is not affected.
    assert arrowflight_query(node1, "SELECT currentUser()", basic_authorization("explicit_user")) == "explicit_user"


def test_arrowflight_anonymous_logins_disabled():
    # An empty global default session user prohibits Arrow Flight calls without a user
    # name: both without the `authorization` header and with Basic credentials with an
    # empty user name.
    for authorization in [None, basic_authorization("")]:
        with pytest.raises(pyarrow.flight.FlightUnauthenticatedError, match="default_session_user"):
            arrowflight_query(node_reject, "SELECT 1", authorization)

    # An explicitly specified user works as usual.
    assert arrowflight_query(node_reject, "SELECT currentUser()", basic_authorization("explicit_user")) == "explicit_user"


def test_native_default_session_user():
    hello = 0
    exception = 2

    # The global default session user on the ordinary port.
    with assert_login_success("global_default_user", "TCP"):
        assert native_hello(9000, "") == hello
    # A nonexistent user still fails.
    assert native_hello(9000, "nonexistent_user") == exception
    # The per-protocol default session user.
    with assert_login_success("proto_tcp_user", "TCP"):
        assert native_hello(9101, "") == hello
    # A protocol without its own default session user uses the global one.
    with assert_login_success("global_default_user", "TCP"):
        assert native_hello(9102, "") == hello
    # An endpoint's default session user is found through the `impl` reference.
    with assert_login_success("proto_endpoint_user", "TCP"):
        assert native_hello(9103, "") == hello
    # An explicitly specified user is not affected.
    with assert_login_success("explicit_user", "TCP"):
        assert native_hello(9101, "explicit_user") == hello
    # An empty default session user prohibits connections without a user name.
    assert native_hello(9104, "") == exception


def test_mysql_default_session_user():
    # pymysql substitutes the OS user name for an empty user name on the client
    # side, so trick it into sending a genuinely empty user name.
    connection = pymysql.connect(
        user="placeholder",
        password="",
        host=node1.ip_address,
        port=9106,
        defer_connect=True,
    )
    connection.user = b""
    connection.connect()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT currentUser()")
            assert cursor.fetchall() == (("proto_mysql_user",),)

    # An explicitly specified user is not affected.
    connection = pymysql.connect(user="explicit_user", password="", host=node1.ip_address, port=9106)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT currentUser()")
            assert cursor.fetchall() == (("explicit_user",),)


def test_postgres_default_session_user():
    with assert_login_success("proto_pg_user", "PostgreSQL"):
        assert postgres_login(9107, "")


def test_interserver_connections_do_not_use_default_session_user():
    # Interserver connections (the cluster has a secret) are authenticated by the
    # initial user, so remote queries must run as the initiating user, not as the
    # default session user.
    assert node1.query("SELECT hostName(), currentUser() FROM clusterAllReplicas('secret_cluster', system.one) ORDER BY hostName()") == "node1\tdefault\nnode2\tdefault\n"


def ws_handshake(sock, host, origin):
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    headers = [
        "GET /webterminal HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
        f"Origin: {origin}",
    ]
    sock.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
    return response


def ws_send_text(sock, payload):
    data = payload.encode("utf-8")
    mask = secrets.token_bytes(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    header = bytearray([0x81])  # FIN | text opcode
    length = len(data)
    if length < 126:
        header.append(0x80 | length)
    else:
        header.append(0x80 | 126)
        header += struct.pack(">H", length)
    header += mask
    sock.sendall(bytes(header) + masked)


def ws_read_opcode(sock, timeout=15.0):
    """Read one WebSocket frame and return its opcode (0x02 = binary PTY data on
    a successful session, 0x08 = close on failure), or None on EOF."""
    sock.settimeout(timeout)

    def recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    header = recv_exact(2)
    if header is None:
        return None
    opcode = header[0] & 0x0F
    length = header[1] & 0x7F
    if length == 126:
        extra = recv_exact(2)
        length = struct.unpack(">H", extra)[0] if extra else 0
    elif length == 127:
        extra = recv_exact(8)
        length = struct.unpack(">Q", extra)[0] if extra else 0
    if length > 0:
        recv_exact(length)
    return opcode


def webterminal_auth_opcode(port, auth_message):
    host = f"{node1.ip_address}:{port}"
    sock = socket.create_connection((node1.ip_address, port), timeout=10)
    try:
        response = ws_handshake(sock, host, origin=f"http://{host}")
        assert response.startswith(b"HTTP/1.1 101"), response
        ws_send_text(sock, auth_message)
        return ws_read_opcode(sock)
    finally:
        sock.close()


def test_webterminal_default_session_user():
    # A web terminal auth message without a "user" field falls back to the
    # endpoint's default session user (here a per-endpoint override), proving the
    # override reaches `WebTerminalRequestHandler` and is not the global default.
    # A successful session forwards PTY data as a binary frame (0x02); a failure
    # would send a close frame (0x08).
    with assert_login_success("proto_webterminal_user", "HTTP"):
        opcode = webterminal_auth_opcode(8125, json.dumps({"type": "auth", "password": ""}))
        assert opcode == 0x02, f"Expected PTY data after successful auth, got opcode={opcode}"

    # An explicitly specified user is not affected by the default session user.
    with assert_login_success("explicit_user", "HTTP"):
        opcode = webterminal_auth_opcode(8125, json.dumps({"type": "auth", "user": "explicit_user", "password": ""}))
        assert opcode == 0x02, f"Expected PTY data after successful auth, got opcode={opcode}"


def test_custom_webterminal_rule_default_session_user():
    # The web terminal can also be exposed through a custom `http_handlers` section
    # (a `rule` with `handler.type = webterminal`). Such a composable HTTP endpoint
    # must still honor the endpoint's own `default_session_user` override, not the
    # global setting. An auth message without a "user" field must therefore log in
    # as the endpoint user.
    with assert_login_success("proto_custom_webterminal_user", "HTTP"):
        opcode = webterminal_auth_opcode(8128, json.dumps({"type": "auth", "password": ""}))
        assert opcode == 0x02, f"Expected PTY data after successful auth, got opcode={opcode}"

    # An explicitly specified user is not affected by the default session user.
    with assert_login_success("explicit_user", "HTTP"):
        opcode = webterminal_auth_opcode(8128, json.dumps({"type": "auth", "user": "explicit_user", "password": ""}))
        assert opcode == 0x02, f"Expected PTY data after successful auth, got opcode={opcode}"


def scrape_prometheus_status(port):
    """GET /metrics on a prometheus listener and return the HTTP status code."""
    url = f"http://{node1.ip_address}:{port}/metrics"
    try:
        response = urllib.request.urlopen(url, timeout=10)
        return response.getcode()
    except urllib.error.HTTPError as e:
        return e.code


def test_prometheus_keeper_metrics_default_session_user():
    # A composable `type = prometheus` listener served through the keeper-metrics-only
    # factory (`prometheus_keeper_metrics_only`) exposes only the `Metrics` protocol,
    # which is served without authentication (`MetricsImpl` does not create a session,
    # unlike the write/read/query protocols). The `default_session_user` override is
    # threaded into `createKeeperPrometheusHandlerFactory` for consistency with the
    # regular prometheus factory, but it has no effect on metrics exposition: a scrape
    # succeeds anonymously regardless of the endpoint's `default_session_user`, and an
    # empty override does not turn metrics scraping into an authenticated endpoint.
    assert scrape_prometheus_status(9108) == 200
    assert scrape_prometheus_status(9109) == 200


def test_config_reload_default_session_user():
    config_path = "/etc/clickhouse-server/config.d/config.xml"

    # The effective default session user comes from the endpoint itself ...
    assert execute_query_http(8126, "SELECT currentUser()") == "reload_effective_before\n"
    # ... and, for an endpoint that references a base via `impl`, the value closest
    # to the endpoint wins (the base's value is shadowed).
    assert execute_query_http(8127, "SELECT currentUser()") == "reload_shadow_endpoint_user\n"

    # In a single reload, change one endpoint's own (effective) value and a base
    # value that a closer module shadows (so the shadow endpoint's effective value
    # does not change).
    node1.replace_in_config(config_path, "reload_effective_before", "reload_effective_after")
    node1.replace_in_config(config_path, "reload_shadow_base_before", "reload_shadow_base_after")
    node1.query("SYSTEM RELOAD CONFIG")

    # The endpoint whose effective value changed is restarted and now serves the
    # new user (the value is baked into the handler factory, so a restart is
    # required for it to take effect).
    for _ in range(30):
        try:
            if execute_query_http(8126, "SELECT currentUser()") == "reload_effective_after\n":
                break
        except Exception:
            pass
        time.sleep(1)
    assert execute_query_http(8126, "SELECT currentUser()") == "reload_effective_after\n"

    # The shadow endpoint's effective value is unchanged, so it keeps serving the
    # same user.
    assert execute_query_http(8127, "SELECT currentUser()") == "reload_shadow_endpoint_user\n"

    # The endpoint with a changed effective value was reloaded ...
    for _ in range(30):
        if node1.contains_in_log("<default_session_user> had been changed, will reload http-reload-effective"):
            break
        time.sleep(1)
    assert node1.contains_in_log("<default_session_user> had been changed, will reload http-reload-effective")
    # ... but the shadow endpoint was not reloaded for `default_session_user`,
    # because only a shadowed base changed and its effective value is the same.
    assert not node1.contains_in_log("<default_session_user> had been changed, will reload http-reload-shadow-endpoint")
