"""Regression tests for the enabled `/webterminal` WebSocket endpoint.

These tests complement the stateless test `04141_webterminal_disabled.sh`,
which only covers the disabled-by-default gate. The endpoint is
security-sensitive (browser-facing, auth-in-band), so the enabled flow is
exercised here to guard against future regressions in the WebSocket
handshake and authentication paths.
"""

import base64
import json
import secrets
import socket
import struct

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    main_configs=["configs/webterminal.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _ws_handshake(sock, host, path="/webterminal"):
    """Send a WebSocket upgrade and read until the end of the response headers."""
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    )
    sock.sendall(request.encode())

    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
    return response


def _ws_send_text(sock, payload):
    """Send a single masked WebSocket text frame to the server."""
    data = payload.encode("utf-8")
    mask = secrets.token_bytes(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))

    header = bytearray()
    header.append(0x81)  # FIN | text opcode
    length = len(data)
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header += struct.pack(">H", length)
    else:
        header.append(0x80 | 127)
        header += struct.pack(">Q", length)
    header += mask
    sock.sendall(bytes(header) + masked)


def _ws_read_frame(sock, timeout=10.0):
    """Read one unmasked WebSocket frame from the server. Returns (opcode, payload) or None on EOF."""
    sock.settimeout(timeout)

    def _recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    header = _recv_exact(2)
    if header is None:
        return None
    opcode = header[0] & 0x0F
    masked = (header[1] & 0x80) != 0
    length = header[1] & 0x7F
    if length == 126:
        extra = _recv_exact(2)
        if extra is None:
            return None
        length = struct.unpack(">H", extra)[0]
    elif length == 127:
        extra = _recv_exact(8)
        if extra is None:
            return None
        length = struct.unpack(">Q", extra)[0]
    if masked:
        # Server-to-client frames must not be masked.
        return None
    payload = _recv_exact(length) if length > 0 else b""
    if payload is None and length > 0:
        return None
    return opcode, payload


def _open_ws(host, port):
    sock = socket.create_connection((host, port), timeout=10)
    response = _ws_handshake(sock, f"{host}:{port}")
    assert response.startswith(b"HTTP/1.1 101"), response
    return sock


def test_enabled_endpoint_serves_html():
    """When the experimental gate is open, plain `GET /webterminal` returns the HTML page."""
    response = instance.http_request("webterminal", method="GET")
    assert response.status_code == 200
    assert "ClickHouse" in response.text or "webterminal" in response.text.lower()


def test_successful_auth_handshake():
    """A valid auth frame on the enabled endpoint must establish a session.

    The embedded `clickhouse-client` writes its banner / prompt to the PTY,
    which the server forwards as a binary WebSocket frame. The presence of
    such a frame (rather than an immediate close) signals a successful
    handshake and authentication.
    """
    sock = _open_ws(instance.ip_address, 8123)
    try:
        _ws_send_text(sock, json.dumps({"type": "auth", "user": "default", "password": ""}))

        frame = _ws_read_frame(sock, timeout=15.0)
        assert frame is not None, "Server closed the connection without sending any frame"
        opcode, payload = frame
        # 0x02 = binary frame (PTY data forwarded from clickhouse-client).
        # 0x08 = close frame, which would indicate auth or protocol failure.
        assert opcode == 0x02, (
            f"Expected binary PTY data after successful auth, got opcode={opcode:#x} payload={payload!r}"
        )
        assert payload, "Expected non-empty PTY data after successful auth"
    finally:
        sock.close()


def test_invalid_auth_rejects_session():
    """An auth frame with bad credentials must not establish a session.

    The server's `Session::authenticate` throws on bad credentials; the
    handler propagates the exception and `SCOPE_EXIT` shuts down the socket.
    The client must observe either a close frame or an EOF, and crucially
    no PTY data (no session established).
    """
    sock = _open_ws(instance.ip_address, 8123)
    try:
        _ws_send_text(
            sock,
            json.dumps({"type": "auth", "user": "default", "password": "definitely-wrong-password"}),
        )

        frame = _ws_read_frame(sock, timeout=15.0)
        if frame is None:
            # EOF after the authenticate exception propagated.
            return
        opcode, _ = frame
        # Must not be a binary frame (no session must be established).
        assert opcode != 0x02, "Server forwarded PTY data despite invalid credentials"
        # A close frame is the only acceptable other outcome.
        assert opcode == 0x08, f"Unexpected opcode={opcode:#x} after invalid auth"
    finally:
        sock.close()


def test_non_auth_first_message_rejected():
    """A first frame that is valid JSON but not an auth message must trigger close 1008.

    This exercises the explicit close path in the handler when `parseAuthMessage`
    returns `false` for a well-formed but wrong-schema payload.
    """
    sock = _open_ws(instance.ip_address, 8123)
    try:
        # Valid JSON, but `type` is not `auth` — handler sends close 1008.
        _ws_send_text(sock, json.dumps({"type": "resize", "cols": 80, "rows": 24}))

        frame = _ws_read_frame(sock, timeout=10.0)
        assert frame is not None, "Server did not respond to non-auth first message"
        opcode, payload = frame
        assert opcode == 0x08, f"Expected close frame, got opcode={opcode:#x}"
        # Close payload is 2-byte code + reason; code 1008 = policy violation.
        if len(payload) >= 2:
            code = struct.unpack(">H", payload[:2])[0]
            assert code == 1008, f"Expected close code 1008, got {code}"
    finally:
        sock.close()
