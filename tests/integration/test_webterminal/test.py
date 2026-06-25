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
# A second instance exercises the same endpoint behind an explicit
# `http_handlers` block (no `defaults`). This guards against regressions
# in the config-mode validator that, prior to this fix, only accepted
# `/js/uplot.js` and `/js/lz-string.js` for `handler_type = "js"` and so
# silently broke `/webterminal` for operators using explicit handler rules.
instance_http_handlers = cluster.add_instance(
    "node_http_handlers",
    main_configs=["configs/webterminal_http_handlers.xml"],
)
# A third instance exercises the operator-configured `Origin` allowlist
# (`webterminal_allowed_origins`). This guards the CSWSH protection on
# the WebSocket upgrade: a request with an allowed `Origin` must succeed,
# anything else must be rejected with `403` before `101`.
instance_allowed_origins = cluster.add_instance(
    "node_allowed_origins",
    main_configs=["configs/webterminal_allowed_origins.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _ws_handshake(sock, host, path="/webterminal", origin=None):
    """Send a WebSocket upgrade and read until the end of the response headers.

    `origin`, when set, is sent as the `Origin` request header. This exercises
    the `Origin` enforcement on the upgrade (same-origin by default, allowlist
    when `webterminal_allowed_origins` is configured).
    """
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    headers = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if origin is not None:
        headers.append(f"Origin: {origin}")
    request = "\r\n".join(headers) + "\r\n\r\n"
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


def _open_ws(host, port, origin=None):
    sock = socket.create_connection((host, port), timeout=10)
    # The server now requires a non-empty `Origin` on every upgrade (browser
    # WebSocket clients always send one). Default to a matching same-origin
    # header so existing tests exercise the happy path; tests that probe
    # `Origin` enforcement pass `origin=` explicitly.
    if origin is None:
        origin = f"http://{host}:{port}"
    response = _ws_handshake(sock, f"{host}:{port}", origin=origin)
    assert response.startswith(b"HTTP/1.1 101"), response
    return sock


def _attempt_ws(host, port, origin=None):
    """Like `_open_ws` but returns the raw status-line response without asserting `101`.

    Used by negative tests that expect the server to reject the upgrade
    with a non-`101` status (e.g. `403` for `Origin` violations).
    """
    sock = socket.create_connection((host, port), timeout=10)
    try:
        response = _ws_handshake(sock, f"{host}:{port}", origin=origin)
    finally:
        sock.close()
    return response


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
    """An auth frame with bad credentials must result in an explicit `1008` close frame.

    The handler catches the `Session::authenticate` exception and sends a
    deterministic policy-violation close (`1008`) so clients can distinguish
    login failure from network errors. We assert the close frame and its
    code strictly: an ungraceful EOF (`1006` to the browser) would be a
    regression of the explicit-close contract.
    """
    sock = _open_ws(instance.ip_address, 8123)
    try:
        _ws_send_text(
            sock,
            json.dumps({"type": "auth", "user": "default", "password": "definitely-wrong-password"}),
        )

        frame = _ws_read_frame(sock, timeout=15.0)
        assert frame is not None, (
            "Server closed the connection without an explicit close frame "
            "(client would see 1006, breaking the auth-failure contract)"
        )
        opcode, payload = frame
        assert opcode == 0x08, f"Expected close frame, got opcode={opcode:#x}"
        assert len(payload) >= 2, f"Close frame missing 2-byte status code, payload={payload!r}"
        code = struct.unpack(">H", payload[:2])[0]
        assert code == 1008, f"Expected close code 1008, got {code}"
    finally:
        sock.close()


def test_http_handlers_config_serves_webterminal_assets():
    """An explicit `http_handlers` block must be able to expose `/webterminal`
    together with the embedded xterm.js / xterm.css / addon assets.

    Before the fix, `handler_type = "js"` only accepted `/js/uplot.js` and
    `/js/lz-string.js`, so a deployment without the `<defaults/>` block
    could not serve the assets `webterminal.html` references and the
    terminal silently failed to initialize in the browser.
    """
    response = instance_http_handlers.http_request("webterminal", method="GET")
    assert response.status_code == 200, response.text
    assert b"xterm.min.js" in response.content

    # All assets referenced by `webterminal.html` must be reachable through
    # the same handler chain.
    js_assets = [
        ("js/xterm.min.js", "application/javascript"),
        ("js/xterm.min.css", "text/css"),
        ("js/addon-fit.min.js", "application/javascript"),
        ("js/addon-web-links.min.js", "application/javascript"),
    ]
    for path, expected_ct in js_assets:
        asset_response = instance_http_handlers.http_request(path, method="GET")
        assert asset_response.status_code == 200, f"{path} returned {asset_response.status_code}"
        content_type = asset_response.headers.get("content-type", "")
        assert expected_ct in content_type, f"{path}: unexpected content-type {content_type!r}"
        assert asset_response.content, f"{path}: empty body"



def test_origin_mismatch_rejected_same_origin():
    """A WebSocket upgrade with a foreign `Origin` must be rejected with `403` before `101`.

    By default (no `webterminal_allowed_origins`), the handler enforces
    same-origin: an `Origin` whose scheme + host differs from the request's
    `Host` is rejected. This is the browser-facing CSWSH protection.
    """
    response = _attempt_ws(instance.ip_address, 8123, origin="http://evil.example.com")
    assert response.startswith(b"HTTP/1.1 403"), response


def test_missing_origin_rejected():
    """A WebSocket upgrade without an `Origin` header must be rejected with `403`.

    Browsers always send `Origin` on a WebSocket upgrade, so a missing
    header is either a non-browser client or a forged upgrade attempt.
    The web terminal is an interactive PTY, so we reject unattributed
    upgrades rather than allow them through.
    """
    # _attempt_ws with origin=None sends no Origin header.
    response = _attempt_ws(instance.ip_address, 8123, origin=None)
    assert response.startswith(b"HTTP/1.1 403"), response


def test_origin_match_accepted_same_origin():
    """A WebSocket upgrade with a matching same-origin `Origin` must succeed.

    Locks in the default-allow path: a non-empty `Origin` whose scheme +
    host equals the request's `Host` must reach the `101` upgrade. Without
    this, a regression that rejects every non-empty origin (over-zealous
    CSWSH check) would still pass the negative tests above.
    """
    host = instance.ip_address
    sock = _open_ws(host, 8123, origin=f"http://{host}:8123")
    sock.close()


def test_allowed_origin_accepted_with_allowlist():
    """With `webterminal_allowed_origins` configured, an `Origin` in the list must succeed.

    This complements the same-origin negative test: when the operator opts
    into an explicit allowlist (typically because the deployment sits
    behind a TLS-terminating proxy that hides `https` from the server),
    listed origins must reach the `101` upgrade.
    """
    sock = _open_ws(
        instance_allowed_origins.ip_address, 8123, origin="https://example.com"
    )
    sock.close()


def test_disallowed_origin_rejected_with_allowlist():
    """With `webterminal_allowed_origins` configured, an `Origin` outside the list must be `403`.

    Symmetric to the positive case: the allowlist must actually exclude
    origins it doesn't name.
    """
    response = _attempt_ws(
        instance_allowed_origins.ip_address, 8123, origin="https://evil.example.com"
    )
    assert response.startswith(b"HTTP/1.1 403"), response


def test_oversized_preauth_frame_rejected_with_1009():
    """A pre-auth frame larger than the auth-stage cap (4 KiB) must trigger a `1009` close.

    Pre-auth, the server applies a tighter per-frame cap than the
    post-auth 16 MiB. The auth payload is tiny
    (`{"type":"auth","user":...,"password":...}`), so 4 KiB is enough
    for legitimate clients but rejects an unauthenticated peer that
    advertises a large payload to force an allocation.
    """
    sock = socket.create_connection((instance.ip_address, 8123), timeout=10)
    try:
        response = _ws_handshake(
            sock, f"{instance.ip_address}:8123", origin=f"http://{instance.ip_address}:8123"
        )
        assert response.startswith(b"HTTP/1.1 101"), response

        # Advertise a 5 KiB frame in the header without actually sending the body.
        oversize = 5 * 1024
        mask = secrets.token_bytes(4)
        header = bytearray()
        header.append(0x81)  # FIN | text opcode
        header.append(0x80 | 126)  # masked, extended 16-bit length follows
        header += struct.pack(">H", oversize)
        header += mask
        sock.sendall(bytes(header))

        frame = _ws_read_frame(sock, timeout=10.0)
        assert frame is not None, "Server did not send a close frame for oversized auth message"
        opcode, payload = frame
        assert opcode == 0x08, f"Expected close frame, got opcode={opcode:#x}"
        assert len(payload) >= 2, f"Close frame missing 2-byte status code, payload={payload!r}"
        code = struct.unpack(">H", payload[:2])[0]
        assert code == 1009, f"Expected close code 1009, got {code}"
    finally:
        sock.close()


def test_oversized_frame_rejected_with_1009():
    """A frame whose advertised payload length exceeds the server's per-frame
    cap (`MAX_FRAME_SIZE`, 16 MiB) must trigger an explicit `1009` close.

    The server inspects the extended length header before reading the payload,
    so the test only needs to send the 14-byte header advertising a huge
    length, not the megabytes themselves. Before the fix, this path returned
    `frame.valid = false` without flagging the oversize condition; the main
    loop then terminated the session silently, and the client saw the
    standard `1000 Session ended` close. The contract is now `1009` so
    clients/tests can distinguish oversized messages from a graceful exit.
    """
    sock = _open_ws(instance.ip_address, 8123)
    try:
        # Frame header: FIN | text opcode, masked, extended 8-byte length = 17 MiB.
        oversize = (16 * 1024 * 1024) + 1
        mask = secrets.token_bytes(4)
        header = bytearray()
        header.append(0x81)  # FIN | text opcode
        header.append(0x80 | 127)  # masked, extended 64-bit length follows
        header += struct.pack(">Q", oversize)
        header += mask
        sock.sendall(bytes(header))

        frame = _ws_read_frame(sock, timeout=10.0)
        assert frame is not None, "Server did not send a close frame for oversized message"
        opcode, payload = frame
        assert opcode == 0x08, f"Expected close frame, got opcode={opcode:#x}"
        assert len(payload) >= 2, f"Close frame missing 2-byte status code, payload={payload!r}"
        code = struct.unpack(">H", payload[:2])[0]
        assert code == 1009, f"Expected close code 1009, got {code}"
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
