import socket
import time
import re
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
)

HTTP_PORT = 8123


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def send_raw_http(host, port, request_bytes, timeout=5):
    """Send a raw HTTP request and return (status_code, body)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    sock.sendall(request_bytes)

    response = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"\r\n\r\n" in response:
                header_part, _, body_part = response.partition(b"\r\n\r\n")
                match = re.search(
                    rb"Content-Length:\s*(\d+)", header_part, re.IGNORECASE
                )
                if match:
                    content_length = int(match.group(1))
                    if len(body_part) >= content_length:
                        break
                else:
                    break
    except socket.timeout:
        pass
    sock.close()

    if not response:
        return 0, ""

    header_part, _, body_part = response.partition(b"\r\n\r\n")
    status_line = header_part.split(b"\r\n")[0].decode()
    parts = status_line.split(" ", 2)
    status_code = int(parts[1]) if len(parts) >= 2 else 0
    body = body_part.decode(errors="replace").strip()
    return status_code, body


def build_http_request(path="/?query=SELECT+1", headers=None):
    """Build a raw HTTP/1.1 request."""
    if headers is None:
        headers = {}
    lines = [f"POST {path} HTTP/1.1"]
    if "Host" not in headers:
        lines.append("Host: localhost")
    lines.append("Content-Length: 0")
    for name, value in headers.items():
        lines.append(f"{name}: {value}")
    lines.append("")
    lines.append("")
    return "\r\n".join(lines).encode()


# -- http_max_fields (configured to 10) --


def test_max_fields_within_limit(started_cluster):
    headers = {f"X-Test-{i}": "v" for i in range(5)}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 200


def test_max_fields_exceeds_limit(started_cluster):
    # 12 custom headers + Host + Content-Length = 14, exceeding limit of 10.
    headers = {f"X-Test-{i}": "v" for i in range(12)}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 400
    assert body == "Too many header fields"


# -- http_max_field_name_size (configured to 64) --


def test_field_name_within_limit(started_cluster):
    headers = {"X-" + "a" * 50: "value"}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 200


def test_field_name_exceeds_limit(started_cluster):
    headers = {"X-" + "a" * 100: "value"}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 400
    assert body == "Field name is too long"


# -- http_max_field_value_size (configured to 256) --


def test_field_value_within_limit(started_cluster):
    headers = {"X-Test": "v" * 200}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 200


def test_field_value_exceeds_limit(started_cluster):
    headers = {"X-Test": "v" * 300}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 400
    assert body == "Field value is too long"


# -- http_max_request_header_size (configured to 512 bytes total) --


def test_total_header_size_within_limit(started_cluster):
    headers = {f"X-T{i}": "v" * 30 for i in range(3)}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 200


def test_total_header_size_exceeds_limit(started_cluster):
    # 5 headers of ~200 bytes value each = ~1000 bytes total, exceeding 512 limit.
    # Each value is under the 256 per-field limit.
    headers = {f"X-Pad-{i}": "v" * 200 for i in range(5)}
    request = build_http_request(headers=headers)
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 400
    assert body == "Total request header size is too large"


# -- http_headers_read_timeout (configured to 3 seconds) --


def test_normal_request_within_timeout(started_cluster):
    request = build_http_request()
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 200


def test_slowloris_exceeds_timeout(started_cluster):
    """Trickling header data slower than the timeout should be rejected."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((node.ip_address, HTTP_PORT))

    sock.sendall(b"POST /?query=SELECT+1 HTTP/1.1\r\n")
    sock.sendall(b"Host: localhost\r\n")
    sock.sendall(b"Content-Length: 0\r\n")

    # Trickle one byte per 0.5 seconds — the 3-second timeout
    # should trigger before we finish sending this header.
    header_line = b"X-Slow: " + b"a" * 100 + b"\r\n"
    send_failed = False
    try:
        for byte in header_line:
            sock.sendall(bytes([byte]))
            time.sleep(0.5)
    except (BrokenPipeError, ConnectionResetError, OSError):
        send_failed = True

    if not send_failed:
        try:
            sock.sendall(b"\r\n")
        except (BrokenPipeError, ConnectionResetError, OSError):
            send_failed = True

    if not send_failed:
        response = b""
        try:
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
        except socket.timeout:
            pass

        if response:
            header_part, _, body_part = response.partition(b"\r\n\r\n")
            status_line = header_part.split(b"\r\n")[0].decode()
            parts = status_line.split(" ", 2)
            status = int(parts[1]) if len(parts) >= 2 else 0
            body = body_part.decode(errors="replace").strip()
            assert status == 400, f"Expected 400, got {status}"
            assert body == "Timeout exceeded while reading HTTP headers"
        # else: connection closed without response — also acceptable.
    # else: send failed with broken pipe — server closed the connection.

    sock.close()


# -- query-string overrides must not bypass pre-auth limits --


def test_max_fields_override_ignored(started_cluster):
    headers = {f"X-Test-{i}": "v" for i in range(12)}
    request = build_http_request(
        path="/?query=SELECT+1&http_max_fields=1000", headers=headers
    )
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 400
    assert body == "Too many header fields"


def test_max_request_header_size_override_ignored(started_cluster):
    headers = {f"X-Pad-{i}": "v" * 200 for i in range(5)}
    request = build_http_request(
        path="/?query=SELECT+1&http_max_request_header_size=10485760",
        headers=headers,
    )
    status, body = send_raw_http(node.ip_address, HTTP_PORT, request)
    assert status == 400
    assert body == "Total request header size is too large"
