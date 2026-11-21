#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: enables the `local_connection_receive_packet_throw` fail point, which affects the whole server:
# a concurrent web terminal or SSH query of another test could consume the injected fault.
# Tag no-fasttest: tests that enable a fail point are kept out of the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=local_connection_receive_packet_throw
MARKER="webterminal_client_error_${CLICKHOUSE_DATABASE}"

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT $FP"' EXIT

python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" "${MARKER}" "${FP}" <<'PYEOF'
import base64
import json
import os
import socket
import struct
import sys
import time
import urllib.request

host, port, marker, fp = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed")
        buf += chunk
    return buf


def send_frame(sock, opcode, payload):
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    if len(payload) < 126:
        header = bytes([0x80 | opcode, 0x80 | len(payload)])
    else:
        header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack(">H", len(payload))
    sock.sendall(header + mask + masked)


def read_frame(sock):
    header = recv_exact(sock, 2)
    opcode = header[0] & 0x0F
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", recv_exact(sock, 8))[0]
    return opcode, recv_exact(sock, length)


def read_until(sock, needle, timeout, output=b""):
    """Accumulate the terminal output until it contains `needle`."""
    deadline = time.monotonic() + timeout
    while needle not in output:
        if time.monotonic() > deadline:
            raise TimeoutError(f"{needle!r} not found in {output[-500:]!r}")
        opcode, payload = read_frame(sock)
        if opcode == 0x08:
            raise EOFError(f"closed by the server: {payload!r}")
        if opcode == 0x09:
            send_frame(sock, 0x0A, payload)
        elif opcode == 0x02:
            output += payload
    return output


def read_until_closed(sock, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            opcode, payload = read_frame(sock)
        except EOFError:
            return
        if opcode == 0x08:
            return
        if opcode == 0x09:
            send_frame(sock, 0x0A, payload)
    raise TimeoutError("the session was not closed")


def http_query(query):
    with urllib.request.urlopen(f"http://{host}:{port}/", data=query.encode(), timeout=60) as reply:
        return reply.read().decode().strip()


sock = socket.create_connection((host, port), timeout=60)
key = base64.b64encode(os.urandom(16)).decode()
request = (
    f"GET /webterminal HTTP/1.1\r\n"
    f"Host: {host}:{port}\r\n"
    f"Origin: http://{host}:{port}\r\n"
    f"Upgrade: websocket\r\n"
    f"Connection: Upgrade\r\n"
    f"Sec-WebSocket-Key: {key}\r\n"
    f"Sec-WebSocket-Version: 13\r\n\r\n"
)
sock.sendall(request.encode())
response = b""
while b"\r\n\r\n" not in response:
    response += recv_exact(sock, 1)
assert response.startswith(b"HTTP/1.1 101"), response

send_frame(sock, 0x01, json.dumps({"type": "auth", "user": "default", "password": ""}).encode())
read_until(sock, b":) ", 60)

# The client fails while receiving the result of a registered query; then the session is idle at the prompt.
http_query(f"SYSTEM ENABLE FAILPOINT {fp}")
send_frame(sock, 0x02, f"SELECT 1 SETTINGS log_comment = '{marker}'\r".encode())
error = b"FAULT_INJECTED"
output = read_until(sock, error, 60)
read_until(sock, b":) ", 60, output[output.index(error):])

print(http_query(f"SELECT count() FROM system.processes WHERE Settings['log_comment'] = '{marker}'"))

send_frame(sock, 0x02, b"SELECT concat('session_', 'still_works')\r")
result = b"session_still_works"
output = read_until(sock, result, 60)
read_until(sock, b":) ", 60, output[output.index(result):])

send_frame(sock, 0x02, b"exit\r")
read_until_closed(sock, 60)
sock.close()
PYEOF
