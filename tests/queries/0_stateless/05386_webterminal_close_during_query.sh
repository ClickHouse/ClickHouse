#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A web terminal session closed while a query is running must not leave the
# output buffers of the embedded client neither finalized nor canceled,
# and must not write the end of the result into the canceled output.

MARKER="webterminal_close_${CLICKHOUSE_DATABASE}"
START=$(${CLICKHOUSE_CLIENT} --query "SELECT toUnixTimestamp(now())")

# Runs the query in a web terminal session and closes the session
# the given number of seconds after the terminal output contains the needle.
function close_during()
{
python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" "$1" "$2" "$3" <<'PYEOF'
import base64
import json
import os
import socket
import struct
import sys
import time

host, port, query, needle, delay = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4].encode(), float(sys.argv[5])


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


def read_until(sock, needle, timeout):
    """Accumulate the terminal output until it contains `needle`."""
    deadline = time.monotonic() + timeout
    output = b""
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

send_frame(sock, 0x02, (query + "\r").encode())
read_until(sock, needle, 60)
time.sleep(delay)

# Close the connection abruptly while the query is running.
sock.close()
print("closed during the query")
PYEOF
}

close_during "SELECT count() FROM numbers(100) WHERE NOT ignore(sleepEachRow(0.5)) SETTINGS max_block_size = 1, log_comment = '${MARKER}'" "Progress:" 0
close_during "SELECT number, sleepEachRow(0.1) FROM numbers(100) SETTINGS max_block_size = 1, log_comment = '${MARKER}_json' FORMAT JSON" '"number"' 0.5

# The queries leave the process list once the sessions of the embedded client are torn down.
for _ in {1..60}; do
    running=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE Settings['log_comment'] IN ('${MARKER}', '${MARKER}_json')")
    [ "$running" = "0" ] && break
    sleep 1
done
echo "$running"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.text_log
    WHERE event_time >= toDateTime(${START})
        AND logger_name = 'WriteBuffer'
        AND message LIKE '%neither finalized nor canceled%'
        AND message LIKE '%ClientEmbeddedRunner%'"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.errors
    WHERE name = 'LOGICAL_ERROR'
        AND last_error_time >= toDateTime(${START})
        AND last_error_message LIKE '%Cannot write to canceled buffer%'"
