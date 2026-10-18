#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The web terminal applies the same limits as other interfaces:
# `max_sessions_for_user` and `idle_connection_timeout` of the authenticated user.

USER_LIMITED="u_sessions_${CLICKHOUSE_DATABASE}"
USER_IDLE="u_idle_${CLICKHOUSE_DATABASE}"
USER_SET="u_set_${CLICKHOUSE_DATABASE}"
PROFILE_LIMITED="p_sessions_${CLICKHOUSE_DATABASE}"

# `max_sessions_for_user` can only be set in a settings profile.
${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_LIMITED}, ${USER_IDLE}, ${USER_SET};
    DROP SETTINGS PROFILE IF EXISTS ${PROFILE_LIMITED};
    CREATE SETTINGS PROFILE ${PROFILE_LIMITED} SETTINGS max_sessions_for_user = 1;
    CREATE USER ${USER_LIMITED} IDENTIFIED WITH plaintext_password BY 'pass' SETTINGS PROFILE ${PROFILE_LIMITED};
    CREATE USER ${USER_IDLE} IDENTIFIED WITH plaintext_password BY 'pass' SETTINGS idle_connection_timeout = 1;
    CREATE USER ${USER_SET} IDENTIFIED WITH plaintext_password BY 'pass' SETTINGS receive_timeout = 0;
"

python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" "${USER_LIMITED}" "${USER_IDLE}" "${USER_SET}" <<'PYEOF'
import base64
import json
import os
import socket
import struct
import sys

host, port, user_limited, user_idle, user_set = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed")
        buf += chunk
    return buf


def open_session(user):
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

    payload = json.dumps({"type": "auth", "user": user, "password": "pass"}).encode()
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(bytes([0x81, 0x80 | len(payload)]) + mask + masked)
    return sock


def read_frame(sock):
    header = recv_exact(sock, 2)
    opcode = header[0] & 0x0F
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", recv_exact(sock, 8))[0]
    return opcode, recv_exact(sock, length)


def send_text(sock, text):
    payload = text.encode()
    assert len(payload) < 126
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(bytes([0x82, 0x80 | len(payload)]) + mask + masked)


def read_until_close(sock):
    """Skip the terminal output and return the code and reason of the close frame."""
    while True:
        opcode, payload = read_frame(sock)
        if opcode == 0x08:
            return struct.unpack(">H", payload[:2])[0], payload[2:].decode()


# The first session of the user with `max_sessions_for_user = 1` is established.
first = open_session(user_limited)
opcode, _ = read_frame(first)
print("first session:", "established" if opcode == 0x02 else f"opcode {opcode}")

# The second simultaneous session is rejected.
second = open_session(user_limited)
print("second session:", *read_until_close(second))
second.close()
first.close()

# A session without any traffic is closed after `idle_connection_timeout`.
idle = open_session(user_idle)
print("idle session:", *read_until_close(idle))
idle.close()

# A query running longer than `idle_connection_timeout` is not interrupted,
# and the session is closed after it becomes idle at the prompt again.
# A large `interactive_delay` keeps the query silent: otherwise the progress and profile events
# repaint the terminal every 100 ms and the output alone would keep the session alive.
query = open_session(user_idle)
send_text(query, "SELECT concat('query_', 'finished'), sleep(2) SETTINGS interactive_delay = 10000000 FORMAT TSV\r")
output = b""
while b"query_finished" not in output:
    opcode, payload = read_frame(query)
    if opcode == 0x08:
        print("query session: closed during the query", struct.unpack(">H", payload[:2])[0], payload[2:].decode())
        sys.exit(1)
    output += payload
print("query session: finished")
print("query session:", *read_until_close(query))
query.close()

# `receive_timeout = 0` disables the timeout of reading a frame, and a `SET` typed in the
# terminal takes effect when the client returns to the prompt.
set_session = open_session(user_set)
send_text(set_session, "SET idle_connection_timeout = 1\r")
print("set session:", *read_until_close(set_session))
set_session.close()
PYEOF

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER_LIMITED}, ${USER_IDLE}, ${USER_SET}; DROP SETTINGS PROFILE ${PROFILE_LIMITED}"
