#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test is built with `ENABLE_LIBRARIES=0`, so it has no MySQL client.

# The value of a `MYSQL_TYPE_BIT` column used to be copied into an eight-byte stack local with
# `memcpy` using the length from the wire, without checking it. A `BIT` column holds at most
# 64 bits, but nothing bounded what the server actually sent, so a malicious or broken MySQL
# endpoint could overflow that local. The length is now validated.
#
# This starts a fake MySQL server that answers with a `BIT` column and returns, depending on the
# remote table name, a value of two, eight, or a hundred bytes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STUB="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_mysql_stub.py"

cat > "$STUB" <<'PYEOF'
#!/usr/bin/env python3
"""A fake MySQL server that answers every `SELECT` with a single `BIT` column.

The size of the returned value is chosen by the remote table name mentioned in the query,
so that one server can serve all the cases of the test.
"""

import socket
import struct
import sys
import threading

PORT = int(sys.argv[1])

# The value returned for a query, by the remote table name it mentions.
VALUES = {
    "bit_two": bytes([0x01, 0x02]),
    "bit_eight": bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
    "bit_oversized": b"A" * 100,
}

CLIENT_LONG_PASSWORD = 0x00000001
CLIENT_LONG_FLAG = 0x00000004
CLIENT_CONNECT_WITH_DB = 0x00000008
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_TRANSACTIONS = 0x00002000
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000

# Deliberately without `CLIENT_SSL`, `CLIENT_COMPRESS` and `CLIENT_DEPRECATE_EOF`, to keep the
# client on the plain, classic path.
CAPABILITIES = (
    CLIENT_LONG_PASSWORD
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
)

SERVER_STATUS_AUTOCOMMIT = 0x0002

MYSQL_TYPE_BIT = 0x10
CHARSET_BINARY = 63

COM_QUIT = 0x01
COM_QUERY = 0x03


def lenenc_int(value):
    if value < 0xFB:
        return bytes([value])
    if value < 1 << 16:
        return b"\xfc" + struct.pack("<H", value)
    if value < 1 << 24:
        return b"\xfd" + struct.pack("<I", value)[:3]
    return b"\xfe" + struct.pack("<Q", value)


def lenenc_str(data):
    return lenenc_int(len(data)) + data


def send_packet(sock, sequence_id, payload):
    header = struct.pack("<I", len(payload))[:3] + bytes([sequence_id & 0xFF])
    sock.sendall(header + payload)


def recv_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def read_packet(sock):
    """Read one wire packet, returning (sequence_id, payload), or None at end of stream."""
    header = recv_exact(sock, 4)
    if header is None:
        return None
    length = header[0] | (header[1] << 8) | (header[2] << 16)
    payload = recv_exact(sock, length)
    if payload is None:
        return None
    return header[3], payload


def handshake_packet():
    payload = bytearray()
    payload += b"\x0a"  # Protocol version.
    payload += b"8.0.0-fake\x00"
    payload += struct.pack("<I", 1)  # Connection id.
    payload += bytes(range(1, 9))  # First part of the scramble.
    payload += b"\x00"  # Filler.
    payload += struct.pack("<H", CAPABILITIES & 0xFFFF)
    payload += bytes([33])  # Character set `utf8_general_ci`.
    payload += struct.pack("<H", SERVER_STATUS_AUTOCOMMIT)
    payload += struct.pack("<H", (CAPABILITIES >> 16) & 0xFFFF)
    payload += bytes([21])  # Length of the whole scramble, including the terminating zero.
    payload += b"\x00" * 10  # Reserved.
    payload += bytes(range(9, 21)) + b"\x00"  # Second part of the scramble.
    payload += b"mysql_native_password\x00"
    return bytes(payload)


def ok_packet():
    return (
        b"\x00"
        + lenenc_int(0)  # Affected rows.
        + lenenc_int(0)  # Last insert id.
        + struct.pack("<H", SERVER_STATUS_AUTOCOMMIT)
        + struct.pack("<H", 0)  # Warnings.
    )


def eof_packet():
    return (
        b"\xfe"
        + struct.pack("<H", 0)  # Warnings.
        + struct.pack("<H", SERVER_STATUS_AUTOCOMMIT)
    )


def column_definition_packet(name, column_type, column_length):
    payload = bytearray()
    payload += lenenc_str(b"def")  # Catalog.
    payload += lenenc_str(b"")  # Schema.
    payload += lenenc_str(b"")  # Table.
    payload += lenenc_str(b"")  # Original table.
    payload += lenenc_str(name)
    payload += lenenc_str(name)  # Original name.
    payload += lenenc_int(0x0C)  # Length of the fixed-size part that follows.
    payload += struct.pack("<H", CHARSET_BINARY)
    payload += struct.pack("<I", column_length)
    payload += bytes([column_type])
    payload += struct.pack("<H", 0)  # Flags.
    payload += bytes([0])  # Decimals.
    payload += b"\x00\x00"  # Filler.
    return bytes(payload)


def send_bit_result_set(sock, sequence_id, value):
    send_packet(sock, sequence_id + 1, lenenc_int(1))  # One column.
    send_packet(sock, sequence_id + 2, column_definition_packet(b"x", MYSQL_TYPE_BIT, len(value)))
    send_packet(sock, sequence_id + 3, eof_packet())
    send_packet(sock, sequence_id + 4, lenenc_str(value))  # The single row.
    send_packet(sock, sequence_id + 5, eof_packet())


def handle_connection(sock):
    send_packet(sock, 0, handshake_packet())
    if read_packet(sock) is None:  # The handshake response; the credentials are not checked.
        return
    send_packet(sock, 2, ok_packet())

    while True:
        packet = read_packet(sock)
        if packet is None:
            return
        sequence_id, payload = packet
        if not payload:
            return
        command = payload[0]
        if command == COM_QUIT:
            return
        if command == COM_QUERY:
            query = payload[1:].decode("utf-8", errors="replace")
            value = next((v for name, v in VALUES.items() if name in query), None)
            if value is not None:
                send_bit_result_set(sock, sequence_id, value)
                continue
        # Everything else - `SET NAMES utf8mb4`, pings, and the like - just succeeds.
        send_packet(sock, sequence_id + 1, ok_packet())


def serve(sock):
    try:
        handle_connection(sock)
    except OSError:
        pass  # The client may hang up at any point, which is fine here.
    finally:
        sock.close()


def main():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", PORT))
    listener.listen(16)
    while True:
        conn, _ = listener.accept()
        threading.Thread(target=serve, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
PYEOF

PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 "$STUB" "$PORT" &
STUB_PID=$!
trap 'kill ${STUB_PID} 2>/dev/null; wait ${STUB_PID} 2>/dev/null; rm -f "${STUB}"' EXIT

# Wait for the fake server to start listening.
for _ in $(seq 1 100); do
    if python3 -c "
import socket, sys
sys.exit(0 if socket.socket().connect_ex(('127.0.0.1', ${PORT})) == 0 else 1)
" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS bit_two;
DROP TABLE IF EXISTS bit_eight;
DROP TABLE IF EXISTS bit_oversized;
CREATE TABLE bit_two (x UInt64) ENGINE = MySQL('127.0.0.1:${PORT}', 'db', 'bit_two', 'user', 'password');
CREATE TABLE bit_eight (x UInt64) ENGINE = MySQL('127.0.0.1:${PORT}', 'db', 'bit_eight', 'user', 'password');
CREATE TABLE bit_oversized (x UInt64) ENGINE = MySQL('127.0.0.1:${PORT}', 'db', 'bit_oversized', 'user', 'password');
"

# A `BIT` value is sent most significant byte first, and shorter values than eight bytes are normal.
echo "--- two bytes ---"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM bit_two"

echo "--- eight bytes ---"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM bit_eight"

# More than eight bytes cannot be a `BIT` value and must be rejected instead of overflowing.
# Before the fix this printed 4702111234474983745, the first eight of the hundred `A` bytes,
# having written the remaining ninety two past the end of the destination.
echo "--- hundred bytes ---"
ERROR=$(${CLICKHOUSE_CLIENT} -q "SELECT x FROM bit_oversized" 2>&1)
if grep -qF "INCORRECT_DATA" <<< "$ERROR" && grep -qE "MySQL sent 100 bytes" <<< "$ERROR"
then
    echo "OK: the oversized value was rejected"
else
    echo "FAIL: the oversized value was not rejected, got: $ERROR"
fi
