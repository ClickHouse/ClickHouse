#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A client allocates from sizes that the server puts on the wire, so a size on its own must not turn
# into an allocation: the strings of an exception received during the handshake are read as they
# arrive, the decompressed size of a block is bounded, and the row count of a block is not used to
# preallocate the columns.

# The decompressed size of a block is taken from its header, before the payload is read.
# `[checksum: 16][method: 1][size_compressed: 4][size_decompressed: 4]`, method `0x82` is LZ4.
printf '%s' "00000000000000000000000000000000821300000000000080" | xxd -r -p > "${CLICKHOUSE_TMP}/05052_huge_block.compressed"
echo -n 'block declaring a 2 GiB decompressed size: '
$CLICKHOUSE_COMPRESSOR --decompress --no-checksum-validation --input "${CLICKHOUSE_TMP}/05052_huge_block.compressed" --output "${CLICKHOUSE_TMP}/05052_huge_block.decompressed" 2>&1 | grep -o -m1 'Too large size_decompressed'

# A `Native` block with one `UInt64` column that declares a thousand billion rows and carries none.
printf '%s' "0180a094a58d1d01780655496e74363400" | xxd -r -p > "${CLICKHOUSE_TMP}/05052_huge_rows.native"
echo -n 'block declaring 1e12 rows: '
$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('${CLICKHOUSE_TMP}/05052_huge_rows.native', 'Native', 'x UInt64')" 2>&1 | grep -o -m1 -e 'TOO_LARGE_ARRAY_SIZE' -e 'MEMORY_LIMIT_EXCEEDED'

CLICKHOUSE_CLIENT_BINARY="$CLICKHOUSE_CLIENT_BINARY" python3 - <<'PYTHON'
import os
import shlex
import socket
import struct
import subprocess
import threading

client = shlex.split(os.environ["CLICKHOUSE_CLIENT_BINARY"])


def varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)


def string(value):
    return varint(len(value)) + value


def exception_packet(message, declared_message_size=None):
    """An `Exception` packet: type, code, name, message, stack trace, `has_nested`."""
    body = string(b"DB::Exception")
    if declared_message_size is None:
        body += string(message)
    else:
        body += varint(declared_message_size) + message
    body += string(b"") + b"\x00"
    return varint(2) + struct.pack("<i", 999) + body


def serve(sock, reply):
    connection, _ = sock.accept()
    with connection:
        connection.recv(4096)  # the `Hello` of the client
        connection.sendall(reply)


def run_client(reply):
    sock = socket.create_server(("127.0.0.1", 0))
    sock.settimeout(30)
    port = sock.getsockname()[1]
    thread = threading.Thread(target=serve, args=(sock, reply), daemon=True)
    thread.start()
    try:
        result = subprocess.run(
            client + ["--host", "127.0.0.1", "--port", str(port), "--query", "SELECT 1"],
            capture_output=True, timeout=60)
        return (result.stdout + result.stderr).decode("utf-8", "replace")
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    finally:
        thread.join(timeout=30)
        sock.close()


output = run_client(exception_packet(b"Mock server error 12345"))
print("exception from the server:", "reported" if "Mock server error 12345" in output else "UNEXPECTED " + repr(output[:200]))

# The same, but the message declares a gigabyte and the payload never arrives.
output = run_client(exception_packet(b"x" * 16, declared_message_size=1 << 30))
print("exception with a 1 GiB message that is not sent:", "reported" if "TIMEOUT" not in output else "TIMEOUT")
PYTHON
rm -f "${CLICKHOUSE_TMP}"/05052_huge_block.* "${CLICKHOUSE_TMP}"/05052_huge_rows.native
