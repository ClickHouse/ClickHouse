#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

python3 - <<'PY'
import os
import shlex
import socket
import struct
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack


TIMEOUT_SECONDS = 10


def encode_varuint(value):
    encoded = bytearray()
    while value > 127:
        encoded.append((value & 127) | 128)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def encode_string(value):
    encoded = value.encode()
    return encode_varuint(len(encoded)) + encoded


def read_varuint(stream):
    value = 0
    shift = 0
    while True:
        encoded = stream.read(1)
        if not encoded:
            raise EOFError("Client disconnected before sending a query")
        value |= (encoded[0] & 127) << shift
        if encoded[0] < 128:
            return value
        shift += 7


def read_string(stream):
    size = read_varuint(stream)
    if len(stream.read(size)) != size:
        raise EOFError("Incomplete client Hello")


def serve_connection(connections, listener, time_zone, response, progress):
    progress.append(f"{time_zone!r}: waiting for a TCP connection")
    connection = connections.enter_context(listener.accept()[0])
    connection.settimeout(TIMEOUT_SECONDS)
    stream = connections.enter_context(connection.makefile("rb"))
    progress.append(f"{time_zone!r}: reading client Hello")
    packet = read_varuint(stream)
    assert packet == 0, f"Expected Hello, got client packet {packet}"
    read_string(stream)
    for unused in range(3):
        read_varuint(stream)
    for unused in range(3):
        read_string(stream)

    progress.append(f"{time_zone!r}: sending server Hello")
    connection.sendall(
        encode_varuint(0)
        + encode_string("ClickHouse")
        + encode_varuint(26)
        + encode_varuint(9)
        + encode_varuint(54058)
        + encode_string(time_zone)
    )
    progress.append(f"{time_zone!r}: waiting for Query")
    packet = read_varuint(stream)
    progress.append(f"{time_zone!r}: received client packet {packet}")
    assert packet == 1, f"Expected Query, got client packet {packet}"
    progress.append(f"{time_zone!r}: sending query response")
    connection.sendall(response)
    connection.shutdown(socket.SHUT_WR)
    progress.append(f"{time_zone!r}: response sent and write side closed")
    return stream


def serve(listener, time_zone, reconnect, progress):
    with ExitStack() as connections:
        if reconnect:
            serve_connection(
                connections,
                listener,
                "Asia/Kolkata",
                encode_varuint(2)
                + struct.pack("<i", 115)
                + encode_string("DB::Exception")
                + encode_string("Reconnect to a server without a usable timezone")
                + encode_string("")
                + b"\x00",
                progress,
            )
        stream = serve_connection(
            connections,
            listener,
            time_zone,
            encode_varuint(1)
            + encode_string("")
            + encode_varuint(0)
            + encode_varuint(2)
            + encode_varuint(1)
            + encode_string("dt")
            + encode_string("DateTime")
            + struct.pack("<I", 0)
            + encode_string("dt64")
            + encode_string("DateTime64(3)")
            + struct.pack("<q", 125)
            + encode_varuint(5),
            progress,
        )
        progress.append(f"{time_zone!r}: waiting for client EOF")
        stream.read()
        progress.append(f"{time_zone!r}: client EOF received")


def check_local_fallback(initial_use_client_time_zone, time_zone, reconnect=False):
    query = """
        SELECT toDateTime(0), toDateTime64('1970-01-01 00:00:00.125', 3)
        SETTINGS use_client_time_zone = 0 FORMAT TSV
    """
    if reconnect:
        query = "SELECT 1;\n" + query
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(TIMEOUT_SECONDS)
        command = shlex.split(os.environ["CLICKHOUSE_CLIENT_BINARY"]) + [
            "--no-secure",
            "--compression", "0",
            "--host", "127.0.0.1",
            "--port", str(listener.getsockname()[1]),
            "--use_client_time_zone", str(initial_use_client_time_zone),
            "--multiquery",
            "--query", query,
        ]
        if reconnect:
            command.append("--ignore-error")
        progress = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            server = executor.submit(serve, listener, time_zone, reconnect, progress)
            try:
                result = subprocess.run(
                    command, env=dict(os.environ, TZ="UTC"), input="", text=True,
                    capture_output=True, timeout=TIMEOUT_SECONDS,
                )
                assert result.returncode == 0, result.stderr
                assert result.stdout == "1970-01-01 00:00:00\t1970-01-01 00:00:00.125\n", result.stdout
                if initial_use_client_time_zone == 0:
                    assert "Proceeding with local time zone." in result.stderr, result.stderr
                if reconnect:
                    assert result.stderr.count("(UNKNOWN_SETTING)") == 1, result.stderr
                    assert "(query: SELECT 1" in result.stderr, result.stderr
                server.result()
            except Exception as error:
                print(
                    f"initial_use_client_time_zone={initial_use_client_time_zone}, reconnect={reconnect}\n"
                    + "\n".join(progress),
                    file=sys.stderr, flush=True,
                )
                if isinstance(error, subprocess.TimeoutExpired):
                    print(f"Client stdout: {error.stdout!r}\nClient stderr: {error.stderr!r}", file=sys.stderr, flush=True)
                if server.done():
                    server.result()
                raise


for initial_use_client_time_zone in (0, 1):
    check_local_fallback(initial_use_client_time_zone, "Invalid/ServerTimezone")
    print(f"initial_use_client_time_zone={initial_use_client_time_zone}: local fallback preserved")

for time_zone in ("", "Invalid/ServerTimezone"):
    check_local_fallback(0, time_zone, reconnect=True)
    print(f"reconnect_server_timezone={time_zone!r}: local fallback preserved")
PY
