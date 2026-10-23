#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CUR_DIR/helpers" <<'PY'
import shlex
import socket
import struct
import subprocess
import sys
import threading

client_command, helpers = sys.argv[1:]
sys.path.insert(0, helpers)
from native_profile_protocol import LEGACY_TIMEZONE_REVISION, encode_block, encode_string, encode_varuint, read_block, read_legacy_query, read_string, read_varuint

client = shlex.split(client_command)
structure = "ts DateTime('UTC'), precise DateTime64(3, 'UTC')"
structure_sql = structure.replace("'", "''")
query = f"INSERT INTO FUNCTION null('{structure_sql}') SELECT * FROM input('{structure_sql}') FORMAT CSV"
data = "2026-01-15 12:00:00,2026-01-15 12:00:00.123\n"
schema = [("ts", "DateTime('UTC')", b""), ("precise", "DateTime64(3, 'UTC')", b"")]


def arguments(options):
    result = []
    index = 0
    aliases = {"h": "host", "p": "port"}
    while index < len(client):
        argument = client[index]
        name = aliases.get(argument.split("=", 1)[0].lstrip("-"), argument.split("=", 1)[0].lstrip("-"))
        if argument.startswith("-") and name in options:
            if "=" not in argument and index + 1 < len(client) and not client[index + 1].startswith("-"):
                index += 1
        else:
            result.append(argument)
        index += 1
    return result + [f"--{name}={value}" for name, value in options.items() if value is not None]


for metadata in (0, 1):
    errors = []
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(30)

        def serve():
            try:
                with listener.accept()[0] as sock:
                    sock.settimeout(30)
                    assert read_varuint(sock) == 0
                    read_string(sock)
                    read_varuint(sock)
                    read_varuint(sock)
                    assert read_varuint(sock) >= LEGACY_TIMEZONE_REVISION
                    for _ in range(3):
                        read_string(sock)
                    hello = encode_varuint(0) + encode_string("ClickHouse")
                    hello += encode_varuint(23) + encode_varuint(3) + encode_varuint(LEGACY_TIMEZONE_REVISION)
                    hello += encode_string("UTC") + encode_string("legacy_timezone_fixture") + encode_varuint(1)
                    hello += encode_varuint(0) + struct.pack("<Q", 0)
                    sock.sendall(hello)
                    assert read_string(sock) == ""  # Quota key addendum.
                    actual_query, settings = read_legacy_query(sock)
                    assert actual_query == query, actual_query
                    assert settings["input_format_defaults_for_omitted_fields"][1] == str(metadata), settings
                    response = b""
                    if metadata:
                        description = "columns format version: 1\n2 columns:\n`ts` DateTime('UTC')\n`precise` DateTime64(3, 'UTC')\n"
                        response += encode_varuint(11) + encode_string("") + encode_string(description)
                    response += encode_varuint(1) + encode_block(schema, 0, LEGACY_TIMEZONE_REVISION)
                    # Legacy servers send `TimezoneUpdate` after the `input` schema.
                    response += encode_varuint(17) + encode_string("Asia/Tokyo")
                    sock.sendall(response)
                    received_rows = 0
                    while True:
                        assert read_varuint(sock) == 2
                        rows, columns = read_block(sock, LEGACY_TIMEZONE_REVISION)
                        if not columns:
                            assert rows == 0
                            break
                        assert rows == 1, (rows, columns)
                        assert columns == {
                            "ts": ("DateTime('UTC')", [1768478400]),
                            "precise": ("DateTime64(3, 'UTC')", [1768478400123]),
                        }, columns
                        received_rows += rows
                    assert received_rows == 1, received_rows
                    progress = encode_varuint(3) + b"".join(encode_varuint(value) for value in (0, 0, 0, 0, 1, 12, 0))
                    sock.sendall(progress + encode_varuint(5))
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        options = {
            "host": "127.0.0.1",
            "port": listener.getsockname()[1],
            "secure": None,
            "no-secure": None,
            "compression": 0,
            "session_timezone": None,
            "use_client_time_zone": 0,
            "apply_settings_from_server": 0,
            "async_insert": 0,
            "send_profile_traces": 0,
            "send_profile_events": 0,
            "send_logs_level": "none",
            "input_format_parallel_parsing": 0,
            "input_format_defaults_for_omitted_fields": metadata,
        }
        result = subprocess.run(arguments(options) + ["--no-secure", "--query", query], input=data, capture_output=True, text=True, timeout=30)
        worker.join(timeout=30)
        assert not worker.is_alive(), "legacy protocol fixture did not finish"
        assert not errors, errors
        assert result.returncode == 0 and not result.stdout, (result.returncode, result.stdout, result.stderr)
    print(f"legacy timezone after input schema: column metadata={metadata}")
PY
