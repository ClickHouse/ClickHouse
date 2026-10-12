#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_DATABASE" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" "$CUR_DIR/helpers" <<'PY'
import collections
import shlex
import socket
import struct
import subprocess
import sys
import uuid

client_command, database, host, port, helpers = sys.argv[1:]
sys.path.insert(0, helpers)
from native_profile_protocol import connect, encode_block, encode_varuint, read_packet, send_query

client = shlex.split(client_command)
table = "native_async_profile_" + uuid.uuid4().hex
qualified_table = f"`{database.replace('`', '``')}`.{table}"
column_count, rows = 64, 512
structure = ", ".join(f"c{index} UInt64" for index in range(column_count))


def control(query):
    result = subprocess.run(client + ["--send_profile_traces=0", "--memory_profiler_sample_probability=0", "--async_insert=0", "--query", query], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


control(f"CREATE TABLE {qualified_table} ({structure}) ENGINE=Memory")
try:
    for wait in (0, 1):
        query_id = "native_async_profile_" + uuid.uuid4().hex
        settings = {
            "async_insert": 1,
            "wait_for_async_insert": wait,
            "wait_for_async_insert_timeout": 30,
            "async_insert_use_adaptive_busy_timeout": 0,
            "async_insert_busy_timeout_min_ms": 100,
            "async_insert_busy_timeout_max_ms": 100,
            "async_insert_max_data_size": 1 << 30,
            "send_profile_events": 1,
            "send_profile_traces": 1,
            "send_logs_level": "none",
            "query_profiler_cpu_time_period_ns": 0,
            "query_profiler_real_time_period_ns": 0,
            "memory_profiler_step": 0,
            "memory_profiler_sample_probability": 1,
            "memory_profiler_sample_min_allocation_size": 4096,
            "memory_profiler_sample_max_allocation_size": 16384,
            "max_untracked_memory": 0,
            "max_threads": 1,
            "max_insert_threads": 1,
            # Force trace delivery into the final drain, after async progress.
            "interactive_delay": 3_600_000_000,
            "input_format_defaults_for_omitted_fields": 0,
        }
        packets, events, samples = [], collections.Counter(), []
        with socket.create_connection((host, int(port)), timeout=30) as sock:
            revision = connect(sock, database)
            send_query(sock, revision, query_id, f"INSERT INTO {qualified_table} FORMAT Native", settings)
            packet, payload = read_packet(sock, revision)
            assert packet == 1, (packet, payload)
            assert payload[0] == 0 and list(payload[1]) == [f"c{index}" for index in range(column_count)], payload
            data = struct.pack(f"<{rows}Q", *range(rows))
            columns = [(f"c{index}", "UInt64", data) for index in range(column_count)]
            sock.sendall(encode_varuint(2) + encode_block(columns, rows, revision))
            sock.sendall(encode_varuint(2) + encode_block([], 0, revision))
            while True:
                packet, payload = read_packet(sock, revision)
                packets.append((packet, payload))
                if packet == 14:
                    count, columns = payload
                    assert count > 0
                    for name, value in zip(columns["name"][1], columns["value"][1]):
                        events[name] += value
                elif packet == 19:
                    count, columns = payload
                    assert count > 0
                    samples += [dict(zip(columns, values)) for values in zip(*(column[1] for column in columns.values()))]
                if packet == 5:
                    break
            # The connection remains synchronized after the final packet.
            sock.sendall(encode_varuint(4))
            assert read_packet(sock, revision)[0] == 4

        types = [packet for packet, _ in packets]
        assert types[-1] == 5 and types.count(3) == 1, types
        progress_index = types.index(3)
        assert 14 in types[progress_index + 1:-1] and 19 in types[progress_index + 1:-1], types
        progress = packets[progress_index][1]
        assert progress["elapsed_ns"] > 0 and all(value == 0 for name, value in progress.items() if name != "elapsed_ns"), (wait, progress)
        assert events["AsyncInsertQuery"] == 1, events
        assert samples and all(sample["query_id"] == query_id for sample in samples), samples[:1]
        assert all(len(sample["trace"]) == len(sample["symbols"]) for sample in samples), samples[:1]
        assert all(sample["trace_type"] not in ("Dropped", "Incomplete") for sample in samples), samples[:1]
        assert any(sample["trace_type"] == "MemorySample" and sample["size"] > 0 and any("NativeReader::" in symbol for symbol in sample["symbols"]) for sample in samples), "missing Native upload samples"
        if not wait:
            control(f"SYSTEM FLUSH ASYNC INSERT QUEUE {qualified_table}")
        expected_sum = rows * (rows - 1) // 2
        assert control(f"SELECT count(), sum(c0), sum(c63) FROM {qualified_table} FORMAT TSV") == f"{rows}\t{expected_sum}\t{expected_sum}"
        control(f"TRUNCATE TABLE {qualified_table}")
        print(f"wait_for_async_insert={wait}: terminal progress, profile events and traces precede completion")
finally:
    control(f"DROP TABLE {qualified_table}")
PY
