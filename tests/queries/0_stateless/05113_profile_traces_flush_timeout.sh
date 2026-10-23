#!/usr/bin/env bash
# Tags: no-parallel
# The failpoints affect final trace flushes on the whole server, including other profiling queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_CURL" "$CLICKHOUSE_URL" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" <<'PY'
import json
import select
import shlex
import socket
import subprocess
import sys
import threading
import time
import uuid
from urllib.parse import urlencode

client_command, curl_command, base_url, server_host, server_port = sys.argv[1:]
client = shlex.split(client_command)
curl = shlex.split(curl_command)
settings = {
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "interactive_delay": 3600000000,
    "max_threads": 1,
    "max_block_size": 1,
}
success_query = "SELECT sum(length(range(number + 100000))) FROM numbers(8) FORMAT TSV"
exception_query = "SELECT length(range(number + 100000)), throwIf(number = 4, 'profile trace SQL exception') FROM numbers(8) FORMAT Null"


def run(command, timeout=30):
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise AssertionError("profile trace completion exceeded its process deadline") from None


def native_arguments(options):
    arguments = []
    index = 0
    while index < len(client):
        argument = client[index]
        name = argument.split("=", 1)[0].removeprefix("--")
        if argument.startswith("--") and name in options:
            if "=" not in argument and index + 1 < len(client) and not client[index + 1].startswith("-"):
                index += 1
        else:
            arguments.append(argument)
        index += 1
    return arguments + [f"--{key}={value}" for key, value in options.items()]


def control(query):
    result = run(native_arguments({"send_profile_traces": 0}) + ["--query", query])
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def command(transport, query, query_id, enabled=1):
    options = dict(settings, query_id=query_id, send_profile_traces=enabled)
    if transport == "native":
        return native_arguments(options) + ["--print-profile-traces", "--query", query]
    options.update(framing_output_format="JSONEachPacketString", http_wait_end_of_query=0,
                   http_response_buffer_size=0, output_format_parallel_formatting=0)
    return curl + ["-sS", base_url + "&" + urlencode(options), "--data-binary", query]


def check_metadata(traces, incomplete):
    if not incomplete:
        assert not traces, "disabled delivery sent profile trace metadata"
        return
    assert sum(row["trace_type"] == "Incomplete" for row in traces) == 1, traces
    for row in traces:
        assert row["trace_type"] in {"Dropped", "Incomplete"}, "undrained samples were sent after the flush was discarded"
        assert not row["trace"] and not row["symbols"], row
        assert int(row["thread_id"]) == int(row["event_time_microseconds"]) == 0, row
        assert int(row["size"]) > 0 if row["trace_type"] == "Dropped" else int(row["size"]) == 0, row


def check_response(transport, result, exception=None, incomplete=False):
    if transport == "native":
        traces = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{"host_name":')]
        check_metadata(traces, incomplete)
        if exception:
            assert result.returncode != 0 and exception in result.stderr, result.stderr
        else:
            assert result.returncode == 0 and result.stdout == "800028\n", (result.stdout, result.stderr)
        return
    assert result.returncode == 0, result.stderr
    packets = [json.loads(line) for line in result.stdout.splitlines() if line]
    assert packets, packets
    check_metadata([row for packet in packets if packet["packet"] == "profile_traces" for row in packet["profile_traces"]], incomplete)
    if exception:
        assert packets[-1]["packet"] == "exception" and exception in packets[-1]["exception"], packets[-1:]
    else:
        assert packets[-1]["packet"] == "progress", packets[-1:]
        assert "".join(packet["data"] for packet in packets if packet["packet"] == "data") == "800028\n", packets


def complete(transport, query, exception=None, enabled=1, wait_for_ack=False):
    query_id = "profile_trace_flush_" + uuid.uuid4().hex
    start = time.monotonic()
    result = run(command(transport, query, query_id, enabled))
    elapsed = time.monotonic() - start
    check_response(transport, result, exception, incomplete=bool(enabled))
    if wait_for_ack:
        assert 9 <= elapsed < 18, f"flush did not respect its single 10-second deadline: {elapsed:.2f}s"
    else:
        assert elapsed < 5, f"query unnecessarily waited for a profile trace flush: {elapsed:.2f}s"


def sql_opt_out(query, exception=None):
    # The normal client promotes inline settings into its wire settings. Rewrite only the SQL
    # text in flight, preserving its length, to exercise wire opt-in followed by SQL opt-out.
    marker = b"SETTINGS send_profile_traces = 1"
    replacements = []
    errors = []
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(15)

    def relay():
        try:
            downstream, _ = listener.accept()
            with downstream, socket.create_connection((server_host, int(server_port)), timeout=15) as upstream:
                pending = b""
                readers = [downstream, upstream]
                while readers:
                    ready, _, _ = select.select(readers, [], [], 15)
                    if not ready:
                        raise TimeoutError("native opt-out proxy made no progress")
                    for source in ready:
                        data = source.recv(65536)
                        target = upstream if source is downstream else downstream
                        if not data:
                            if source is downstream and pending:
                                upstream.sendall(pending)
                            target.shutdown(socket.SHUT_WR)
                            readers.remove(source)
                            continue
                        if source is downstream:
                            data = pending + data
                            replacements.extend([1] * data.count(marker))
                            data = data.replace(marker, marker[:-1] + b"0")
                            # Retain only a possible marker prefix; buffering the whole handshake
                            # until a SQL query arrives would prevent the client from sending one.
                            keep = min(len(marker) - 1, len(data))
                            while keep and not data.endswith(marker[:keep]):
                                keep -= 1
                            pending = data[-keep:] if keep else b""
                            data = data[:-keep] if keep else data
                        if data:
                            target.sendall(data)
        except OSError as error:
            errors.append(str(error))
        finally:
            listener.close()

    worker = threading.Thread(target=relay, daemon=True)
    worker.start()
    query_id = "profile_trace_sql_optout_" + uuid.uuid4().hex
    original = command("native", query, query_id)
    proxied = []
    index = 0
    while index < len(original):
        argument = original[index]
        if argument in ("--host", "--port"):
            index += 2
            continue
        if not argument.startswith(("--host=", "--port=")):
            proxied.append(argument)
        index += 1
    proxied += ["--host=127.0.0.1", f"--port={listener.getsockname()[1]}"]
    start = time.monotonic()
    try:
        result = run(proxied, timeout=5)
    finally:
        worker.join(timeout=16)
    assert not worker.is_alive(), "native opt-out proxy did not finish"
    assert not errors, errors
    assert len(replacements) == 1, f"expected exactly one SQL replacement, got {len(replacements)}"
    assert time.monotonic() - start < 5, "SQL opt-out still waited for the profile trace collector"
    check_response("native", result, exception)


for failure in ("write", "ack"):
    failpoint = f"profile_traces_flush_{failure}_timeout"
    control(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    try:
        if failure == "ack":
            for transport in ("native", "HTTP"):
                complete(transport, success_query, enabled=0)
                print(f"{transport}: disabled profiling does not wait for the collector")
            sql_opt_out(success_query.replace(" FORMAT TSV", " SETTINGS send_profile_traces = 1 FORMAT TSV"))
            missing_table = "profile_traces_missing_" + uuid.uuid4().hex
            sql_opt_out(f"SELECT * FROM {missing_table} SETTINGS send_profile_traces = 1", "UNKNOWN_TABLE")
            sql_opt_out(f"INSERT INTO {missing_table} SETTINGS send_profile_traces = 1 VALUES (1)", "UNKNOWN_TABLE")
            print("native: SQL opt-out preserves results and analysis exceptions without waiting")
        for transport in ("native", "HTTP"):
            complete(transport, success_query, wait_for_ack=failure == "ack")
            print(f"{transport}: {failure} timeout preserves the SQL result and discards undrained traces")
            complete(transport, exception_query, exception="profile trace SQL exception", wait_for_ack=failure == "ack")
            print(f"{transport}: {failure} timeout preserves the original SQL exception")
    finally:
        control(f"SYSTEM DISABLE FAILPOINT {failpoint}")
PY
