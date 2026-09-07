#!/usr/bin/env bash
# Tags: no-msan
# The sampling query profiler is disabled under MSan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_CURL" "$CLICKHOUSE_URL" "$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP" <<'PY'
import json
import shlex
import subprocess
import sys
import time
import uuid
from urllib.parse import urlencode

client_command, curl_command, base_url, remote_address = sys.argv[1:]
client = shlex.split(client_command)
curl = shlex.split(curl_command)
allowed_types = {"CPU", "Real", "Memory", "MemorySample", "MemoryPeak"}
settings = {
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 1000000,
    "query_profiler_real_time_period_ns": 1000000,
    "memory_profiler_step": 65536,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_block_size": 65536,
    "max_execution_time": 60,
    "trace_profile_events": 1,
    "trace_profile_events_list": "FunctionExecute",
    "prefer_localhost_replica": 0,
}


def run(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=90)
    except subprocess.TimeoutExpired:
        raise AssertionError("profile trace type query did not finish within 90 seconds") from None
    assert result.returncode == 0, result.stderr
    return result


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


def quote(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def wait_for_profile_events(query):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        run(client + ["--query", "SYSTEM FLUSH LOGS trace_log"])
        if run(client + ["--query", query]).stdout.strip() == "1":
            return
        time.sleep(0.1)
    raise AssertionError("profile events did not appear in trace_log within 20 seconds")


def execute(transport, query, query_id):
    options = dict(settings, query_id=query_id)
    if transport == "native":
        result = run(native_arguments(options) + ["--print-profile-traces", "--query", query])
        assert not result.stdout, result.stdout
        return [json.loads(line) for line in result.stderr.splitlines() if line]
    options.update(framing_output_format="JSONEachPacketString", http_wait_end_of_query=0,
                   http_response_buffer_size=0, output_format_parallel_formatting=0)
    result = run(curl + ["-sS", base_url + "&" + urlencode(options), "--data-binary", query])
    packets = [json.loads(line) for line in result.stdout.splitlines() if line]
    assert packets and packets[-1]["packet"] == "progress", packets[-1:]
    return [sample for packet in packets if packet["packet"] == "profile_traces" for sample in packet["profile_traces"]]


for transport in ("native", "HTTP"):
    for remote in (False, True):
        types = set()
        observed_ids = set()
        for query in (
            "SELECT sum(sipHash64(number)) FROM numbers(10000000)",
            "SELECT length(range(number + 100000)) FROM numbers(8)",
        ):
            query_id = "profile_trace_types_" + uuid.uuid4().hex
            if remote:
                query = f"SELECT * FROM remote({quote(remote_address)}, view({query}))"
            samples = execute(transport, query + " FORMAT Null", query_id)
            assert samples, "no samples were streamed"
            unsupported = {sample["trace_type"] for sample in samples} - allowed_types
            assert not unsupported, sorted(unsupported)
            types.update(sample["trace_type"] for sample in samples)
            observed_ids.update(sample["query_id"] for sample in samples)
            if remote:
                assert any(sample["query_id"] != query_id for sample in samples), "no forwarded samples"
            else:
                assert all(sample["query_id"] == query_id for sample in samples), samples

        assert types == allowed_types, sorted(types)
        ids = ", ".join(quote(value) for value in sorted(observed_ids))
        wait_for_profile_events(f"""
            SELECT countIf(trace_type = 'ProfileEvent' AND event = 'FunctionExecute' AND notEmpty(trace)) > 0
            FROM system.trace_log
            WHERE event_date >= today() - 1 AND event_time >= now() - 600 AND query_id IN ({ids})
        """)
        print(f"{transport} {'remote' if remote else 'local'}: supported types streamed; ProfileEvent retained in trace_log")
PY
