#!/usr/bin/env bash
# Tags: no-msan, no-parallel
# The sampling query profiler is disabled under MSan.
# Keep competing profilers from dropping this query's samples in the shared pipe.

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
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 0,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_block_size": 65536,
    "max_execution_time": 60,
    "trace_profile_events": 0,
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


def execute(transport, query, query_id, sample_settings):
    options = dict(settings, **sample_settings, query_id=query_id)
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
        # Keep the shared trace pipe and log within budget: timers and allocation
        # sampling run in separate queries, and only the latter traces profile events.
        for query, sample_settings, required_types in (
            (
                "SELECT sum(sipHash64(number)) FROM numbers(1000000000000)",
                {"query_profiler_cpu_time_period_ns": 10000000,
                 "query_profiler_real_time_period_ns": 100000000,
                 # Let the leaf finish and flush samples before the initiator's deadline.
                 "max_rows_to_read": 0, "max_execution_time": settings["max_execution_time"] if remote else 2,
                 "timeout_overflow_mode": "throw" if remote else "break",
                 "max_execution_time_leaf": 2, "timeout_overflow_mode_leaf": "break"},
                {"CPU", "Real"},
            ),
            (
                "SELECT length(range(number + 100000)) FROM numbers(8)",
                {"memory_profiler_step": 65536, "memory_profiler_sample_probability": 1,
                 "trace_profile_events": 1},
                {"Memory", "MemorySample", "MemoryPeak"},
            ),
        ):
            if remote:
                query = f"SELECT * FROM remote({quote(remote_address)}, view({query}))"
            types = set()
            for attempt in range(4):
                query_id = "profile_trace_types_" + uuid.uuid4().hex
                samples = execute(transport, query + " FORMAT Null", query_id, sample_settings)
                for sample in samples:
                    if sample["trace_type"] in {"Dropped", "Incomplete"}:
                        assert sample["query_id"] and sample["host_name"], sample
                        assert not sample["trace"] and not sample["symbols"], sample
                        assert int(sample["thread_id"]) == int(sample["event_time_microseconds"]) == 0, sample
                        if sample["trace_type"] == "Dropped":
                            assert int(sample["size"]) > 0, sample
                        else:
                            assert int(sample["size"]) == 0, sample
                samples = [sample for sample in samples if sample["trace_type"] not in {"Dropped", "Incomplete"}]
                unsupported = {sample["trace_type"] for sample in samples} - allowed_types
                assert not unsupported, sorted(unsupported)
                for sample in samples:
                    if sample["trace_type"] in {"CPU", "Real"}:
                        assert int(sample["size"]) == 0, sample
                assert all(sample["query_id"] for sample in samples), samples
                if remote:
                    samples = [sample for sample in samples if sample["query_id"] != query_id]
                else:
                    assert all(sample["query_id"] == query_id for sample in samples), samples
                types.update(sample["trace_type"] for sample in samples)
                if required_types <= types:
                    break
                if attempt < 3:
                    time.sleep(0.1)
            assert required_types <= types, (transport, remote, sorted(required_types - types))
        print(f"{transport} {'remote' if remote else 'local'}: supported types streamed; ProfileEvent excluded from stream")
PY
