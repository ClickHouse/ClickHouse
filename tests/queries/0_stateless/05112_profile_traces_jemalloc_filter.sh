#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug, no-llvm-coverage, no-parallel
# These builds do not provide the jemalloc profiler used by this test, as in 03594.
# Keep other profilers from delaying the query-specific `system.trace_log` witnesses.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_CURL" "$CLICKHOUSE_URL" <<'PY'
import json
import shlex
import subprocess
import sys
import time
import uuid
from urllib.parse import urlencode

client_command, curl_command, base_url = sys.argv[1:]
client = shlex.split(client_command)
curl = shlex.split(curl_command)


def run(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=90)
    except subprocess.TimeoutExpired:
        raise AssertionError("jemalloc profile trace query did not finish within 90 seconds") from None
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


def wait_for_jemalloc_samples(query):
    deadline = time.monotonic() + 20
    table_exists = False
    while time.monotonic() < deadline:
        if not table_exists:
            table_exists = run(client + ["--query", "EXISTS TABLE system.trace_log"]).stdout.strip() == "1"
        if table_exists and run(client + ["--query", query]).stdout.strip() == "1":
            return
        time.sleep(0.1)
    raise AssertionError("no actual jemalloc allocation callback appeared in trace_log within 20 seconds")


enabled = run(client + ["--query", "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC'"]).stdout.strip()
assert enabled == "1", "this test requires an actual jemalloc-enabled server"
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
    "jemalloc_enable_profiler": 1,
    "jemalloc_collect_profile_samples_in_trace_log": 1,
    "max_threads": 1,
    # Keep the `UInt64` source blocks above the memory-sampling threshold.
    "max_block_size": 65536,
}
query = "SELECT number FROM numbers(1000000) ORDER BY number FORMAT Null"
for transport in ("native", "HTTP"):
    query_id = "profile_traces_jemalloc_" + uuid.uuid4().hex
    options = dict(settings, query_id=query_id)
    if transport == "native":
        result = run(native_arguments(options) + ["--print-profile-traces", "--query", query])
        assert not result.stdout, result.stdout
        samples = [json.loads(line) for line in result.stderr.splitlines() if line]
    else:
        options.update(framing_output_format="JSONEachPacketString")
        result = run(curl + ["-sS", base_url + "&" + urlencode(options), "--data-binary", query])
        packets = [json.loads(line) for line in result.stdout.splitlines() if line]
        assert packets and packets[-1]["packet"] == "progress", packets[-1:]
        samples = [sample for packet in packets if packet["packet"] == "profile_traces" for sample in packet["profile_traces"]]
    types = {sample["trace_type"] for sample in samples}
    assert "MemorySample" in types and types <= {"CPU", "Real", "Memory", "MemorySample", "MemoryPeak", "Dropped", "Incomplete"}, sorted(types)
    wait_for_jemalloc_samples(f"""
        SELECT countIf(trace_type = 'JemallocSample' AND size > 0 AND ptr != 0 AND notEmpty(trace)) > 0
        FROM system.trace_log
        WHERE event_date >= today() - 1 AND event_time >= now() - 600 AND query_id = '{query_id}'
    """)
    print(f"{transport}: MemorySample streamed; real JemallocSample retained only in trace_log")
PY
