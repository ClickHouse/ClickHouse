#!/usr/bin/env bash
# Tags: no-msan, no-parallel
# The sampling query profiler is disabled under MSan.
# Millisecond sampling can exhaust the shared profiler pipe and delay trace delivery.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_CURL" "$CLICKHOUSE_URL" <<'PY'
import json
import shlex
import subprocess
import sys
import uuid
from urllib.parse import urlencode

client_command, curl_command, base_url = sys.argv[1:]
client = shlex.split(client_command)
curl = shlex.split(curl_command)
settings = {
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 1000000,
    "query_profiler_real_time_period_ns": 1000000,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "memory_profiler_step": 65536,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_execution_time": 60,
    "interactive_delay": 1000,
}
delivery_functions = (
    "InternalProfileTracesQueue::getBlock",
    "InternalProfileTracesQueue::finish",
    "TCPHandler::sendProfileTraces",
    "IFramingFormat::pumpProfileTraces",
)


def run(command):
    result = subprocess.run(command, capture_output=True, text=True, timeout=90)
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


def execute(transport, query, options):
    if transport == "Native":
        result = run(native_arguments(options) + ["--print-profile-traces", "--query", query])
        assert not result.stdout, result.stdout
        return [json.loads(line) for line in result.stderr.splitlines() if line]
    options.update(framing_output_format="JSONEachPacketString", http_wait_end_of_query=0,
                   http_response_buffer_size=0, output_format_parallel_formatting=0)
    result = run(curl + ["-sS", base_url + "&" + urlencode(options), "--data-binary", query])
    packets = [json.loads(line) for line in result.stdout.splitlines() if line]
    assert packets and packets[-1]["packet"] == "progress", packets[-1:]
    return [sample for packet in packets if packet["packet"] == "profile_traces" for sample in packet["profile_traces"]]


for transport in ("Native", "HTTP"):
    resolved_types = set()
    # Samples can be dropped when concurrent queries saturate the shared profiler pipe.
    for _ in range(4):
        for allocations in (True, False):
            options = dict(settings, query_id="profile_traces_delivery_" + uuid.uuid4().hex)
            if allocations:
                query = "SELECT length(range(number + 100000)) FROM numbers(128) FORMAT Null"
            else:
                query = "SELECT sum(sipHash64(number)) FROM numbers(1000000000) FORMAT Null"
                options.update(memory_profiler_sample_probability=0, memory_profiler_step=0,
                               query_profiler_cpu_time_period_ns=10000000, query_profiler_real_time_period_ns=100000000,
                               max_rows_to_read=0, max_execution_time=2, timeout_overflow_mode="break")
            for sample in execute(transport, query, options):
                if sample["trace_type"] not in ("CPU", "Real"):
                    continue
                if any(sample["symbols"]):
                    resolved_types.add(sample["trace_type"])
                delivery = [symbol for symbol in sample["symbols"] if any(function in symbol for function in delivery_functions)]
                assert not delivery, (transport, sample)
        if resolved_types == {"CPU", "Real"}:
            break
    assert resolved_types == {"CPU", "Real"}, (transport, "missing query samples with resolved symbols", sorted(resolved_types))
    print(f"{transport}: CPU and Real query samples streamed without delivery stacks")
PY
