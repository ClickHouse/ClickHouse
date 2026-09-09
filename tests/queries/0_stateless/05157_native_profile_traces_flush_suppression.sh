#!/usr/bin/env bash
# Tags: no-msan, no-parallel
# The sampling query profiler is disabled under MSan.
# High-rate samples need timely delivery through the shared trace collector.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" <<'PY'
import json
import shlex
import subprocess
import sys
import uuid

client = shlex.split(sys.argv[1])
settings = {
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 1000000,
    "query_profiler_real_time_period_ns": 1000000,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "interactive_delay": 1000,
    "max_threads": 1,
    "max_insert_threads": 1,
    "async_insert": 0,
    "max_insert_block_size": 1,
    "min_insert_block_size_rows": 0,
    "min_insert_block_size_bytes": 0,
    "input_format_parallel_parsing": 0,
    "max_execution_time": 30,
    "compression": 0,
}
socket_functions = (
    "WriteBufferFromPocoSocket::socketSendBytesImpl",
    "Poco::Net::SocketImpl::sendBytes",
    "Poco::Net::StreamSocketImpl::sendBytes",
)


def arguments(options):
    result = []
    index = 0
    while index < len(client):
        argument = client[index]
        name = argument.split("=", 1)[0].removeprefix("--")
        if argument.startswith("--") and name in options:
            if "=" not in argument and index + 1 < len(client) and not client[index + 1].startswith("-"):
                index += 1
        else:
            result.append(argument)
        index += 1
    return result + [f"--{key}={value}" for key, value in options.items()]


def control(query):
    result = subprocess.run(arguments({"send_profile_traces": 0}) + ["--query", query], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def execute(query, input_data=None, overrides=None):
    options = dict(settings, query_id="native_trace_flush_" + uuid.uuid4().hex)
    options.update(overrides or {})
    result = subprocess.run(
        arguments(options) + ["--print-profile-traces", "--query", query],
        input=input_data,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr
    samples = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{"host_name":')]
    timer_samples = [sample for sample in samples if sample["trace_type"] in ("CPU", "Real")]
    assert timer_samples and any(any(sample["symbols"]) for sample in timer_samples), "missing resolved query timer samples"
    return result.stdout, timer_samples


def socket_samples(samples, caller):
    return [
        sample
        for sample in samples
        if any(caller in symbol for symbol in sample["symbols"])
        and any("WriteBufferFromPocoSocket::nextImpl" in symbol for symbol in sample["symbols"])
        and any(function in symbol for symbol in sample["symbols"] for function in socket_functions)
        and not any("TCPHandler::startInsertQuery" in symbol for symbol in sample["symbols"])
    ]


# After the insertion schema is sent, this upload produces no ordinary response
# payload, logs, or profile events. Concrete socket sends from the upload loop
# therefore belong to trace delivery; an empty flush entry is not sufficient.
table = "native_trace_flush_" + uuid.uuid4().hex
control(f"CREATE TABLE {table} (n UInt64, arr Array(UInt64) MATERIALIZED range(n)) ENGINE = Null")
try:
    # Each `UInt64` array reaches the 65536-byte sampling threshold, before `PODArray` padding.
    output, samples = execute(f"INSERT INTO {table} (n) FORMAT TSV", "8192\n" * 256)
    assert not output, output
    assert any("TCPHandler::processInsertQuery" in symbol for sample in samples for symbol in sample["symbols"]), "missing insertion-handler timer samples"
    assert not socket_samples(samples, "TCPHandler::processInsertQuery"), socket_samples(samples, "TCPHandler::processInsertQuery")[:1]
finally:
    control(f"DROP TABLE {table}")
print("native INSERT trace socket sends are excluded from streamed samples")

# A guard over every socket flush would incorrectly hide ordinary query output.
output, samples = execute(
    "SELECT number FROM numbers(200000) FORMAT TSV",
    overrides={"memory_profiler_sample_probability": 0, "max_block_size": 128},
)
rows = output.splitlines()
assert len(rows) == 200000 and rows[0] == "0" and rows[-1] == "199999"
assert socket_samples(samples, "TCPHandler::processOrdinaryQuery"), "ordinary native result socket sends were excluded from the profile"
print("ordinary native result socket sends remain visible in streamed samples")
PY
