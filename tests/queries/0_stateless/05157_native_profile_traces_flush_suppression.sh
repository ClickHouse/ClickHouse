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
import time
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


def control(query, timeout=30):
    result = subprocess.run(arguments({"send_profile_traces": 0}) + ["--query", query], capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, result.stderr
    return result.stdout


def execute(query, input_data=None, overrides=None, backpressure=False):
    options = dict(settings, query_id="native_trace_flush_" + uuid.uuid4().hex)
    options.update(overrides or {})
    command = arguments(options) + ["--print-profile-traces", "--query", query]
    if backpressure:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            try:
                wait_for_socket_send(process, options["query_id"])
                stdout, stderr = process.communicate(timeout=45)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=10)
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    else:
        result = subprocess.run(command, input=input_data, capture_output=True, text=True, timeout=45)
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


def wait_for_socket_send(process, query_id):
    # Holding the client's stdout fills the socket buffers. Observe the query's
    # actual send stack independently of streaming suppression, and keep it
    # blocked until the query profiler has run while that stack is visible.
    query = f"""
        SELECT
            (SELECT max(ProfileEvents['QueryProfilerRuns']) FROM system.processes WHERE query_id = '{query_id}') AS profiler_runs,
            arrayMap(address -> demangle(addressToSymbol(address)), trace) AS symbols
        FROM system.stack_trace
        WHERE thread_id IN (SELECT arrayJoin(thread_ids) FROM system.processes WHERE query_id = '{query_id}')
            AND query_id = '{query_id}'
        SETTINGS allow_introspection_functions = 1
        FORMAT JSONEachRow
    """
    deadline = time.monotonic() + 20
    # The profiler temporarily masks the signal used by `system.stack_trace`.
    # Keep the first confirmed send across missing observations, and still
    # require a later send observation with additional profiler runs.
    first_runs = None
    samples = []
    while (remaining := deadline - time.monotonic()) > 0:
        assert process.poll() is None, process.stderr.read()
        samples = [json.loads(line) for line in control(query, timeout=remaining).splitlines()]
        sending = [
            sample for sample in socket_samples(samples, "TCPHandler::processOrdinaryQuery")
            if not any("TCPHandler::sendProfileTraces" in symbol for symbol in sample["symbols"])
        ]
        if sending:
            runs = max(int(sample["profiler_runs"]) for sample in sending)
            if first_runs is None:
                first_runs = runs
            if runs >= first_runs + 10:
                return
        time.sleep(0.01)
    raise AssertionError(f"ordinary query did not remain in a socket send while sampled: {samples}")


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
# A wide result fills the socket buffers with few blocks, so the positive
# control does not depend on sampling a short, immediately writable send.
# Defer trace batches until the final drain so the blocked write is query output.
output, samples = execute(
    "SELECT number, repeat('x', 65536) FROM numbers(512) FORMAT TSV",
    overrides={"memory_profiler_sample_probability": 0, "max_block_size": 16, "interactive_delay": 30000000},
    backpressure=True,
)
assert output.count("\n") == 512
assert output.startswith("0\t" + "x" * 65536 + "\n") and output.endswith("511\t" + "x" * 65536 + "\n")
assert socket_samples(samples, "TCPHandler::processOrdinaryQuery"), "ordinary native result socket sends were excluded from the profile"
print("ordinary native result socket sends remain visible in streamed samples")
PY
