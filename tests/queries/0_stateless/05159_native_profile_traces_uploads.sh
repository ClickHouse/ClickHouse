#!/usr/bin/env bash
# Tags: no-parallel
# The failpoint suppresses final trace flush acknowledgements for the whole server.

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
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 1024,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_insert_threads": 1,
    "max_block_size": 1024,
    "max_insert_block_size": 1024,
    "min_insert_block_size_rows": 0,
    "min_insert_block_size_bytes": 0,
    "input_format_parallel_parsing": 0,
    "async_insert": 0,
    "max_execution_time": 30,
    "send_timeout": 30,
    "receive_timeout": 30,
    "interactive_delay": 1000,
    "compression": 0,
}


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
    options = dict(settings, send_profile_traces=0, memory_profiler_sample_probability=0)
    result = subprocess.run(arguments(options) + ["--query", query], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def execute(query, data, extra=None, enabled=1, error=False):
    initial_id = "native_upload_" + uuid.uuid4().hex
    options = dict(settings, query_id=initial_id, send_profile_traces=enabled)
    started = time.monotonic()
    result = subprocess.run(
        arguments(options) + ["--print-profile-traces", "--query", query] + (extra or []),
        input=data,
        capture_output=True,
        text=True,
        timeout=60,
    )
    elapsed = time.monotonic() - started
    samples = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{"host_name":')]
    for sample in samples:
        assert sample["query_id"] == initial_id, sample
        assert len(sample["trace"]) == len(sample["symbols"]), sample
        assert sample["trace_type"] != "Incomplete", sample
    if error:
        assert result.returncode != 0 and "upload_reject" in result.stderr and "violated" in result.stderr, result.stderr
        assert elapsed < 8, f"upload exception waited for the trace collector: {elapsed:.2f}s"
        assert not samples, samples[:1]
    else:
        assert result.returncode == 0, result.stderr
        if enabled:
            assert any(sample["trace_type"] == "MemorySample" and int(sample["size"]) > 0 and any("NativeReader::" in symbol for symbol in sample["symbols"]) for sample in samples), (
                "missing native input allocation samples"
            )
        else:
            assert not samples, samples[:1]
    return result.stdout.strip()


# Many columns produce qualifying allocations in each upload block. The client
# sends external tables without reading the response until their input ends.
rows = 32768
structure = ", ".join(f"c{index} UInt64" for index in range(64))
row = "\t".join(["1"] * 64) + "\n"
data = row * rows
external = ["--external", "--file=-", "--name=upload_data", f"--structure={structure}", "--format=TSV"]
for enabled in (0, 1):
    assert execute("SELECT count(), sum(c0) FROM upload_data FORMAT TSV", data, external, enabled) == f"{rows}\t{rows}"
print("external uploads finish and retain enabled trace samples")

table = "native_upload_" + uuid.uuid4().hex
control(f"CREATE TABLE {table} ({structure}) ENGINE = Memory")
try:
    insert_rows = 1024
    for query in (f"INSERT INTO {table} FORMAT TSV", f"INSERT INTO {table} SELECT * FROM input('{structure}') FORMAT TSV"):
        assert not execute(query, row * insert_rows)
        assert control(f"SELECT count(), sum(c0) FROM {table} FORMAT TSV") == f"{insert_rows}\t{insert_rows}"
        control(f"TRUNCATE TABLE {table}")
    print("INSERT and input uploads retain samples and complete every row")
finally:
    control(f"DROP TABLE {table}")

control(f"CREATE TABLE {table} (n UInt64, CONSTRAINT upload_reject CHECK n = 0) ENGINE = Null")
try:
    control("SYSTEM ENABLE FAILPOINT profile_traces_flush_ack_timeout")
    try:
        execute(f"INSERT INTO {table} FORMAT TSV", "1\n" * rows, error=True)
        assert control("SELECT 1") == "1"
    finally:
        control("SYSTEM DISABLE FAILPOINT profile_traces_flush_ack_timeout")
finally:
    control(f"DROP TABLE {table}")
print("upload exceptions preserve the error without waiting for trace collection")
PY
