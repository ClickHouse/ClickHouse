#!/usr/bin/env bash

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
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "memory_profiler_step": 0,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_block_size": 1,
    "max_execution_time": 20,
}
arguments = []
overridden = set(settings) | {"query_id"}
index = 0
while index < len(client):
    argument = client[index]
    name = argument.split("=", 1)[0].removeprefix("--")
    if argument.startswith("--") and name in overridden:
        if "=" not in argument and index + 1 < len(client) and not client[index + 1].startswith("-"):
            index += 1
    else:
        arguments.append(argument)
    index += 1
arguments += [f"--{key}={value}" for key, value in settings.items()]
cases = (
    ("unicode", "é_日本_😀".encode(), "é_日本_😀"),
    ("escaping", b'"\\\n\t\r\x01', '"\\\n\t\r\x01'),
    ("invalid", b"\xff", "\ufffd"),
    ("truncated", b"\xe2\x82", "\ufffd"),
)
for name, suffix, expected_suffix in cases:
    prefix = "native_profile_utf8_" + uuid.uuid4().hex + "_"
    query_id = prefix.encode() + suffix
    result = subprocess.run(
        arguments + [
            "--print-profile-traces",
            b"--query_id=" + query_id,
            "--query=SELECT length(range(number + 100000)) FROM numbers(128) FORMAT Null",
        ],
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, (name, result.returncode, result.stderr)
    assert not result.stdout, (name, result.stdout)
    assert result.stderr.endswith(b"\n"), (name, result.stderr)
    samples = [json.loads(line.decode("utf-8")) for line in result.stderr.split(b"\n")[:-1]]
    assert any(sample["trace_type"] == "MemorySample" and int(sample["size"]) > 0 for sample in samples), (name, samples)
    assert all(sample["query_id"] == prefix + expected_suffix for sample in samples), (name, samples)
    print(name + ": valid UTF-8 JSON and expected query ID")
PY
