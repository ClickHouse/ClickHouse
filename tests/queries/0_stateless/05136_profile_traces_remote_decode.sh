#!/usr/bin/env bash
# Tags: no-msan, no-parallel
# The sampling query profiler is disabled under MSan.
# Exhaustive allocation sampling can fill the shared bounded profiler pipe, discarding other queries' samples.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_TCP" <<'PY'
import json
import shlex
import subprocess
import sys
import uuid

client = shlex.split(sys.argv[1])
address = sys.argv[2].replace("\\", "\\\\").replace("'", "\\'")
settings = {
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 0,
    "memory_profiler_step": 0,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_execution_time": 30,
    "prefer_localhost_replica": 0,
    "interactive_delay": 1000,
}


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


query = (
    f"SELECT * FROM remote('{address}', "
    "view(SELECT length(range(number + 100000)) FROM numbers(8))) FORMAT Null"
)
for asynchronous in (0, 1):
    for hedged in (0, 1):
        # The shared trace pipe can discard all remote samples under concurrent test load.
        for attempt in range(4):
            query_id = "profile_traces_decode_" + uuid.uuid4().hex
            options = dict(settings, query_id=query_id, async_socket_for_remote=asynchronous, use_hedged_requests=hedged)
            result = subprocess.run(
                native_arguments(options) + ["--print-profile-traces", "--query", query],
                capture_output=True, text=True, timeout=60,
            )
            assert result.returncode == 0, result.stderr
            assert not result.stdout, result.stdout
            samples = [json.loads(line) for line in result.stderr.splitlines() if line]
            for sample in samples:
                if sample["query_id"] != query_id:
                    continue
                decoding = [symbol for symbol in sample["symbols"] if "Connection::receiveProfileTraces" in symbol]
                assert not decoding, (asynchronous, hedged, sample["trace_type"], decoding)
            if any(sample["query_id"] != query_id and any(sample["symbols"]) for sample in samples):
                break
        else:
            raise AssertionError((asynchronous, hedged, "no remote samples with resolved symbols"))
        print(f"async={asynchronous} hedged={hedged}: remote samples received without decoder feedback")
PY
