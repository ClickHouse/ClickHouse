#!/usr/bin/env bash
# Tags: no-parallel
# The failpoint suppresses final trace flush acknowledgements for the whole server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" <<'PY'
import json
import shlex
import signal
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
    "memory_profiler_sample_probability": 0,
    "max_block_size": 1,
    "max_threads": 1,
    "max_execution_time": 30,
    "partial_result_on_first_cancel": 0,
    "interactive_delay": 1000,
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
    result = subprocess.run(
        arguments({"send_profile_traces": 0}) + ["--query", query],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


process = None
control("SYSTEM ENABLE FAILPOINT profile_traces_flush_ack_timeout")
try:
    for enabled in (0, 1):
        query_id = "native_profile_cancel_" + uuid.uuid4().hex
        options = dict(settings, send_profile_traces=enabled, query_id=query_id)
        process = subprocess.Popen(
            arguments(options)
            + [
                "--print-profile-traces",
                "--query",
                "SELECT sleepEachRow(0.01) FROM numbers(10000) FORMAT Null",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if control(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") == "1":
                break
            assert process.poll() is None, process.communicate()
            time.sleep(0.02)
        else:
            raise AssertionError("query did not enter the process list")

        # `SIGINT` makes the native client send `Cancel`. No samples are needed to reproduce
        # the unwanted wait: the final collector barrier also runs for an empty queue.
        started = time.monotonic()
        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=20)
        elapsed = time.monotonic() - started
        assert elapsed < 5, f"native cancellation waited for trace collector: {elapsed:.2f}s"
        assert process.returncode == 0 and not stdout, (process.returncode, stdout, stderr)
        # The global profiler can send samples before `Cancel`, even when the query's timers are disabled.
        samples = [json.loads(line) for line in stderr.splitlines() if line.startswith("{")]
        assert enabled or not samples, samples
        for sample in samples:
            assert sample["query_id"] == query_id, sample
            trace_type = sample["trace_type"]
            assert trace_type != "Incomplete", ("cancellation flushed the trace collector", sample)
            assert trace_type in {"CPU", "Real", "Memory", "MemorySample", "MemoryPeak", "Dropped"}, sample
        assert control(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") == "0"
        assert control("SELECT 1") == "1"
        print(f"send_profile_traces={enabled}: cancellation does not wait for the collector")
finally:
    control("SYSTEM DISABLE FAILPOINT profile_traces_flush_ack_timeout")
    if process is not None and process.poll() is None:
        process.send_signal(signal.SIGINT)
        process.communicate(timeout=20)
PY
