#!/usr/bin/env bash
# Tags: no-msan, no-parallel
# The sampling query profiler is disabled under MSan. The overflow case needs
# timely delivery through the shared collector to fill this query's bounded queue.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_URL" <<'PY'
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

base_url = urllib.parse.urlsplit(sys.argv[1])
settings = {
    "framing_output_format": "JSONEachPacketString",
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 10000000,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 0,
    "max_threads": 1,
    "max_block_size": 1,
    "interactive_delay": 1000,
    "output_format_parallel_formatting": 0,
    "http_response_buffer_size": 0,
}


def run(query, buffering="http_wait_end_of_query", sql_framing=False, overrides=None, error=False):
    options = dict(urllib.parse.parse_qsl(base_url.query))
    options.update(settings)
    options.update(overrides or {})
    options.pop("http_wait_end_of_query", None)
    options.pop("wait_end_of_query", None)
    options[buffering] = 1
    options["query_id"] = "http_buffered_trace_" + uuid.uuid4().hex
    if sql_framing:
        options.pop("framing_output_format")
    url = urllib.parse.urlunsplit(base_url._replace(query=urllib.parse.urlencode(options)))
    request = urllib.request.Request(url, data=query.encode())
    try:
        response = urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as exception:
        response = exception
    with response:
        packets = [json.loads(line) for line in response.read().decode().splitlines() if line]
        status = response.status
    terminal = "exception" if error else "progress"
    assert packets and packets[-1]["packet"] == terminal, (status, [packet["packet"] for packet in packets])
    if error:
        assert status == 500 and "buffered_trace_retention" in packets[-1]["exception"], packets[-1]
    else:
        assert status == 200, status
    assert not any(packet["packet"] == "data" for packet in packets), "unexpected buffered result payload"
    batches = [packet["profile_traces"] for packet in packets if packet["packet"] == "profile_traces"]
    assert batches and all(0 < len(batch) <= 1024 for batch in batches), [len(batch) for batch in batches]
    samples = [sample for batch in batches for sample in batch]
    assert all(sample["query_id"] == options["query_id"] for sample in samples)
    assert not any(sample["trace_type"] == "Incomplete" for sample in samples), "collector flush was incomplete"
    return samples


def require_early_sleep_samples(samples):
    times = [int(sample["event_time_microseconds"]) for sample in samples if sample["trace_type"] == "Real" and any("FunctionSleep" in symbol for symbol in sample["symbols"])]
    span = max(times) - min(times) if times else 0
    assert span >= 200000, f"early query samples were discarded: {len(times)} sleep samples spanning {span}us"
    assert not any(sample["trace_type"] == "Dropped" for sample in samples), "small query unexpectedly overflowed the trace queue"


failing_query = "SELECT sleepEachRow(0.01), throwIf(number = 50, 'buffered_trace_retention') FROM numbers(51)"
require_early_sleep_samples(run(failing_query + " FORMAT Null", error=True))
print("buffered query exceptions retain early trace samples")

# SQL changes are applied after the HTTP handler has already selected its buffering.
require_early_sleep_samples(
    run(
        failing_query + " SETTINGS framing_output_format='JSONEachPacketString', http_wait_end_of_query=0 FORMAT Null",
        buffering="wait_end_of_query",
        sql_framing=True,
        error=True,
    )
)
print("legacy buffering and SQL framing retain early trace samples")

require_early_sleep_samples(run("INSERT INTO FUNCTION null('n UInt64') SELECT number + sleepEachRow(0.01) FROM numbers(40)"))
print("buffered queries without results retain samples before final progress")

# Each array allocates at least 65536 bytes. Pace the one-row blocks so the
# collector can deliver the samples to the bounded queue, where losses are counted.
samples = run(
    "SELECT range(toUInt64(8192 + number % 2 + sleepEachRow(0.0001))) FROM numbers(5000) FORMAT Null",
    overrides={
        "query_profiler_real_time_period_ns": 0,
        "memory_profiler_sample_probability": 1,
        "memory_profiler_sample_min_allocation_size": 65536,
        "max_untracked_memory": 0,
    },
)
assert sum(int(sample["size"]) for sample in samples if sample["trace_type"] == "Dropped") > 0, "queue overflow was not reported"
assert 0 < sum(sample["trace_type"] not in {"Dropped", "Incomplete"} for sample in samples) <= 8192
print("buffered trace overflow is reported with bounded batches")
PY
