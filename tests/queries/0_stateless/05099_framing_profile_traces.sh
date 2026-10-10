#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_URL" "$CLICKHOUSE_CURL" "$CLICKHOUSE_CLIENT" <<'PY'
import json
import shlex
import subprocess
import sys
import time
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

base_url, curl_command, client_command = sys.argv[1:]
curl = shlex.split(curl_command)
client = shlex.split(client_command)
base_url += (
    "&send_profile_events=0&send_logs_level=none"
    "&http_wait_end_of_query=0&http_response_buffer_size=0"
    "&output_format_parallel_formatting=0&max_threads=1&max_block_size=1"
    "&query_profiler_cpu_time_period_ns=0&query_profiler_real_time_period_ns=0"
    "&memory_profiler_step=0&memory_profiler_sample_probability=1"
    "&memory_profiler_sample_min_allocation_size=64000&max_untracked_memory=0"
    "&interactive_delay=3600000000"
)
allocation_query = "SELECT length(range(number + 100000)) FROM numbers(8)"


def request(parameters, query, url=base_url):
    return subprocess.run(
        curl + ["-sS", url + parameters, "--data-binary", query],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8")


def packets(response, framing):
    if framing != "EventStream":
        return [json.loads(line) for line in response.splitlines() if line]
    result = []
    for event in response.split("\n\n"):
        if not event:
            continue
        lines = event.splitlines()
        assert len(lines) == 2, event
        assert lines[0].startswith("event: "), event
        assert lines[1].startswith("data: "), event
        kind = lines[0][7:]
        value = lines[1][6:]
        result.append({"packet": kind, kind: value if kind == "data" else json.loads(value)})
    return result


def check_samples(items, expected_query_id=None, terminal="progress", allow_missing=False):
    assert items and items[-1]["packet"] == terminal, items[-1:]
    assert all(item["packet"] != "profile_events" for item in items), "profile events were not disabled"
    batches = [item["profile_traces"] for item in items if item["packet"] == "profile_traces"]
    assert all(0 < len(batch) <= 1024 for batch in batches), "unbounded or empty batch"
    samples = [sample for batch in batches for sample in batch]
    for sample in samples:
        assert set(sample) == {
            "host_name", "query_id", "trace_type", "thread_id",
            "event_time_microseconds", "trace", "symbols", "size",
        }, sample
        assert isinstance(sample["host_name"], str) and sample["host_name"], sample
        if expected_query_id is not None:
            assert sample["query_id"] == expected_query_id, sample
        for key in ("thread_id", "event_time_microseconds", "size"):
            assert isinstance(sample[key], str), (key, sample)
            int(sample[key])
        if sample["trace_type"] in {"Dropped", "Incomplete"}:
            assert not sample["trace"] and not sample["symbols"], sample
            assert int(sample["thread_id"]) == int(sample["event_time_microseconds"]) == 0, sample
            assert int(sample["size"]) > 0 if sample["trace_type"] == "Dropped" else int(sample["size"]) == 0, sample
            continue
        assert sample["trace_type"] in {"CPU", "Real", "Memory", "MemorySample", "MemoryPeak"}, sample
        assert int(sample["thread_id"]) > 0 and int(sample["event_time_microseconds"]) > 0, sample
        assert sample["trace"] and len(sample["trace"]) == len(sample["symbols"]), sample
        assert all(isinstance(address, str) and int(address) >= 0 for address in sample["trace"]), sample
        assert all(isinstance(symbol, str) for symbol in sample["symbols"]), sample
    if terminal == "exception":
        assert "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" in items[-1]["exception"], items[-1]
        assert not any(
            item["packet"] == "progress" and "result_rows" in item["progress"] for item in items
        ), "failed query emitted final progress"
    observed = any(sample["trace_type"] == "MemorySample" for sample in samples)
    assert observed or allow_missing, "no memory samples"
    return observed


def check_query_samples(parameters, query, framing, expected_query_id=None, terminal="progress", url=base_url):
    # The shared profiler pipe can discard every sample of a short query under load.
    for attempt in range(4):
        items = packets(request(parameters, query, url), framing)
        if check_samples(items, expected_query_id, terminal, allow_missing=True):
            return
        if attempt < 3:
            time.sleep(0.1)
    raise AssertionError("no profile trace packets in four query observations")


# Large allocations produce samples without depending on profiler timer scheduling.
# A long interactive delay makes every sample depend on the final collector barrier and drain.
for framing in ("JSONEachPacketString", "JSONEachPacketBase64", "EventStream"):
    query_id = "framing_traces_" + uuid.uuid4().hex
    parameters = f"&framing_output_format={framing}&send_profile_traces=1&query_id={query_id}"
    check_query_samples(parameters, allocation_query + " FORMAT Null", framing, query_id)
    print(f"{framing}: bounded samples and final progress")

framing = "JSONEachPacketString"
parameters = f"&framing_output_format={framing}"
for setting in ("", "&send_profile_traces=0"):
    items = packets(request(parameters + setting, allocation_query + " FORMAT Null"), framing)
    assert items[-1]["packet"] == "progress", items
    assert not any(item["packet"] == "profile_traces" for item in items), items
print("default and explicit disable: no samples")

items = packets(request(
    parameters + "&send_profile_traces=1",
    allocation_query + " SETTINGS send_profile_traces=0 FORMAT Null",
), framing)
assert not any(item["packet"] == "profile_traces" for item in items), items
print("query settings disable URL capture")

check_query_samples(
    "",
    allocation_query + " SETTINGS framing_output_format='JSONEachPacketString', send_profile_traces=1 FORMAT Null",
    framing,
)
print("query settings enable framing and samples")

plain_query = "SELECT length(range(number + 100000)) FROM numbers(1)"
assert request("&send_profile_traces=1", plain_query + " FORMAT TSV") == "100000\n"
assert request(
    parameters + "&send_profile_traces=1",
    plain_query + " SETTINGS framing_output_format='None' FORMAT TSV",
) == "100000\n"
print("plain HTTP and query settings disable framing")

for buffering in (0, 1):
    check_query_samples(
        parameters + f"&send_profile_traces=1&http_wait_end_of_query={buffering}",
        "SELECT length(range(number + 100000)), throwIf(number = 4) FROM numbers(8) FORMAT Null",
        framing, terminal="exception",
    )
    print(f"buffering={buffering}: samples precede terminal exception")

query_id = "framing_traces_" + uuid.uuid4().hex + "_"
check_query_samples(
    parameters + f"&send_profile_traces=1&query_id={query_id}%FF",
    allocation_query + " FORMAT Null",
    framing, query_id + "\ufffd",
)
print("query identifiers are sanitized to UTF-8")

restricted_user = "framing_traces_user_" + uuid.uuid4().hex
subprocess.run(client + ["--query", f"CREATE USER {restricted_user} IDENTIFIED WITH no_password"], check=True)
try:
    parts = urlsplit(base_url)
    restricted_parameters = [(key, value) for key, value in parse_qsl(parts.query) if key not in ("user", "password")]
    restricted_parameters.append(("user", restricted_user))
    restricted_url = urlunsplit(parts._replace(query=urlencode(restricted_parameters)))
    denied = packets(request(parameters, "SELECT count() FROM system.trace_log", restricted_url), framing)
    assert denied[-1]["packet"] == "exception" and "ACCESS_DENIED" in denied[-1]["exception"], denied
    check_query_samples(
        parameters + "&send_profile_traces=1",
        "SELECT ignore(range(1000000)) FORMAT Null",
        framing, url=restricted_url,
    )
finally:
    subprocess.run(client + ["--query", f"DROP USER {restricted_user}"], check=True)
print("samples do not require access to system.trace_log")

# Verify a sample reaches the client while its query remains in the process list.
query_id = "framing_traces_live_" + uuid.uuid4().hex
live_query = "SELECT length(range(number + 100000)), sleepEachRow(0.05) FROM numbers(100) FORMAT JSONEachRow"
process = subprocess.Popen(
    curl + [
        "-sS", "-N", base_url + parameters
        + f"&send_profile_traces=1&query_id={query_id}&interactive_delay=20000",
        "--data-binary", live_query,
    ],
    stdout=subprocess.PIPE,
    text=True,
)
items = []
observed_running = False
for line in process.stdout:
    packet = json.loads(line)
    items.append(packet)
    if packet["packet"] == "profile_traces" and not observed_running:
        count = subprocess.run(
            client + ["--query", f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
        observed_running = int(count) > 0
assert process.wait() == 0
check_samples(items, query_id)
assert observed_running, "samples were delivered only after the query finished"
print("samples arrive while the query is running")
PY
