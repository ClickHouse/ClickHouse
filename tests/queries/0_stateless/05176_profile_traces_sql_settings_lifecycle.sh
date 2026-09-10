#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

python3 - "$CLICKHOUSE_URL" "$CLICKHOUSE_DATABASE" <<'PY'
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

base_url = urllib.parse.urlsplit(sys.argv[1])
database = sys.argv[2].replace("`", "``")
source = f"`{database}`.profile_sql_source"
restored = f"`{database}`.profile_sql_restored"
backup_prefix = "profile_sql_" + uuid.uuid4().hex
settings = {
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 1,
    "memory_profiler_sample_min_allocation_size": 65536,
    "max_untracked_memory": 0,
    "max_threads": 1,
    "max_insert_threads": 1,
    "max_block_size": 65536,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "output_format_parallel_formatting": 0,
}


def request(query, options):
    parameters = dict(urllib.parse.parse_qsl(base_url.query))
    parameters.update(settings)
    parameters.update(options)
    parameters["query_id"] = "profile_sql_" + uuid.uuid4().hex
    url = urllib.parse.urlunsplit(base_url._replace(query=urllib.parse.urlencode(parameters)))
    try:
        response = urllib.request.urlopen(urllib.request.Request(url, data=query.encode()), timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.read().decode(), parameters["query_id"]


def control(query):
    status, body, _ = request(query, {"framing_output_format": "None", "send_profile_traces": 0, "memory_profiler_sample_probability": 0})
    assert status == 200, (status, body)
    return body.strip()


def run(query, *, sql_framing=False, url_traces=0, expected_status=None, error=False):
    status, body, query_id = request(
        query,
        {"framing_output_format": "None" if sql_framing else "JSONEachPacketString", "send_profile_traces": url_traces},
    )
    packets = [json.loads(line) for line in body.splitlines() if line]
    assert packets, (status, body)
    terminal = "exception" if error else "progress"
    assert packets[-1]["packet"] == terminal, (status, packets[-1])
    if error:
        assert status != 200 and "UNKNOWN_TABLE" in packets[-1]["exception"], (status, packets[-1])
        assert not any(packet["packet"] == "data" for packet in packets), "analysis error emitted result data"
    else:
        assert status == 200, (status, body)
        payload = "".join(packet.get("data", "") for packet in packets)
        assert expected_status in payload, payload
    batches = [packet["profile_traces"] for packet in packets if packet["packet"] == "profile_traces"]
    assert all(0 < len(batch) <= 1024 for batch in batches), [len(batch) for batch in batches]
    samples = [sample for batch in batches for sample in batch]
    assert all(sample["query_id"] == query_id for sample in samples), "trace subscription crossed queries"
    assert not any(sample["trace_type"] in {"Dropped", "Incomplete"} for sample in samples), "small query lost samples"
    return samples


def require_worker_allocation(samples, operation):
    # The starter executes in a background thread inherited from the query's group.
    # Its allocations distinguish real interpreter work from trailing response samples.
    symbol = f"DB::BackupsWorker::{operation}Starter::do{operation}"
    assert any(
        sample["trace_type"] == "MemorySample"
        and int(sample["size"]) > 0
        and any(symbol in frame for frame in sample["symbols"])
        and not any("DB::HTTPHandler::" in frame for frame in sample["symbols"])
        for sample in samples
    ), f"no background {operation} allocation among {len(samples)} trace rows"


try:
    control(f"CREATE TABLE {source} (x UInt64) ENGINE=Memory")
    control(f"INSERT INTO {source} SELECT number FROM numbers(100000)")
    for name, url_traces, sql_settings, sql_framing in (
        ("url", 1, "", False),
        ("sql_trace", 0, " SETTINGS send_profile_traces=1", False),
        ("sql_both", 0, " SETTINGS framing_output_format='JSONEachPacketString', send_profile_traces=1", True),
    ):
        samples = run(
            f"BACKUP TABLE {source} TO Disk('backups', '{backup_prefix}_{name}')" + sql_settings,
            url_traces=url_traces,
            sql_framing=sql_framing,
            expected_status="BACKUP_CREATED",
        )
        require_worker_allocation(samples, "Backup")
        print(f"{name} BACKUP captures background allocations")

    samples = run(
        f"RESTORE TABLE {source} AS {restored} FROM Disk('backups', '{backup_prefix}_sql_both') SETTINGS framing_output_format='JSONEachPacketString', send_profile_traces=1",
        sql_framing=True,
        expected_status="RESTORED",
    )
    require_worker_allocation(samples, "Restore")
    assert control(f"SELECT count(), sum(x) FROM {restored} FORMAT TSV") == "100000\t4999950000"
    print("SQL RESTORE captures background allocations and restores rows")

    samples = run(
        f"BACKUP TABLE {source} TO Disk('backups', '{backup_prefix}_disabled') SETTINGS send_profile_traces=0",
        url_traces=1,
        expected_status="BACKUP_CREATED",
    )
    assert not samples, "SQL opt-out emitted traces"
    print("SQL opt-out discards the URL trace subscription")

    run(
        f"SELECT * FROM `{database}`.profile_sql_missing SETTINGS framing_output_format='JSONEachPacketString', send_profile_traces=1",
        sql_framing=True,
        error=True,
    )
    print("SQL opt-in preserves the original analysis exception")

    status, body, _ = request(
        "SELECT 42 SETTINGS send_profile_traces=1 FORMAT TSV",
        {"framing_output_format": "None", "send_profile_traces": 1},
    )
    assert status == 200 and body == "42\n", (status, body)
    print("plain HTTP remains unframed with trace delivery enabled")
finally:
    control(f"DROP TABLE IF EXISTS {restored} SYNC")
    control(f"DROP TABLE IF EXISTS {source} SYNC")
PY
