#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_URL" "$CLICKHOUSE_CURL" "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" <<'PY'
import json
import shlex
import subprocess
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

base_url, curl_command, client_command, host, port = sys.argv[1:]
curl = shlex.split(curl_command)
client = shlex.split(client_command)
address = f"{host}:{port}".replace("'", "''")


def query(hedged, nested=None, outer=None):
    if nested is None:
        sql = f"SELECT value FROM remote('{address}', system.settings) WHERE name='send_profile_traces'"
    else:
        sql = (
            f"SELECT value FROM remote('{address}', view(SELECT value FROM system.settings "
            f"WHERE name='send_profile_traces' SETTINGS send_profile_traces={nested}))"
        )
    sql += (
        " FORMAT TabSeparated SETTINGS prefer_localhost_replica=0, "
        f"use_hedged_requests={hedged}, max_threads=1, send_profile_events=0, send_logs_level='none', "
        "query_profiler_cpu_time_period_ns=0, query_profiler_real_time_period_ns=0, "
        "memory_profiler_sample_probability=0, memory_profiler_step=0"
    )
    if outer:
        sql += ", " + outer
    return sql


def http(sql, framing, enabled, expected=None, expect_error=False):
    parts = urlsplit(base_url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params.update(
        framing_output_format=framing,
        send_profile_traces=str(enabled),
        send_profile_events="0",
        send_logs_level="none",
        query_profiler_cpu_time_period_ns="0",
        query_profiler_real_time_period_ns="0",
        memory_profiler_sample_probability="0",
        memory_profiler_step="0",
    )
    url = urlunsplit(parts._replace(query=urlencode(params)))
    result = subprocess.run(
        curl + ["--silent", "--show-error", "--write-out", "\n%{http_code}", "--data-binary", sql, url],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    body, status = result.stdout.rsplit("\n", 1)
    if body.startswith("{"):
        packets = [json.loads(line) for line in body.splitlines()]
        failed = any(packet["packet"] == "exception" for packet in packets)
        value = "".join(packet["data"] for packet in packets if packet["packet"] == "data")
    else:
        failed = int(status) >= 400
        value = body
    if expect_error:
        assert failed and "CANNOT_PARSE_BOOL" in body, (status, body)
    else:
        assert not failed and int(status) == 200, (status, body)
        assert value == f"{expected}\n", (expected, value)


for hedged in (0, 1):
    http(query(hedged), "None", 1, "0")
    http(query(hedged, outer="send_profile_traces=1"), "None", 0, "0")
    http(query(hedged, nested="1"), "None", 0, "0")
    http(query(hedged), "JSONEachPacketString", 1, "1")
    http(
        query(hedged, nested="'true'", outer="framing_output_format='JSONEachPacketString', send_profile_traces=1"),
        "None", 0, "1",
    )
    http(query(hedged, nested="'1'", outer="send_profile_traces=0"), "JSONEachPacketString", 1, "0")
    http(query(hedged, nested="1", outer="send_profile_traces=DEFAULT"), "JSONEachPacketString", 1, "0")
    for opt_out in ("0", "'false'"):
        http(query(hedged, nested=opt_out), "JSONEachPacketString", 1, "0")
    http(query(hedged, nested="'not-a-bool'"), "JSONEachPacketString", 1, expect_error=True)

    for nested, expected in ((None, "1"), ("0", "0")):
        result = subprocess.run(
            client + ["--query", query(hedged, nested=nested, outer="send_profile_traces=1")],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.stdout == f"{expected}\n", (expected, result.stdout, result.stderr)
    print(f"hedged={hedged}: plain HTTP disabled, supported transports and SQL opt-outs preserved")
PY
