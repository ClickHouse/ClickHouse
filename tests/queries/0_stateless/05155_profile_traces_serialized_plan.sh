#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_URL" "$CLICKHOUSE_CURL" "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" <<'PY'
import shlex
import subprocess
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

base_url, curl_command, client_command, host, port = sys.argv[1:]
curl = shlex.split(curl_command)
client = shlex.split(client_command)
address = f"{host}:{port}".replace("'", "''")


def query(hedged, enabled, nested):
    return (
        f"SELECT value FROM remote('{address}', view(SELECT value FROM system.settings "
        f"WHERE name='send_profile_traces' SETTINGS send_profile_traces={nested})) "
        "FORMAT TabSeparated SETTINGS enable_analyzer=1, serialize_query_plan=1, "
        "distributed_group_by_no_merge=0, prefer_localhost_replica=0, max_parallel_replicas=1, "
        f"use_hedged_requests={hedged}, send_profile_traces={enabled}, max_threads=1, "
        "send_profile_events=0, send_logs_level='none', query_profiler_cpu_time_period_ns=0, "
        "query_profiler_real_time_period_ns=0, memory_profiler_sample_probability=0, memory_profiler_step=0"
    )


def native(sql, expected):
    result = subprocess.run(
        client + ["--query", sql],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.stdout == f"{expected}\n", (sql, expected, result.stdout, result.stderr)


def plain_http(sql):
    parts = urlsplit(base_url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params.update(framing_output_format="None")
    url = urlunsplit(parts._replace(query=urlencode(params)))
    result = subprocess.run(
        curl + ["--silent", "--show-error", "--write-out", "\n%{http_code}", "--data-binary", sql, url],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    body, status = result.stdout.rsplit("\n", 1)
    assert status == "200" and body == "0\n", (sql, status, body)


for hedged in (0, 1):
    native(query(hedged, enabled=1, nested=1), "1")
    native(query(hedged, enabled=1, nested=0), "0")
    native(query(hedged, enabled=0, nested=1), "0")
    plain_http(query(hedged, enabled=0, nested=1))
    plain_http(query(hedged, enabled=1, nested=1))
    print(f"hedged={hedged}: serialized plans preserve trace transport gating and SQL opt-outs")
PY
