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
setting = "send_profile_traces"
clauses = (
    (f"{setting}=0, {setting}=1", "1"),
    (f"{setting}=1, {setting}=0", "0"),
    (f"{setting}=0, {setting}", "1"),
    (f"{setting}, {setting}=0", "0"),
    (f"{setting}='false', {setting}='true'", "1"),
    (f"{setting}=0, {setting}=1, {setting}=DEFAULT", "1"),
    (f"{setting}=DEFAULT, {setting}=0, {setting}=1", "1"),
)


def query(clause, serialized=0, hedged=0, enabled="1", local=False):
    # A compatibility clause preserves changes matching the inherited value during constraint checks.
    inner = f"SELECT value FROM system.settings WHERE name='{setting}' SETTINGS compatibility='26.9', {clause}"
    table = f"view({inner})" if local else f"remote('{address}', view({inner}))"
    return (
        f"SELECT * FROM {table} FORMAT TabSeparated SETTINGS "
        f"serialize_query_plan={serialized}, use_hedged_requests={hedged}, {setting}={enabled}, "
        "prefer_localhost_replica=0, max_parallel_replicas=1, distributed_group_by_no_merge=0, max_threads=1, "
        "send_profile_events=0, send_logs_level='none', query_profiler_cpu_time_period_ns=0, "
        "query_profiler_real_time_period_ns=0, memory_profiler_sample_probability=0, memory_profiler_step=0"
    )


def native_batch(cases):
    # Each statement has query-local settings; reuse the client to bound sanitizer startup costs.
    sql = ";\n".join(sql for sql, _ in cases)
    expected = "".join(f"{value}\n" for _, value in cases)
    result = subprocess.run(client + ["--query", sql], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0 and result.stdout == expected, (sql, expected, result.stdout, result.stderr)


def invalid_native(sql):
    result = subprocess.run(client + ["--query", sql], capture_output=True, text=True, timeout=30)
    assert result.returncode != 0 and "(CANNOT_PARSE_BOOL)" in result.stderr, (sql, result.returncode, result.stderr)


def plain_http(sql):
    parts = urlsplit(base_url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params.update(framing_output_format="None")
    url = urlunsplit(parts._replace(query=urlencode(params)))
    result = subprocess.run(
        curl + ["--silent", "--show-error", "--write-out", "\n%{http_code}", "--data-binary", sql, url],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    body, status = result.stdout.rsplit("\n", 1)
    assert status == "200" and body == "0\n", (sql, status, body)


native_batch([(query(clause, local=True), expected) for clause, expected in clauses])
print("local duplicate and shorthand controls passed")

for serialized in (0, 1):
    for hedged in (0, 1):
        native_cases = [(query(clause, serialized, hedged), expected) for clause, expected in clauses]
        final_opt_in = clauses[0][0]
        native_cases.append((query(final_opt_in, serialized, hedged, enabled="0"), "0"))
        native_cases.append((query(final_opt_in, serialized, hedged, enabled="DEFAULT"), "0"))
        native_batch(native_cases)
        plain_http(query(final_opt_in, serialized, hedged))
        for invalid in (f"{setting}='not-a-bool', {setting}=1", f"{setting}=1, {setting}='not-a-bool'"):
            invalid_native(query(invalid, serialized, hedged))
        print(f"serialized={serialized} hedged={hedged}: duplicates, defaults, gating and invalid values preserved")
PY
