#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `system.errors` keeps only the latest query ID for each error code.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

query_id_prefix="04648_handled_fallback_${CLICKHOUSE_DATABASE}_${RANDOM}"

http_port=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    retry_attempts = 0

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-Length', '2')
        self.end_headers()

    def do_GET(self):
        if self.path == '/ready':
            status = 200
            payload = b''
        elif self.path == '/retry':
            Handler.retry_attempts += 1
            status = 500 if Handler.retry_attempts == 1 else 200
            payload = b'' if status == 500 else b'1\\n'
        elif self.path.startswith('/always_fail'):
            status = 500
            payload = b''
        else:
            status = 200
            payload = b'1\\n'

        self.send_response(status)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', ${http_port}), Handler).serve_forever()
" &
http_pid=$!
trap 'kill ${http_pid} 2>/dev/null || true; wait ${http_pid} 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:${http_port}/ready" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

run_clean_query()
{
    local suffix=$1
    local query=$2
    local query_id="${query_id_prefix}_${suffix}"

    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
    $CLICKHOUSE_CLIENT --query "SELECT count() = 0 FROM system.errors WHERE query_id = '${query_id}'"
    $CLICKHOUSE_CLIENT --query "SELECT count() = 0 FROM system.error_log WHERE last_error_query_id = '${query_id}'"
}

run_clean_query format_query "SELECT formatQueryOrNull('SELECT (') IS NULL"
run_clean_query readable_size "SELECT parseReadableSizeOrNull('invalid') IS NULL"
run_clean_query case_fallback "SELECT CASE 0 WHEN 0 THEN 1::Int128 WHEN 1 THEN 2::Int128 ELSE 3::Int128 END"
run_clean_query accurate_cast "SELECT accurateCastOrNull('not_bool', 'Bool') IS NULL"
run_clean_query csv_default "SELECT x FROM format(CSV, 'x UInt64', 'bad') SETTINGS input_format_csv_use_default_on_bad_values = 1"
run_clean_query csv_skip "SELECT groupArray(x) FROM format(CSV, 'x UInt64', '1\nbad\n2') SETTINGS input_format_allow_errors_num = 1"
run_clean_query http_retry "SELECT x FROM url('http://127.0.0.1:${http_port}/retry', 'TSV', 'x UInt8') SETTINGS http_max_tries = 2, http_retry_initial_backoff_ms = 1"
run_clean_query url_failover "SELECT x FROM url('http://127.0.0.1:${http_port}/{always_fail|data}', 'TSV', 'x UInt8') SETTINGS http_max_tries = 1, send_logs_level = 'fatal'"

terminal_http_query_id="${query_id_prefix}_terminal_http"
terminal_http_error_count_before=$($CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.errors WHERE name = 'RECEIVED_ERROR_FROM_REMOTE_IO_SERVER' AND NOT remote")
if $CLICKHOUSE_CLIENT --query_id "$terminal_http_query_id" --query "SELECT x FROM url('http://127.0.0.1:${http_port}/always_fail', 'TSV', 'x UInt8') SETTINGS http_max_tries = 2, http_retry_initial_backoff_ms = 1" >/dev/null 2>&1; then
    echo "The terminal HTTP request unexpectedly succeeded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SELECT value = ${terminal_http_error_count_before} + 1 FROM system.errors WHERE name = 'RECEIVED_ERROR_FROM_REMOTE_IO_SERVER' AND NOT remote AND query_id = '${terminal_http_query_id}'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT --query "SELECT sum(value) = 1 FROM system.error_log WHERE error = 'RECEIVED_ERROR_FROM_REMOTE_IO_SERVER' AND NOT remote AND last_error_query_id = '${terminal_http_query_id}'"

terminal_failover_query_id="${query_id_prefix}_terminal_failover"
terminal_failover_error_count_before=$($CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.errors WHERE name = 'NETWORK_ERROR' AND NOT remote")
if $CLICKHOUSE_CLIENT --query_id "$terminal_failover_query_id" --query "SELECT x FROM url('http://127.0.0.1:${http_port}/{always_fail|always_fail2}', 'TSV', 'x UInt8') SETTINGS http_max_tries = 1" >/dev/null 2>&1; then
    echo "The terminal URL failover unexpectedly succeeded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SELECT value = ${terminal_failover_error_count_before} + 1 FROM system.errors WHERE name = 'NETWORK_ERROR' AND NOT remote AND query_id = '${terminal_failover_query_id}'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT --query "SELECT sum(value) = 1 FROM system.error_log WHERE error = 'NETWORK_ERROR' AND NOT remote AND last_error_query_id = '${terminal_failover_query_id}'"
$CLICKHOUSE_CLIENT --query "SELECT count() = 0 FROM system.error_log WHERE error != 'NETWORK_ERROR' AND last_error_query_id = '${terminal_failover_query_id}'"

strict_query_id="${query_id_prefix}_strict"
strict_error_count_before=$($CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.errors WHERE name = 'CANNOT_PARSE_NUMBER' AND NOT remote")
if $CLICKHOUSE_CLIENT --query_id "$strict_query_id" --query "SELECT parseReadableSize('invalid')" >/dev/null 2>&1; then
    echo "The strict parser unexpectedly succeeded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SELECT value = ${strict_error_count_before} + 1 FROM system.errors WHERE name = 'CANNOT_PARSE_NUMBER' AND NOT remote AND query_id = '${strict_query_id}'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT --query "SELECT sum(value) = 1 FROM system.error_log WHERE error = 'CANNOT_PARSE_NUMBER' AND NOT remote AND last_error_query_id = '${strict_query_id}'"

strict_csv_query_id="${query_id_prefix}_strict_csv"
strict_csv_error_count_before=$($CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.errors WHERE name = 'INCORRECT_DATA' AND NOT remote")
if $CLICKHOUSE_CLIENT --query_id "$strict_csv_query_id" --query "SELECT x FROM format(CSV, 'x UInt64', 'bad')" >/dev/null 2>&1; then
    echo "The strict CSV parser unexpectedly succeeded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SELECT value = ${strict_csv_error_count_before} + 1 FROM system.errors WHERE name = 'INCORRECT_DATA' AND NOT remote AND query_id = '${strict_csv_query_id}'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT --query "SELECT sum(value) = 1 FROM system.error_log WHERE error = 'INCORRECT_DATA' AND NOT remote AND last_error_query_id = '${strict_csv_query_id}'"
