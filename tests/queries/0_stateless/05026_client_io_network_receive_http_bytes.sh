#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NetworkReceiveBytes (client-fed INSERT), WriteBufferFromHTTPBytes and ReadWriteBufferFromHTTPBytes (url() write/read)
# must reach the client's ProfileEvents stream, which ClientBase::onProfileEvents sums into the live IO rate.

# Prints the thread-group total of one counter as the client received it in the ProfileEvents stream.
function client_total()
{
    grep -o "\[ 0 \] $1: [0-9]*" | tail -n 1 | awk '{ print $NF + 0 }'
}

# Whether the outer query's query_log entry (by query_id: the HTTP requests below get their own ids)
# carries a nonzero counter.
function query_log_counter_positive()
{
    ${CLICKHOUSE_CLIENT} -q "
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['$2'] > 0 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND query_id = '$1'"
}

http_url="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${CLICKHOUSE_DATABASE}"
query_id_prefix="05026_client_io_network_receive_http_bytes_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05026_io_upload (x UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05026_io_http_write (x UInt64) ENGINE = Memory"

# (a) A client-fed INSERT: the data packets read from the socket count as NetworkReceiveBytes.
upload_bytes=$(seq 1 100000 | ${CLICKHOUSE_CLIENT} --query_id "${query_id_prefix}_upload" \
    --print-profile-events --profile-events-delay-ms=-1 \
    -q "INSERT INTO t_05026_io_upload FORMAT TSV" 2>&1 | client_total NetworkReceiveBytes)
echo "client-fed INSERT streams NetworkReceiveBytes to the client: $(( ${upload_bytes:-0} > 0 ))"
echo "client-fed INSERT logs NetworkReceiveBytes: $(query_log_counter_positive "${query_id_prefix}_upload" NetworkReceiveBytes)"
${CLICKHOUSE_CLIENT} -q "SELECT 'client-fed INSERT rows', count() FROM t_05026_io_upload"

# (b) A write through url() to the server's own HTTP endpoint: the request body goes out through
# WriteBufferFromHTTP, whose payload bytes count as WriteBufferFromHTTPBytes.
http_write_bytes=$(${CLICKHOUSE_CLIENT} --query_id "${query_id_prefix}_http_write" \
    --print-profile-events --profile-events-delay-ms=-1 \
    -q "INSERT INTO FUNCTION url('${http_url}&query=INSERT+INTO+t_05026_io_http_write+FORMAT+TSV', 'TSV', 'x UInt64')
        SELECT number FROM numbers(100000)" 2>&1 | client_total WriteBufferFromHTTPBytes)
echo "url() write streams WriteBufferFromHTTPBytes to the client: $(( ${http_write_bytes:-0} > 0 ))"
echo "url() write logs WriteBufferFromHTTPBytes: $(query_log_counter_positive "${query_id_prefix}_http_write" WriteBufferFromHTTPBytes)"
${CLICKHOUSE_CLIENT} -q "SELECT 'url() write rows', count() FROM t_05026_io_http_write"

# (c) A read through url(): the response body comes in through ReadWriteBufferFromHTTP.
http_read_bytes=$(${CLICKHOUSE_CLIENT} --query_id "${query_id_prefix}_http_read" \
    --print-profile-events --profile-events-delay-ms=-1 \
    -q "SELECT count() FROM url('${http_url}&query=SELECT+number+FROM+numbers(100000)+FORMAT+TSV', 'TSV', 'x UInt64') FORMAT Null" \
    2>&1 | client_total ReadWriteBufferFromHTTPBytes)
echo "url() read streams ReadWriteBufferFromHTTPBytes to the client: $(( ${http_read_bytes:-0} > 0 ))"
echo "url() read logs ReadWriteBufferFromHTTPBytes: $(query_log_counter_positive "${query_id_prefix}_http_read" ReadWriteBufferFromHTTPBytes)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05026_io_upload"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_05026_io_http_write"
