#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP listener and the url() table function

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ReadWriteBufferFromHTTP::doWithRetries must stop retrying once the query is cancelled.
# The listener accepts every connection and never answers, so each attempt ends with a
# receive timeout. The two checks on the retry path are pinned separately:
#   - the check BEFORE the backoff sleep, by the first section: cancellation happens while
#     attempt 1 is still reading, so the loop must throw as soon as that attempt fails
#     instead of sleeping out a 20s backoff first (pinned by the elapsed time);
#   - the check AFTER the backoff sleep, by the second section: cancellation happens during
#     a 9s backoff, so the loop must throw when the sleep ends with exactly one request
#     sent, instead of starting another attempt (pinned by the request count).
# http_make_head_request = 0 keeps the request counts exact: it defaults to true and the
# stress runner randomizes it, and a HEAD probe issues its own requests through the same
# counter.

PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import socket
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', $PORT))
srv.listen(128)
accepted = []
while True:
    conn, _ = srv.accept()
    accepted.append(conn)  # hold the connection open and never write a response
" &
LISTENER_PID=$!
KILL_ERR="${CLICKHOUSE_TMP}/04648_kill_err_${CLICKHOUSE_DATABASE}.txt"
trap 'kill $LISTENER_PID 2>/dev/null ||:; wait $LISTENER_PID 2>/dev/null ||:; rm -f "$KILL_ERR"' EXIT

for _ in $(seq 1 100); do
    python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(1)
try:
    s.connect(('127.0.0.1', $PORT))
except OSError:
    sys.exit(1)
s.close()
" && break
    sleep 0.1
done

# How many HTTP requests the finished query actually sent.
requests_sent()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT 'requests sent: ' || toString(ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'])
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

# Cancellation lands while attempt 1 is still reading, so only the check before the backoff
# sleep can stop the loop before a 20s sleep.
echo "--- cancelled during a read ---"
QUERY_ID_READ="04648_read_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_READ" --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 1, http_receive_timeout = 2, http_max_tries = 3,
             http_retry_initial_backoff_ms = 20000, http_retry_max_backoff_ms = 30000,
             http_make_head_request = 0
" 2>&1 | grep -o -m1 -e TIMEOUT_EXCEEDED -e POCO_EXCEPTION
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
requests_sent "$QUERY_ID_READ"
if [ "$ELAPSED_MS" -lt 12000 ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

# Cancellation lands during the 9s backoff, so only the check after the sleep can stop the
# loop from sending a second request.
echo "--- cancelled during a backoff ---"
QUERY_ID_BACKOFF="04648_backoff_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_BACKOFF" --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 5, http_receive_timeout = 2, http_max_tries = 4,
             http_retry_initial_backoff_ms = 9000, http_retry_max_backoff_ms = 10000,
             http_make_head_request = 0
" 2>&1 | grep -o -m1 -e TIMEOUT_EXCEEDED -e POCO_EXCEPTION
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
requests_sent "$QUERY_ID_BACKOFF"
if [ "$ELAPSED_MS" -lt 25000 ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

# The same loop, cancelled by KILL QUERY instead of by a timeout.
echo "--- KILL QUERY ---"
QUERY_ID="04648_kill_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID" --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 0, http_receive_timeout = 2, http_max_tries = 4,
             http_retry_initial_backoff_ms = 9000, http_retry_max_backoff_ms = 10000,
             http_make_head_request = 0
" > /dev/null 2> "$KILL_ERR" &
CLIENT_PID=$!

# Wait until the first request is actually on the wire, so the cancellation lands while the
# read is in flight and is therefore observed on the retry path, not before the first attempt.
for _ in $(seq 1 100); do
    RETRIES=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'] FROM system.processes WHERE query_id = '$QUERY_ID'")
    if [ "${RETRIES:-0}" -ge 1 ]; then break; fi
    sleep 0.2
done
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null
wait $CLIENT_PID 2>/dev/null ||:
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
grep -o -m1 -e QUERY_WAS_CANCELLED -e POCO_EXCEPTION "$KILL_ERR"
requests_sent "$QUERY_ID"
if [ "$ELAPSED_MS" -lt 6000 ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

rm -f "$KILL_ERR"
