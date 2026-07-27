#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP listener and the url() table function

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ReadWriteBufferFromHTTP::doWithRetries must stop retrying once the query is cancelled.
# The listener accepts the connection and never answers, so every attempt ends with
# a receive timeout. With http_max_tries = 10, http_receive_timeout = 2 and the backoff
# below, the whole loop takes ~50s when cancellation is ignored, and a few seconds when
# it is honoured.

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
trap 'kill $LISTENER_PID 2>/dev/null ||:; wait $LISTENER_PID 2>/dev/null ||:' EXIT

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

RETRY_SETTINGS="http_receive_timeout = 2, http_max_tries = 10, http_retry_initial_backoff_ms = 100, http_retry_max_backoff_ms = 10000"
# The loop needs at least ~50s when cancellation is ignored; allow a generous margin
# for a loaded CI host while still failing the unfixed build.
LIMIT_MS=25000

echo "--- max_execution_time ---"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 3, $RETRY_SETTINGS
" 2>&1 | grep -o -m1 -e TIMEOUT_EXCEEDED -e POCO_EXCEPTION
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
if [ "$ELAPSED_MS" -lt "$LIMIT_MS" ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

echo "--- KILL QUERY ---"
QUERY_ID="04648_kill_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID" --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 0, $RETRY_SETTINGS
" > /dev/null 2> "${CLICKHOUSE_TMP}/04648_kill_err.txt" &
CLIENT_PID=$!

# Wait until the query has entered the retry loop (i.e. the first attempt already timed out),
# so the cancellation lands on the retry path rather than before the first attempt.
for _ in $(seq 1 100); do
    RETRIES=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'] FROM system.processes WHERE query_id = '$QUERY_ID'")
    if [ "${RETRIES:-0}" -ge 2 ]; then break; fi
    sleep 0.2
done
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null
wait $CLIENT_PID 2>/dev/null ||:
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
grep -o -m1 -e QUERY_WAS_CANCELLED -e POCO_EXCEPTION "${CLICKHOUSE_TMP}/04648_kill_err.txt"
if [ "$ELAPSED_MS" -lt "$LIMIT_MS" ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

rm -f "${CLICKHOUSE_TMP}/04648_kill_err.txt"
