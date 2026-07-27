#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP listener and the url() table function

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ReadWriteBufferFromHTTP::doWithRetries must stop retrying once the query is cancelled.
# The listener accepts every connection and never answers, so each attempt ends with a
# receive timeout. The two checks on the retry path are pinned separately:
#   - the check BEFORE the backoff sleep, by the first section: cancellation is observed
#     while attempt 1 is still reading, so the loop must throw as soon as that attempt
#     fails instead of sleeping out a 20s backoff first (pinned by the elapsed time);
#   - the check AFTER the backoff sleep, by the second section: the query is killed while
#     the loop is inside a 20s backoff, so it must throw when the sleep ends with exactly
#     one request sent, instead of starting another attempt (pinned by the request count).
# The second section does not derive that phase from a clock started before the query ran:
# it waits until the server itself reports one request sent and more elapsed time than the
# receive timeout, so the backoff phase is observed rather than assumed.
# http_make_head_request = 0 keeps the request counts exact: it defaults to true and the
# stress runner randomizes it, and a HEAD probe issues its own requests through the same
# counter.
# parallel_replicas_for_cluster_engines = 0 keeps the read on the initiator. With parallel
# replicas enabled, url() is served by StorageURLCluster, so the HTTP reads happen in
# secondary queries on the replicas and neither system.processes nor the initiator's
# system.query_log row ever reports ReadWriteBufferFromHTTPRequestsSent.

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
# CLICKHOUSE_TMP is shared between concurrently running copies of the suite when the runner
# is given --database, so every temp file is scoped with the test database name.
BACKOFF_ERR="${CLICKHOUSE_TMP}/04648_backoff_err_${CLICKHOUSE_DATABASE}.txt"
PHASE_LOG="${CLICKHOUSE_TMP}/04648_phase_${CLICKHOUSE_DATABASE}.txt"
trap 'kill $LISTENER_PID 2>/dev/null ||:; wait $LISTENER_PID 2>/dev/null ||:; rm -f "$BACKOFF_ERR" "$PHASE_LOG"' EXIT

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

# Cancellation is observed while attempt 1 is still reading: max_execution_time fires at 5s,
# long before the 10s receive timeout expires, so only the check before the backoff sleep can
# stop the loop before it sleeps for 20s.
echo "--- cancelled during a read ---"
QUERY_ID_READ="04648_read_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_READ" --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 5, http_receive_timeout = 10, http_max_tries = 3,
             http_retry_initial_backoff_ms = 20000, http_retry_max_backoff_ms = 30000,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0
" 2>&1 | grep -o -m1 -e TIMEOUT_EXCEEDED -e POCO_EXCEPTION
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
requests_sent "$QUERY_ID_READ"
if [ "$ELAPSED_MS" -lt 20000 ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

# The query is killed while the loop is inside a 20s backoff, so only the check after the
# sleep can stop it from sending a second request.
echo "--- KILL QUERY during a backoff ---"
QUERY_ID_BACKOFF="04648_backoff_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID_BACKOFF" --query "
    SELECT * FROM url('http://127.0.0.1:$PORT/', 'TSV', 'c1 UInt64')
    SETTINGS max_execution_time = 0, http_receive_timeout = 2, http_max_tries = 4,
             http_retry_initial_backoff_ms = 20000, http_retry_max_backoff_ms = 30000,
             http_make_head_request = 0, parallel_replicas_for_cluster_engines = 0
" > /dev/null 2> "$BACKOFF_ERR" &
CLIENT_PID=$!

# Wait until the server reports that one request has been sent AND that more time has passed
# than the 2s receive timeout: attempt 1 has then already failed and the loop is inside the
# 20s backoff, which leaves ample slack for the KILL to be delivered before the sleep ends.
BACKOFF_OBSERVED=0
for _ in $(seq 1 200); do
    PAIR=$(${CLICKHOUSE_CLIENT} --query "
        SELECT toString(ProfileEvents['ReadWriteBufferFromHTTPRequestsSent']) || ' ' || toString(toUInt64(elapsed))
        FROM system.processes WHERE query_id = '$QUERY_ID_BACKOFF'")
    REQUESTS=${PAIR%% *}
    SECONDS_ELAPSED=${PAIR##* }
    if [ "${REQUESTS:-0}" -ge 1 ] && [ "${SECONDS_ELAPSED:-0}" -ge 6 ]; then
        BACKOFF_OBSERVED=1
        echo "accepted requests=$REQUESTS elapsed=${SECONDS_ELAPSED}s" >> "$PHASE_LOG"
        break
    fi
    sleep 0.2
done
if [ "$BACKOFF_OBSERVED" -eq 1 ]; then echo "backoff phase observed"; fi
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$QUERY_ID_BACKOFF' SYNC" > /dev/null
wait $CLIENT_PID 2>/dev/null ||:
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
grep -o -m1 -e QUERY_WAS_CANCELLED -e POCO_EXCEPTION "$BACKOFF_ERR"
requests_sent "$QUERY_ID_BACKOFF"
if [ "$ELAPSED_MS" -lt 35000 ]; then echo "stopped early"; else echo "kept retrying for ${ELAPSED_MS} ms"; fi

rm -f "$BACKOFF_ERR" "$PHASE_LOG"
