#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP listener

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A query that misses a cache-layout dictionary blocks in
# CacheDictionaryUpdateQueue::waitForCurrentUpdateFinish for up to
# QUERY_WAIT_TIMEOUT_MILLISECONDS. That wait must observe cancellation of the query
# (KILL QUERY, max_execution_time) instead of deferring it until the timeout expires.
# The listener accepts the connection and never answers, so the update never finishes.

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

# 30s wait budget: long enough that an unfixed build blows the limit below, short enough
# that the timeout arm of the test does not take a full minute.
WAIT_MS=30000
# A cancelled query must return in well under WAIT_MS. Generous, so a loaded CI host does
# not make this timing-fragile, while still failing an unfixed build (which waits 30s).
LIMIT_MS=15000

# http_max_tries = 1 keeps the abandoned update in the queue's worker short: the update the
# cancelled query walks away from keeps running there, and DROP DICTIONARY below joins that
# worker (CacheDictionaryUpdateQueue::stopAndWait -> update_pool.wait()).
${CLICKHOUSE_CLIENT} --query "
    DROP DICTIONARY IF EXISTS dict_04649;
    CREATE DICTIONARY dict_04649 (key UInt64, value String)
    PRIMARY KEY key
    SOURCE(HTTP(URL 'http://127.0.0.1:$PORT/' FORMAT 'TabSeparated'))
    LIFETIME(MIN 0 MAX 1)
    LAYOUT(CACHE(SIZE_IN_CELLS 10 QUERY_WAIT_TIMEOUT_MILLISECONDS $WAIT_MS))
    SETTINGS(http_receive_timeout = 120, http_max_tries = 1);
"

echo "--- max_execution_time ---"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query "
    SELECT dictGetString('dict_04649', 'value', toUInt64(1)) SETTINGS max_execution_time = 3
" 2>&1 | grep -o -m1 -e "Timeout exceeded" -e "source seems unavailable"
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
if [ "$ELAPSED_MS" -lt "$LIMIT_MS" ]; then echo "stopped early"; else echo "waited out the timeout: ${ELAPSED_MS} ms"; fi

echo "--- KILL QUERY ---"
QUERY_ID="04649_kill_${CLICKHOUSE_DATABASE}"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID" --query "
    SELECT dictGetString('dict_04649', 'value', toUInt64(2)) SETTINGS max_execution_time = 0
" > /dev/null 2> "${CLICKHOUSE_TMP}/04649_kill_err.txt" &
CLIENT_PID=$!

# Wait until the query is actually blocked in the dictionary update wait, so the
# cancellation lands on that wait rather than before it.
for _ in $(seq 1 200); do
    FOUND=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")
    if [ "${FOUND:-0}" -ge 1 ]; then break; fi
    sleep 0.1
done
${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null
wait $CLIENT_PID 2>/dev/null ||:
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
grep -o -m1 -e QUERY_WAS_CANCELLED -e "source seems unavailable" "${CLICKHOUSE_TMP}/04649_kill_err.txt"
if [ "$ELAPSED_MS" -lt "$LIMIT_MS" ]; then echo "stopped early"; else echo "waited out the timeout: ${ELAPSED_MS} ms"; fi

# Not cancelled: the configured wait budget must still be honoured, i.e. the slice loop
# must not extend (or shorten) the total deadline.
echo "--- not cancelled: timeout still fires at the configured budget ---"
START=$(date +%s%N)
${CLICKHOUSE_CLIENT} --query "
    SELECT dictGetString('dict_04649', 'value', toUInt64(3)) SETTINGS max_execution_time = 0
" 2>&1 | grep -o -m1 -e "source seems unavailable" -e "Timeout exceeded"
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
if [ "$ELAPSED_MS" -ge $(( WAIT_MS - 3000 )) ] && [ "$ELAPSED_MS" -lt $(( WAIT_MS * 2 )) ]; then
    echo "waited the configured budget"
else
    echo "unexpected elapsed: ${ELAPSED_MS} ms"
fi

rm -f "${CLICKHOUSE_TMP}/04649_kill_err.txt"

# Stop the listener before dropping: DROP DICTIONARY joins the update worker, which is still
# inside the read the cancelled query abandoned. With the listener gone that read fails at once.
kill $LISTENER_PID 2>/dev/null ||:
wait $LISTENER_PID 2>/dev/null ||:
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY dict_04649"
