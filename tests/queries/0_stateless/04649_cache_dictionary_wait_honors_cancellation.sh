#!/usr/bin/env bash
# Tags: no-fasttest, long
# Tag no-fasttest: needs a local HTTP listener
# Tag long: the uncancelled arm deliberately waits out the full 30s dictionary budget

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A query that misses a cache-layout dictionary blocks in
# CacheDictionaryUpdateQueue::waitForCurrentUpdateFinish for up to
# QUERY_WAIT_TIMEOUT_MILLISECONDS. That wait must observe cancellation of the query
# (KILL QUERY, and max_execution_time with the default timeout_overflow_mode = 'throw')
# instead of deferring it until the timeout expires. In 'break' mode the cancellation
# checker deliberately does not mark the query, so the wait still ends at its own timeout.
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
# Bound for the max_execution_time arm only (the KILL arm has its own, narrower one below). The
# server arms that timeout for the query itself, so this measures the whole client invocation: a
# cancelled query must return in well under WAIT_MS. Generous, so a loaded CI host does not make
# it timing-fragile, while still failing an unfixed build (which waits 30s).
LIMIT_MS=15000

# http_max_tries = 1 keeps the abandoned update in the queue's worker short: the update the
# cancelled query walks away from keeps running there, and DROP DICTIONARY below joins that
# worker (CacheDictionaryUpdateQueue::stopAndWait -> update_pool.wait).
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
${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID" --query "
    SELECT dictGetString('dict_04649', 'value', toUInt64(2)) SETTINGS max_execution_time = 0
" > /dev/null 2> "${CLICKHOUSE_TMP}/04649_kill_err.txt" &
CLIENT_PID=$!

# Wait until the query has been executing for a bounded interval, so it is actually parked in
# the dictionary wait and the cancellation lands there. Gating only on the query being visible
# in system.processes is not enough: ProcessList::insert publishes it before the pipeline
# executor is attached, so if KILL QUERY won that race the cancellation would be thrown from
# QueryStatus::addPipelineExecutor instead, and this arm would pass without the fix. The wait
# budget is 30s, so a 1s floor still leaves the bound below ample margin.
KILL_ELAPSED=
for _ in $(seq 1 600); do
    KILL_ELAPSED=$(${CLICKHOUSE_CLIENT} --query "
        SELECT max(elapsed) FROM system.processes WHERE query_id = '$QUERY_ID'")
    if [ -n "$KILL_ELAPSED" ] && awk "BEGIN{exit !($KILL_ELAPSED > 1)}"; then break; fi
    sleep 0.1
done

if [ -z "$KILL_ELAPSED" ] || ! awk "BEGIN{exit !($KILL_ELAPSED > 1)}" 2>/dev/null; then
    # Not in .reference on purpose: never reaching the wait means the arm measured nothing, so
    # the test must fail with a readable diff instead of falling through to the KILL.
    echo "query did not reach the dictionary wait"
    cat "${CLICKHOUSE_TMP}/04649_kill_err.txt"
    # No KILL was issued on this path, so terminate the background client here to keep the rest
    # of the test bounded.
    kill $CLIENT_PID 2>/dev/null ||:
    wait $CLIENT_PID 2>/dev/null ||:
else
    # The timer covers only the KILL and the cancelled client's exit, so the bound has to
    # separate "returns" from "waits out the remaining dictionary budget". KILL QUERY SYNC polls
    # every 100ms until the query leaves the process list: with the fix that costs one
    # clickhouse-client startup (~8s worst case on a loaded sanitizer host, as measured in
    # 04410_primes_source_cancellation.sh) plus the 100ms wait slice plus the 100ms SYNC
    # granularity; without it the query stays listed for the ~29s left of its 30s budget. 15s
    # separates the two with margin on both sides, so do not re-tighten it.
    KILL_LIMIT_MS=15000
    KILL_START=$(date +%s%N)
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" > /dev/null
    wait $CLIENT_PID 2>/dev/null ||:
    KILL_MS=$(( ($(date +%s%N) - KILL_START) / 1000000 ))
    grep -o -m1 -e QUERY_WAS_CANCELLED -e "source seems unavailable" "${CLICKHOUSE_TMP}/04649_kill_err.txt"
    if [ "$KILL_MS" -lt "$KILL_LIMIT_MS" ]; then echo "stopped early"; else echo "waited out the timeout: ${KILL_MS} ms"; fi
fi

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
