#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the s3 table function needs AWS S3 support, which the fast-test build leaves out (-DENABLE_LIBRARIES=0).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A socket that is bound but never listened on holds the port for as long as the test runs: connect()
# still gets ECONNREFUSED, so every S3 request fails and the client keeps retrying (s3_retry_attempts
# defaults to 500) for minutes, while bind() from anything else gets EADDRINUSE. The assertions are
# which error is reported and which status is returned, never the elapsed time; `timeout` is a
# fail-fast belt so a regression cannot hang the suite.
PORT_FILE="${CLICKHOUSE_TMP}/05227_deadport_${CLICKHOUSE_DATABASE}.port"
rm -f "$PORT_FILE"

python3 -c "
import os, socket, time
s = socket.socket()
s.bind(('127.0.0.1', 0))
with open('$PORT_FILE.tmp', 'w') as f:
    f.write(str(s.getsockname()[1]))
os.rename('$PORT_FILE.tmp', '$PORT_FILE')
time.sleep(900)
" &
HOLDER_PID=$!

LISTENER_STDIN="${CLICKHOUSE_TMP}/05227_stdin_${CLICKHOUSE_DATABASE}"
LISTENER_PORT_FILE="${CLICKHOUSE_TMP}/05227_listener_${CLICKHOUSE_DATABASE}.port"
LISTENER_LOG="${CLICKHOUSE_TMP}/05227_listener_${CLICKHOUSE_DATABASE}.log"
rm -f "$LISTENER_STDIN" "$LISTENER_PORT_FILE" "$LISTENER_LOG"

cleanup() {
    kill "$HOLDER_PID" 2>/dev/null
    kill "${STDIN_PID:-}" 2>/dev/null
    kill "${CURL_PID:-}" 2>/dev/null
    if [ -n "${LOCAL_PID:-}" ]; then
        for _ in {1..100}; do
            kill -0 "$LOCAL_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$LOCAL_PID" 2>/dev/null
        wait "$LOCAL_PID" 2>/dev/null
    fi
    rm -f "$PORT_FILE" "$LISTENER_STDIN" "$LISTENER_PORT_FILE" "$LISTENER_LOG"
}
trap cleanup EXIT

for _ in {1..300}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done

PORT=$(cat "$PORT_FILE")

QUERY="SELECT * FROM s3('http://127.0.0.1:${PORT}/test/x.tsv', 'test', 'testtest', 'TSV', 'x UInt64')"

# The error is matched by presence rather than by counting lines: the client is run with
# --send_logs_level=warning, so it prints the server's log line as well as the exception.
echo '--- clickhouse-local honours max_execution_time while the S3 client retries'
OUT=$(timeout 60 $CLICKHOUSE_LOCAL -q "$QUERY SETTINGS max_execution_time = 2" 2>&1); RC=$?
echo "$OUT" | grep -oF 'TIMEOUT_EXCEEDED' | head -n 1
# 159 = TIMEOUT_EXCEEDED. The status also rejects 124, which is what `timeout` returns if the
# process hangs at exit instead of unwinding the cancellation checker.
[ "$RC" = 159 ] && echo 'exit TIMEOUT_EXCEEDED' || echo "unexpected exit $RC"

echo '--- the server answers the same query the same way'
OUT=$(timeout 60 $CLICKHOUSE_CLIENT -q "$QUERY SETTINGS max_execution_time = 2" 2>&1); RC=$?
echo "$OUT" | grep -oF 'TIMEOUT_EXCEEDED' | head -n 1
[ "$RC" = 159 ] && echo 'exit TIMEOUT_EXCEEDED' || echo "unexpected exit $RC"

echo '--- without a time limit the S3 failure is still reported as an S3 failure'
OUT=$(timeout 60 $CLICKHOUSE_LOCAL -q "$QUERY SETTINGS s3_retry_attempts = 1" 2>&1); RC=$?
echo "$OUT" | grep -cF 'TIMEOUT_EXCEEDED'
echo "$OUT" | grep -oF 'S3_ERROR' | head -n 1
# Not pinned to a number: 499 (S3_ERROR) truncates to 243 in an exit status.
[ "$RC" != 124 ] && echo 'exit not a timeout' || echo 'unexpected exit 124'

echo '--- with timeout_overflow_mode = break the expiry does not cancel the query'
# That mode checks the limit instead of cancelling, so no timeout is reported. What ends the query
# is not asserted: whether the retries run out first or a pipeline check stops the read and returns
# an empty result depends on scheduling. The attempts sleep 25 ms doubling in between, so eight of
# them are past a one-second limit on the backoff alone, whatever a refused connection costs.
OUT=$(timeout 60 $CLICKHOUSE_LOCAL -q "$QUERY SETTINGS max_execution_time = 1, \
    timeout_overflow_mode = 'break', s3_retry_attempts = 8" 2>&1); RC=$?
echo "$OUT" | grep -cF 'TIMEOUT_EXCEEDED'
[ "$RC" != 124 ] && echo 'exit not a timeout' || echo 'unexpected exit 124'

echo '--- a query on a local listener keeps its time limit while the process shuts down'
# Once `SYSTEM START LISTEN` turns this process into a server, teardown stops the listeners and then
# waits up to five seconds for their connections to drain, so a query still running in that window has
# to keep reporting its own limit. The verdict is read from the process's own log instead of from the
# client, because the client only learns the error once the S3 retry loop reaches its next attempt
# boundary - its only cancellation checkpoint - and that can land after those five seconds, in which
# case teardown closes the connection with no response at all.
# OS-assigned ports (`--tcp_port 0 --http_port 0`) keep the test parallel-safe.
mkfifo "$LISTENER_STDIN"
sleep 900 > "$LISTENER_STDIN" &
STDIN_PID=$!

$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port 0 --http_port 0 --interactive \
    --logger.level=debug --logger.log="$LISTENER_LOG" \
    --query "SYSTEM START LISTEN QUERIES ALL; SELECT getServerPort('http_port') FORMAT TSV" \
    < "$LISTENER_STDIN" > "$LISTENER_PORT_FILE" 2>/dev/null &
LOCAL_PID=$!

for _ in {1..600}; do
    [ -s "$LISTENER_PORT_FILE" ] && break
    sleep 0.1
done
read -r LISTENER_PORT < "$LISTENER_PORT_FILE"
[ -n "${LISTENER_PORT:-}" ] || { echo 'failed to start the local listener'; exit 1; }

LISTENER_QUERY_ID="05227_listener_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CURL} -sS --max-time 60 --data-binary "$QUERY SETTINGS max_execution_time = 2" \
    "http://127.0.0.1:${LISTENER_PORT}/?query_id=${LISTENER_QUERY_ID}" > /dev/null 2>&1 &
CURL_PID=$!

# Shutting the listener down before the query starts would tear it down with nothing in flight, which
# asserts nothing. This line is written when execution begins, which is also when the
# `max_execution_time` clock starts, so the deadline is still two seconds ahead of the kill below.
for _ in {1..600}; do
    grep -qF "{${LISTENER_QUERY_ID}} <Debug> executeQuery" "$LISTENER_LOG" 2>/dev/null && break
    sleep 0.1
done
grep -qF "{${LISTENER_QUERY_ID}} <Debug> executeQuery" "$LISTENER_LOG" 2>/dev/null \
    || { echo 'the listener query never started'; exit 1; }

# Closing stdin ends the local session, so the listeners shut down through the normal path.
kill "$STDIN_PID" 2>/dev/null

# Teardown stops the listeners before it waits for their connections, so a refused connection (curl
# status 7) is the process saying teardown is under way, which the kill above only asked for.
for _ in {1..600}; do
    PROBE=0
    ${CLICKHOUSE_CURL} -sS --max-time 2 "http://127.0.0.1:${LISTENER_PORT}/?query=SELECT+1" \
        > /dev/null 2>&1 || PROBE=$?
    [ "$PROBE" = 7 ] && break
    sleep 0.1
done

# Either the deadline is noticed, or the process gets all the way out without noticing it.
for _ in {1..600}; do
    grep -F 'Cancelling the task because of the timeout' "$LISTENER_LOG" 2>/dev/null \
        | grep -qF "query_id: ${LISTENER_QUERY_ID}" && break
    kill -0 "$LOCAL_PID" 2>/dev/null || break
    sleep 0.1
done

# The deadline has to be noticed after teardown has begun: that is the window in which the checker
# used to be gone, leaving the query with no writer of the cancellation flag at all.
# Both stamps come from this log so that they share one clock: the log's time zone is the one the
# process resolved for itself, which is not necessarily the shell's.
# Teardown begins by destroying the local connection, so that session's `Logout` is the first line
# teardown writes, and it is written by the thread that drives teardown.
MAIN_SESSION=$(grep -oE 'LOCAL-Session-[0-9a-f-]+' "$LISTENER_LOG" 2>/dev/null | head -n 1)
# An empty pattern matches every line, which would stamp process start: nothing can fail that.
TEARDOWN_AT=
[ -n "$MAIN_SESSION" ] && TEARDOWN_AT=$(grep -F "$MAIN_SESSION" "$LISTENER_LOG" 2>/dev/null \
    | grep -F 'Logout' | head -n 1 | cut -c 1-26)
CANCELLED_AT=$(grep -F 'Cancelling the task because of the timeout' "$LISTENER_LOG" 2>/dev/null \
    | grep -F "query_id: ${LISTENER_QUERY_ID}" | head -n 1 | cut -c 1-26)
if [ -n "$TEARDOWN_AT" ] && [ -n "$CANCELLED_AT" ] && [[ "$CANCELLED_AT" > "$TEARDOWN_AT" ]]; then
    echo 'cancelled by its own time limit during shutdown'
else
    echo "unexpected: teardown started by ${TEARDOWN_AT:-never}, cancelled ${CANCELLED_AT:-never}"
fi

# Nothing is left to read from the process, and its own exit waits for the S3 client to come out of
# the request it is in the middle of.
kill "$LOCAL_PID" 2>/dev/null
