#!/usr/bin/env bash
# Tags: no-fasttest

# `--print-profile-events` with a delay defers the events of a query into an accumulator that is
# flushed when the query ends. When the `--server_logs_file` sink is unavailable at that point (a
# FIFO without a reader), the deferred events are intentionally dropped - fail-close, see
# 04672_client_deferred_profile_events_blocked_logs_file. What must never happen is the accumulator
# silently surviving into the next query of the same client session: the counters of the first
# query would then be printed under the second one. This test runs two queries in one session -
# the first with the sink unavailable, the second with a reader attached - and checks that the
# second query prints only its own events.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events_leak.fifo"
CLIENT_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events_leak.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events_leak.err"
CAPTURED="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events_leak.captured"

CLIENT=""
READER=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    [ -n "$READER" ] && kill -9 "$READER" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_OUT" "$CLIENT_ERR" "$CAPTURED"
}
trap cleanup EXIT

mkfifo "$FIFO"

# The delay is longer than either query, so all events are deferred to the per-query trailing
# flush. The first query is an INSERT - its `InsertQuery` counter cannot legitimately appear in
# the events of the second query, a SELECT. The FIFO has no reader while the first query runs and
# finishes, so its trailing flush finds the sink unavailable and must drop the deferred events.
# The second query sleeps long enough for the reader to be attached before it ends.
$CLICKHOUSE_CLIENT --server_logs_file="$FIFO" \
    --print-profile-events --profile-events-delay-ms=600000 \
    --query "INSERT INTO FUNCTION null('x UInt8') VALUES (1); SELECT sleep(0.2) FROM numbers(30) SETTINGS max_block_size = 1;" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT=$!

# Wait for the first row of the second query: the INSERT prints nothing, so any output means the
# first query - including its trailing flush against the reader-less FIFO - is fully finished.
for _ in {0..600}
do
    [ -s "$CLIENT_OUT" ] && break
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.1
done

if ! [ -s "$CLIENT_OUT" ]
then
    echo "FAIL: the second query produced no output"
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
    exit 0
fi

# Attach a reader while the second query is still sleeping: its trailing flush finds the sink
# available and prints the deferred events of the second query - and only of the second query.
cat "$FIFO" > "$CAPTURED" &
READER=$!

for _ in {0..600}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: the client is stuck"
    exit 0
fi

# The client has exited, so the FIFO's write side is closed and the reader sees EOF promptly.
for _ in {0..100}
do
    kill -0 "$READER" 2>/dev/null || break
    sleep 0.1
done

if grep -q 'SleepFunctionCalls' "$CAPTURED"
then
    echo "OK: the deferred events were flushed with their own query"
else
    echo "FAIL: the deferred events of the second query were lost"
    echo "--- captured ---"
    cat "$CAPTURED"
fi

if grep -q 'InsertQuery' "$CAPTURED"
then
    echo "FAIL: the events of the first query leaked into the second one"
    echo "--- captured ---"
    cat "$CAPTURED"
else
    echo "OK: no events leaked from the previous query"
fi
