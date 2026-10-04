#!/usr/bin/env bash
# Tags: no-fasttest

# `--print-profile-events` with a delay keeps accumulating the events into a single block that is
# printed only after the query has finished. By then the interrupt handler is stopped, so nothing
# could ever abandon a wait for the log sink to become writable: acquiring an explicit
# `--server_logs_file` from that epilogue must not wait at all. Otherwise a FIFO that never gets a
# reader hangs the client forever - after the result has already been delivered - and not even a
# Ctrl+C can get it out. Diagnostics are dropped instead.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events.fifo"
CLIENT_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_deferred_profile_events.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

# No reader is ever attached to this FIFO, so its `open()` for writing never completes.
mkfifo "$FIFO"

# The delay is longer than the query, so no profile-events block is printed while the query runs
# and everything is deferred to the post-query flush. No `--send_logs_level`, so the log sink is
# not touched before that point either.
$CLICKHOUSE_CLIENT --server_logs_file="$FIFO" \
    --print-profile-events --profile-events-delay-ms=600000 \
    --query "SELECT 1" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT=$!

# The client must finish on its own: an unusable diagnostics sink is not a reason to hang.
for _ in {0..150}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is stuck opening the log file after the query has finished"
    kill -9 "$CLIENT" 2>/dev/null
    exit 0
fi

echo "OK: client terminated"

if [ "$(cat "$CLIENT_OUT")" = "1" ]
then
    echo "OK: the result was returned"
else
    echo "FAIL: unexpected result"
    echo "--- client stdout ---"
    cat "$CLIENT_OUT"
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
fi
