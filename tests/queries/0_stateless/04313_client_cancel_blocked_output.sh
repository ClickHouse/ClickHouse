#!/usr/bin/env bash
# Tags: no-fasttest

# Pressing Ctrl+C in the client must terminate the output of a result set promptly,
# even when the client is blocked writing to a slow/stuck output sink.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_blocked_output"
FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.fifo"
rm -f "$FIFO"
mkfifo "$FIFO"

# A reader that opens the pipe but never consumes data: the client quickly fills the pipe
# buffer and then blocks inside write(). Without an interruptible write, a single Ctrl+C
# would only set the cancellation flag while the restarted write() keeps the client stuck.
sleep 1000 < "$FIFO" &
HOLDER=$!

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" \
    --query "SELECT number, repeat('x', 1000) FROM numbers(1000000000)" > "$FIFO" 2>/dev/null &
CLIENT=$!

# Wait until the query is actually running (and thus blocked writing the result).
for _ in {0..60}
do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID'" | grep -F '1' > /dev/null && break
    sleep 0.5
done

# A single Ctrl+C must be enough to terminate the client.
kill -SIGINT "$CLIENT"

# Give the client a few seconds to terminate gracefully.
for _ in {0..50}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still running after Ctrl+C"
    kill -9 "$CLIENT" 2>/dev/null
else
    echo "OK: client terminated after Ctrl+C"
fi

wait "$CLIENT" 2>/dev/null
kill "$HOLDER" 2>/dev/null
wait "$HOLDER" 2>/dev/null
rm -f "$FIFO"
