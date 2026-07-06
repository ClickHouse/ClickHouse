#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT \
    --query_id="$query_id" \
    --max_block_size=50000000 \
    --query "SELECT DISTINCT number % 10000000 FROM numbers(50000000) FORMAT Null SETTINGS max_rows_to_read=0, log_queries=1" \
    > /dev/null 2>&1 &
client_pid=$!

wait_for_query_to_start "$query_id"

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id='$query_id'" > /dev/null

# Bounded wait: with cancellation the query should exit promptly
# (well below the 10+ seconds a full 50M-row hash build would take)
timeout=30
start=$EPOCHSECONDS
while kill -0 "$client_pid" 2>/dev/null; do
    if ((EPOCHSECONDS - start > timeout)); then
        echo "FAIL: Query was not cancelled promptly"
        exit 1
    fi
    sleep 0.1
done
wait "$client_pid" 2>/dev/null || true

# Verify cancellation via query_log
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id='$query_id' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -qF "QUERY_WAS_CANCELLED" || exit 1

echo "OK"
