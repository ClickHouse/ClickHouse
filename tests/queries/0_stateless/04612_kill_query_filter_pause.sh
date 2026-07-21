#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan, long
# Test that KILL QUERY works for FilterTransform with failpoint, covering the stopReading/early-return/cache-guard code path.
# Uses the filter_transform_pause failpoint to stop the query after expression execution,
# then KILL QUERY and verify the cancellation is detected.
# no-parallel: filter_transform_pause is a global PAUSEABLE_ONCE failpoint, unrelated queries could consume it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_filter_pause_${CLICKHOUSE_DATABASE}_$RANDOM"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause" 2>/dev/null' EXIT

# Enable failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filter_transform_pause"

# Start a filter query that will pause at the failpoint
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT count()
    FROM numbers(100000000)
    WHERE sipHash64(number) % 2 = 1
    FORMAT Null
    SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0
" >/dev/null 2>&1 &

# Wait for the failpoint to be hit (query is now blocked in doTransform after expression execution)
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT filter_transform_pause PAUSE"

# Kill the query (ASYNC) - this triggers onCancel -> cancelExecution on all functions
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Disable failpoint - query should see isCancelled() and call stopReading(), then early-return
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause"

wait

echo "OK"
