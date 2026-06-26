#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# EXPLAIN ANALYZE runs the analyzed query for real, so an infinite stream must stay
# cancellable - the cancellation must reach the inner pipeline through the process list element.

query_id="04340_explain_analyze_cancel_${CLICKHOUSE_DATABASE}"

# Start EXPLAIN ANALYZE over an infinite stream in the background.
${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "EXPLAIN ANALYZE SELECT * FROM system.numbers FORMAT Null" >/dev/null 2>&1 &
client_pid=$!

# Wait (up to ~5s) until the query is actually running, then cancel it.
for _ in {1..50}; do
    found=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
    [[ "$found" == "1" ]] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"

# The client must terminate now that the inner pipeline was cancelled.
wait "$client_pid" 2>/dev/null

# The server is still responsive after cancelling the analyzed query.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
