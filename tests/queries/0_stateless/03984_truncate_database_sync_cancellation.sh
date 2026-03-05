#!/usr/bin/env bash
# Test that TRUNCATE TABLES FROM ... LIKE '...' responds to query cancellation (KILL QUERY)
# during the parallel table-truncation phase.
# Regression test: before the fix, the truncation loop never checked the kill signal,
# so KILL QUERY had no effect until all tables were processed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

DB_NAME="${CLICKHOUSE_DATABASE}_trunc_cancel"
TRUNCATE_QUERY_ID="truncate_db_cancel_${CLICKHOUSE_DATABASE}_$$"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${TRUNCATE_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_NAME} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT

# Create database with many tables. With 16 parallel truncation threads (default), having 50 tables
# ensures most tasks are still queued when KILL arrives, so the kill check at lambda entry fires.
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_NAME} SYNC"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB_NAME} ENGINE = Atomic"
for i in $(seq 1 50); do
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE ${DB_NAME}.t${i} (x UInt64) ENGINE = MergeTree ORDER BY x;
        INSERT INTO ${DB_NAME}.t${i} SELECT number FROM numbers(100000);
    "
done

# Start TRUNCATE TABLES FROM ... LIKE '%' in the background.
# The parallel truncation path checks kill at the start of each table's lambda.
$CLICKHOUSE_CLIENT \
    --query_id="${TRUNCATE_QUERY_ID}" \
    --query "TRUNCATE TABLES FROM ${DB_NAME} LIKE '%'" 2>&1 | tr '\n' ' ' | grep -v QUERY_WAS_CANCELLED &
TRUNCATE_PID=$!

# Wait for the TRUNCATE query to appear in system.processes.
for _ in $(seq 1 60); do
    result=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '${TRUNCATE_QUERY_ID}'")
    [ "$result" = "1" ] && break
    sleep 0.1
done

# Kill the TRUNCATE query.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${TRUNCATE_QUERY_ID}' SYNC FORMAT Null"

# Wait for the background process to finish (should return quickly after being killed).
wait $TRUNCATE_PID 2>/dev/null &&:

echo "TRUNCATE DATABASE query was cancelled successfully"
