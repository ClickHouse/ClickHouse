#!/usr/bin/env bash
# Test that DETACH DATABASE SYNC responds to query cancellation (KILL QUERY).
# Regression test for a bug where waitDetachedTableNotInUse would hang indefinitely
# when the DETACH DATABASE query was killed, because it never checked for cancellation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

DB_NAME="${CLICKHOUSE_DATABASE}_detach_cancel"
LONG_QUERY_ID="long_select_detach_${CLICKHOUSE_DATABASE}_$$"
DETACH_QUERY_ID="detach_db_cancel_${CLICKHOUSE_DATABASE}_$$"

function cleanup()
{
    # Kill background queries if still running
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${LONG_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${DETACH_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    # Database may or may not exist depending on where we failed
    $CLICKHOUSE_CLIENT --query "ATTACH DATABASE IF NOT EXISTS ${DB_NAME}" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_NAME} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT

# Create database and table
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_NAME} SYNC"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB_NAME} ENGINE = Atomic"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${DB_NAME}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${DB_NAME}.t SELECT number FROM numbers(100)"

# Start a long-running SELECT to keep the table storage "in use" (holds a StoragePtr reference).
# This prevents the detached table from becoming "not in use",
# causing waitDetachedTableNotInUse to wait.
$CLICKHOUSE_CLIENT \
    --query_id="${LONG_QUERY_ID}" \
    --function_sleep_max_microseconds_per_block=60000000 \
    --query "SELECT sleepEachRow(1) FROM ${DB_NAME}.t LIMIT 60 FORMAT Null" 2>/dev/null &

# Wait for the SELECT to appear in system.processes
for _ in $(seq 1 60); do
    result=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '${LONG_QUERY_ID}'")
    [ "$result" = "1" ] && break
    sleep 0.1
done
if [ "$result" != "1" ]; then
    echo "FAIL: long SELECT did not appear in system.processes" >&2
    exit 1
fi

# Start DETACH DATABASE SYNC in background.
# The SYNC keyword triggers waitDetachedTableNotInUse for each table UUID.
# Since the table is still "in use" by the long SELECT, it will block there.
$CLICKHOUSE_CLIENT \
    --query_id="${DETACH_QUERY_ID}" \
    --query "DETACH DATABASE ${DB_NAME} SYNC" >/dev/null 2>&1 &
DETACH_PID=$!

# Wait for the DETACH query to appear in system.processes
for _ in $(seq 1 60); do
    result=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '${DETACH_QUERY_ID}'")
    [ "$result" = "1" ] && break
    sleep 0.1
done
if [ "$result" != "1" ]; then
    echo "FAIL: DETACH query did not appear in system.processes" >&2
    exit 1
fi

# Give it a moment to enter waitDetachedTableNotInUse
sleep 2

# Kill the DETACH query
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${DETACH_QUERY_ID}' SYNC FORMAT Null"

# Wait for the DETACH process to finish (should return quickly after being killed).
# Use a bounded wait: on the old binary without the fix, KILL QUERY has no effect
# and the DETACH hangs forever — detect that and fail explicitly.
for _ in $(seq 1 100); do
    kill -0 $DETACH_PID 2>/dev/null || break
    sleep 0.1
done

if kill -0 $DETACH_PID 2>/dev/null; then
    echo "FAIL: DETACH query was not cancelled within 10 seconds" >&2
    kill $DETACH_PID 2>/dev/null ||:
    wait $DETACH_PID 2>/dev/null ||:
    exit 1
fi

wait $DETACH_PID 2>/dev/null &&:

echo "DETACH query was cancelled successfully"

# Kill the long SELECT to allow cleanup
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${LONG_QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
wait 2>/dev/null ||:
