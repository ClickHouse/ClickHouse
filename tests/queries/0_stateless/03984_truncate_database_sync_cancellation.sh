#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses a PAUSEABLE failpoint; concurrent test instances would share the
# same global failpoint channel and interfere with each other's ENABLE/DISABLE sequence.

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
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT truncate_database_tables_pause" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${TRUNCATE_QUERY_ID}' FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_NAME} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${DB_NAME} SYNC"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${DB_NAME} ENGINE = Atomic"
for i in $(seq 1 5); do
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE ${DB_NAME}.t${i} (x UInt64) ENGINE = MergeTree ORDER BY x;
        INSERT INTO ${DB_NAME}.t${i} SELECT number FROM numbers(100);
    "
done

# Enable the PAUSEABLE failpoint so each per-table lambda blocks before the kill check.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT truncate_database_tables_pause"

# Start TRUNCATE TABLES FROM ... LIKE '%' in the background.
$CLICKHOUSE_CLIENT \
    --query_id="${TRUNCATE_QUERY_ID}" \
    --query "TRUNCATE TABLES FROM ${DB_NAME} LIKE '%'" 2>/dev/null &
TRUNCATE_PID=$!

# Wait until at least one lambda has paused at the failpoint (deterministic).
# This returns as soon as pause_count > 0, with no polling or sleeping.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT truncate_database_tables_pause PAUSE"

# Set is_killed WITHOUT waiting for the query to exit (no SYNC keyword).
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${TRUNCATE_QUERY_ID}' FORMAT Null"

# Wake up all paused lambdas and disable the failpoint so queued lambdas skip it.
# Lambdas resume → call throwIfKilled() → throw QUERY_WAS_CANCELLED.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT truncate_database_tables_pause"

wait $TRUNCATE_PID 2>/dev/null ||:

# Verify via query_log that the query was cancelled (exception_code 394 = QUERY_WAS_CANCELLED).
# DDL queries like TRUNCATE execute synchronously inside the interpreter, before the pipeline
# starts, so they are logged as ExceptionBeforeStart rather than ExceptionWhileProcessing.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.query_log
    WHERE query_id = '${TRUNCATE_QUERY_ID}'
      AND exception_code = 394
      AND current_database = currentDatabase()
"
echo "TRUNCATE DATABASE LIKE path: kill was successful"
