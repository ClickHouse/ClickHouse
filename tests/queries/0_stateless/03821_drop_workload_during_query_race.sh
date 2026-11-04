#!/usr/bin/env bash
# Tags: race, no-parallel

# Test for race condition when a workload is dropped while queries are still running.
# This could cause an exception in the Lease destructor when trying to release CPU resources,
# because the scheduler queue has been marked as not usable.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use fixed names with unique prefix to avoid conflicts
WORKLOAD_NAME="wl_03821"
RESOURCE_NAME="rs_03821"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $WORKLOAD_NAME" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS $RESOURCE_NAME" 2>/dev/null ||:
}

# Clean up any previous state
cleanup

trap cleanup EXIT

function thread_query()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Run a short query that uses CPU scheduling
        # Use smaller numbers so queries finish quickly
        # Ignore expected errors when workload is dropped during query
        $CLICKHOUSE_CLIENT --format Null -q "SELECT sum(number) FROM numbers(100000) SETTINGS workload = '$WORKLOAD_NAME'" 2>&1 \
            | { grep -v -e "RESOURCE_ACCESS_DENIED" -e "INVALID_SCHEDULER_NODE" -e "There is no resource" -e "^$" || true; }
    done
}

function thread_drop_create()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Drop and recreate the workload while queries may be running
        # This creates the race condition when queries are releasing their leases
        $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $WORKLOAD_NAME" 2>/dev/null ||:
        $CLICKHOUSE_CLIENT -q "CREATE WORKLOAD IF NOT EXISTS $WORKLOAD_NAME" 2>/dev/null ||:
    done
}

# Create resource and workload (no concurrent thread limit to avoid throttling)
$CLICKHOUSE_CLIENT -nm -q "
    CREATE OR REPLACE RESOURCE $RESOURCE_NAME (WORKER THREAD, MASTER THREAD);
    CREATE OR REPLACE WORKLOAD $WORKLOAD_NAME;
"

TIMEOUT=1

# Start query threads
thread_query &
thread_query &
thread_query &
thread_query &

# Start drop/create thread
thread_drop_create &

wait

# Server should still be alive
$CLICKHOUSE_CLIENT -q "SELECT 1"
