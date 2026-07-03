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

# Non-empty if any client call is killed by its per-call timeout below (exit 124),
# i.e. the drop/create race actually wedged a query or DDL. Checked after `wait`.
WEDGE_FLAG="${CLICKHOUSE_TMP}/03821_wedged_${CLICKHOUSE_TEST_UNIQUE_NAME}.flag"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $WORKLOAD_NAME" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS $RESOURCE_NAME" 2>/dev/null ||:
    rm -f "$WEDGE_FLAG"
}

# Clean up any previous state
cleanup

# Also clean up on TERM: clickhouse-test sends SIGTERM before SIGKILL on timeout.
trap cleanup EXIT TERM

# Bound every client call so a wedged query/DDL cannot block `wait` until the test cap,
# where SIGKILL skips the EXIT trap and leaks the global root workload and thread resource.
CALL_TIMEOUT=30

function thread_query()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Run a short query that uses CPU scheduling
        # Use smaller numbers so queries finish quickly
        # Ignore expected errors when workload is dropped during query
        timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT --format Null -q "SELECT sum(number) FROM numbers(100000) SETTINGS workload = '$WORKLOAD_NAME'" 2>&1 \
            | { grep -v -e "RESOURCE_ACCESS_DENIED" -e "INVALID_SCHEDULER_NODE" -e "There is no resource" -e "^$" || true; }
        # PIPESTATUS[0] is timeout(1)'s exit: 124 means it killed a wedged query.
        [ "${PIPESTATUS[0]}" = 124 ] && echo wedged > "$WEDGE_FLAG"
    done
}

function thread_drop_create()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Drop and recreate the workload while queries may be running
        # This creates the race condition when queries are releasing their leases
        # Tolerate expected race errors, but record a per-call timeout (124 = wedged DDL).
        local rc=0
        timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $WORKLOAD_NAME" 2>/dev/null || rc=$?
        [ "$rc" = 124 ] && echo wedged > "$WEDGE_FLAG"
        rc=0
        timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "CREATE WORKLOAD IF NOT EXISTS $WORKLOAD_NAME" 2>/dev/null || rc=$?
        [ "$rc" = 124 ] && echo wedged > "$WEDGE_FLAG"
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

# If any client call hit its per-call timeout, the race actually wedged a query or DDL.
# Fail loudly instead of masking the very hang this test is meant to surface.
if [ -e "$WEDGE_FLAG" ]; then
    echo "FAIL: a query or DDL was wedged for ${CALL_TIMEOUT}s (scheduler race), killed by timeout" >&2
    exit 1
fi

# Server should still be alive
timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "SELECT 1"
