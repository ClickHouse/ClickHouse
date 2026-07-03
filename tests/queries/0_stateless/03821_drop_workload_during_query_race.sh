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

# Non-empty if any client call is killed by its per-call timeout below,
# i.e. the drop/create race actually wedged a query or DDL. Checked after `wait`.
WEDGE_FLAG="${CLICKHOUSE_TMP}/03821_wedged_${CLICKHOUSE_TEST_UNIQUE_NAME}.flag"

# Per-call hard bound: send TERM at CALL_TIMEOUT, then KILL if still alive after KILL_GRACE.
# Plain `timeout` is only a soft TERM and waits for the child; a client stuck in the exact
# bad state this test defends against could ignore TERM and block until the outer test cap,
# which skips the EXIT trap and leaks the global root workload / thread resource.
CALL_TIMEOUT=30
KILL_GRACE=5

# Run a client call under the hard bound. Returns the call's exit code; 124 (killed by TERM)
# or 137 (had to KILL after the grace) both mean the call was wedged.
function bounded()
{
    timeout --signal=TERM --kill-after="$KILL_GRACE" "$CALL_TIMEOUT" "$@"
}

# A timeout-killed client can leave its query running server-side (shell_config.sh
# wait_for_queries_to_finish). The workload/resource are global server singletons but
# clickhouse-test runs each shell test in a fresh random database, so a query leaked by
# an earlier timed-out invocation lives in a different database while still holding them.
# Match by the fixed object name (every query holding them names them in its text)
# instead of current_database, so cross-invocation lingering work is killed too. The
# KILL's own text contains those names, hence the NOT ILIKE '%KILL QUERY%' self-exclusion.
function kill_lingering_queries()
{
    bounded $CLICKHOUSE_CLIENT -q "
        KILL QUERY WHERE (query ILIKE '%$WORKLOAD_NAME%' OR query ILIKE '%$RESOURCE_NAME%')
            AND query NOT ILIKE '%KILL QUERY%'
        SYNC
    " 2>/dev/null ||:
}

function cleanup()
{
    # Kill lingering server-side work first, then drop under a hard bound so cleanup
    # itself cannot hang past the outer test cap and reintroduce the leak.
    kill_lingering_queries
    bounded $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $WORKLOAD_NAME" 2>/dev/null ||:
    bounded $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS $RESOURCE_NAME" 2>/dev/null ||:
    rm -f "$WEDGE_FLAG"
}

# Clean up any previous state
cleanup

# Also clean up on TERM: clickhouse-test sends SIGTERM before SIGKILL on timeout.
trap cleanup EXIT TERM

# 124 (killed by TERM) or 137 (killed by KILL after grace): the call was wedged.
function is_wedged()
{
    [ "$1" = 124 ] || [ "$1" = 137 ]
}

function thread_query()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Run a short query that uses CPU scheduling
        # Use smaller numbers so queries finish quickly
        # Ignore expected errors when workload is dropped during query
        bounded $CLICKHOUSE_CLIENT --format Null -q "SELECT sum(number) FROM numbers(100000) SETTINGS workload = '$WORKLOAD_NAME'" 2>&1 \
            | { grep -v -e "RESOURCE_ACCESS_DENIED" -e "INVALID_SCHEDULER_NODE" -e "There is no resource" -e "^$" || true; }
        # PIPESTATUS[0] is the bounded call's exit; a hard-bound kill means a wedged query.
        is_wedged "${PIPESTATUS[0]}" && echo wedged > "$WEDGE_FLAG"
    done
}

function thread_drop_create()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        # Drop and recreate the workload while queries may be running
        # This creates the race condition when queries are releasing their leases
        # Tolerate expected race errors, but record a hard-bound kill (wedged DDL).
        local rc=0
        bounded $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $WORKLOAD_NAME" 2>/dev/null || rc=$?
        is_wedged "$rc" && echo wedged > "$WEDGE_FLAG"
        rc=0
        bounded $CLICKHOUSE_CLIENT -q "CREATE WORKLOAD IF NOT EXISTS $WORKLOAD_NAME" 2>/dev/null || rc=$?
        is_wedged "$rc" && echo wedged > "$WEDGE_FLAG"
    done
}

# Create resource and workload (no concurrent thread limit to avoid throttling).
# Bound the setup DDL too, and fail fast if it is wedged: an unbounded call here could
# block on the same scheduler state until the outer test cap SIGKILLs the shell (skipping
# the EXIT trap) after a global singleton was already created, reintroducing the leak.
setup_rc=0
bounded $CLICKHOUSE_CLIENT -nm -q "
    CREATE OR REPLACE RESOURCE $RESOURCE_NAME (WORKER THREAD, MASTER THREAD);
    CREATE OR REPLACE WORKLOAD $WORKLOAD_NAME;
" || setup_rc=$?
if is_wedged "$setup_rc"; then
    echo "FAIL: setup DDL was wedged for at least ${CALL_TIMEOUT}s (scheduler race), killed by timeout" >&2
    exit 1
fi

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
    echo "FAIL: a query or DDL was wedged for at least ${CALL_TIMEOUT}s (scheduler race), killed by timeout" >&2
    exit 1
fi

# Server should still be alive
bounded $CLICKHOUSE_CLIENT -q "SELECT 1"
