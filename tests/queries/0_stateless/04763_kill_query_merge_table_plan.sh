#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# Tag no-fasttest: the deterministic waits below take about 23 seconds of a test that runs in
# about 28, which is a large share of the fast test per-test timeout of 60 seconds.
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint, so a concurrent
# instance pausing or resuming the same channel would break the synchronisation.
# Tag no-random-settings: the two-second deadline must not elapse before the first child pauses
# at the failpoint, and randomized settings change how much analysis work precedes that pause.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FP="storage_merge_create_children_plans_pause"
CONTROL_QID="merge_plan_control_${CLICKHOUSE_DATABASE}_$$"
KILL_QID="merge_plan_kill_${CLICKHOUSE_DATABASE}_$$"
BREAK_QID="merge_plan_break_${CLICKHOUSE_DATABASE}_$$"

# How many children a query built a plan for. `createChildrenPlans` logs one line per child, so
# this counts exactly how far the loop got, which is what the deadline is supposed to bound.
function children_planned()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '$1' AND message LIKE 'Building plan for child table%'
    "
}

# How many parts a query selected. `ReadFromMergeTree::initializePipeline` increments this while
# building a child's pipeline, so a non-zero value means the retained plans were really built.
function parts_selected()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
        SELECT sum(ProfileEvents['SelectedParts']) FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND query_id = '$1' AND type != 'QueryStart' AND current_database = currentDatabase()
    "
}

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('${KILL_QID}', '${BREAK_QID}') FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

for i in $(seq 1 4); do
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS t${i};
        CREATE TABLE t${i} (x UInt64) ENGINE = MergeTree ORDER BY x;
        INSERT INTO t${i} VALUES (${i});
    "
done

# Positive control: with no deadline every matched child is planned and read.
$CLICKHOUSE_CLIENT --query_id="${CONTROL_QID}" \
    --query "SELECT sum(x) FROM merge(currentDatabase(), '^t[0-9]+$')"

# The guard must stop the per-child loop on KILL QUERY.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"

$CLICKHOUSE_CLIENT --query_id="${KILL_QID}" \
    --query "SELECT sum(x) FROM merge(currentDatabase(), '^t[0-9]+$')" > /dev/null 2>&1 &
KILL_PID=$!

# The first child is now paused inside the region under test. Bound the wait: if the query
# never pauses, this must fail with a diagnostic instead of consuming the per-test timeout.
if ! timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
    echo "FAIL: the query never paused at the failpoint in the kill arm"
fi

# Set is_killed while the thread sits inside the per-child loop.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${KILL_QID}' FORMAT Null"

# Resume WITHOUT disabling, so the failpoint channel survives for the second observation.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${FP}"

# A cancelled build must stop, so it must never reach a second child and pause again.
# Only status 124 means the wait timed out, which is the expected outcome. Status 0 means the
# loop kept iterating, and any other status means the wait itself failed.
kill_wait_rc=0
timeout 10 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || kill_wait_rc=$?
if [ "$kill_wait_rc" -eq 0 ]; then
    echo "FAIL: the loop paused again after the query was cancelled"
elif [ "$kill_wait_rc" -eq 124 ]; then
    echo "cancelled: the plan build stopped iterating over children"
else
    echo "FAIL: the wait failed with status ${kill_wait_rc}"
fi

# Release before waiting: on an unpatched build the loop is paused at another child right now,
# and DISABLE is the only statement that wakes it. NOTIFY would leave the channel enabled and
# the loop would pause again at the next child.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
timeout 30 tail --pid="${KILL_PID}" -f /dev/null
wait "${KILL_PID}" 2>/dev/null ||:

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.query_log
    WHERE query_id = '${KILL_QID}' AND current_database = currentDatabase() AND exception_code = 394
"

# timeout_overflow_mode = 'break' makes checkTimeLimit return false instead of throwing, and such
# a deadline never sets the killed flag, so ignoring that false would leave the loop
# uninterruptible. The documented semantics of that mode are to return the partial result, so the
# build must stop and the query must still succeed over the children planned before the deadline.
# The pause makes the elapsed time exceed max_execution_time without relying on timing.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"

# max_execution_time is measured from before parsing, so leave enough margin that the budget
# cannot elapse during startup and make the guard fire before any child pauses. The discriminator
# is the sleep below, not this limit, so it must stay well under that sleep.
$CLICKHOUSE_CLIENT --query_id="${BREAK_QID}" --query "
    SELECT sum(x) FROM merge(currentDatabase(), '^t[0-9]+$')
    SETTINGS max_execution_time = 2, timeout_overflow_mode = 'break'
" > /dev/null 2> "${CLICKHOUSE_TMP}/04763_break.err" &
BREAK_PID=$!

if ! timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
    echo "FAIL: the query never paused at the failpoint in the break arm"
fi
sleep 3
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${FP}"

break_wait_rc=0
timeout 10 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || break_wait_rc=$?
if [ "$break_wait_rc" -eq 0 ]; then
    echo "FAIL: the loop paused again in 'break' overflow mode"
elif [ "$break_wait_rc" -eq 124 ]; then
    echo "break mode: the plan build stopped iterating over children"
else
    echo "FAIL: the wait failed with status ${break_wait_rc}"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
timeout 30 tail --pid="${BREAK_PID}" -f /dev/null
break_client_rc=0
wait "${BREAK_PID}" || break_client_rc=$?

# 'break' returns a partial result, so the client must succeed with no error on stderr.
echo "break mode client status: ${break_client_rc}"
echo "break mode stderr bytes: $(wc -c < "${CLICKHOUSE_TMP}/04763_break.err")"
rm -f "${CLICKHOUSE_TMP}/04763_break.err"

# How far the loop got. The control above plans all four children; honoring the deadline must plan
# strictly fewer. Note the row count cannot be used as the oracle here: the same elapsed budget
# that stops the plan build has also stopped the pipeline, so both outcomes return zero rows.
control_planned=$(children_planned "${CONTROL_QID}")
break_planned=$(children_planned "${BREAK_QID}")
if [ "$control_planned" -lt 4 ]; then
    echo "FAIL: the control planned ${control_planned} children instead of 4"
elif [ "$break_planned" -lt "$control_planned" ]; then
    echo "break mode planned fewer children than the control"
else
    echo "FAIL: 'break' overflow mode planned all ${break_planned} children"
fi

# Stopping early must keep the children already planned, not discard them: over-truncating
# `selected_tables` would leave the pipeline empty, which all the assertions above would still
# accept. A lower bound, because the number of parts is not the property under test.
break_parts=$(parts_selected "${BREAK_QID}")
if [ "$break_parts" -ge 1 ]; then
    echo "break mode built the pipeline of the children it planned"
else
    echo "FAIL: 'break' overflow mode selected ${break_parts} parts, so it dropped the planned children"
fi
