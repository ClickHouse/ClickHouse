#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings
# no-parallel: the test waits on a process-global PAUSEABLE failpoint channel, so a
#   concurrent instance pausing or resuming it would break the ENABLE/WAIT/NOTIFY/DISABLE
#   sequence.
# no-fasttest: the test reserves seconds for the bounded waits; the fast test job has a
#   short per-test budget.
# no-random-settings: the pause fires at the 4096th row of the subquery block, so the
#   block must be larger than that and must not be cut short by a randomized limit or
#   timeout.

# Regression test for cancellation of an `IN` subquery while its `Set` is being built.
#
# Every `WHERE x IN (SELECT ...)` / `GLOBAL IN (SELECT ...)` materializes the subquery
# into an in-memory hash table before evaluating the left-hand side. The build loop in
# `Set::insertFromBlockImplCase` polls `CurrentThread::checkIfNotCancelled` once per 4096
# rows, and at the same boundary it pauses on the `creating_sets_transform_pause`
# failpoint. The pause anchors the test deterministically inside the hashing loop: a
# commit that reverts the polling also drops the `pauseFailPoint` call, so the wait
# below would time out and the test would fail.
#
# The failpoint is `PAUSEABLE`: it parks every hit until it is disabled, and
# `SYSTEM NOTIFY FAILPOINT` releases the parked threads without disarming it. That is
# what tells cancellation inside the loop from cancellation after it. The final exit
# code and `QUERY_WAS_CANCELLED` checks alone cannot: `PipelineExecutor::finalizeExecution`
# calls `checkTimeLimit`, which throws `QUERY_WAS_CANCELLED` for a killed query even if
# the whole block was hashed and the kill was noticed only after it.
#
# The plain `IN` arm therefore releases the thread parked at row 4096 with `NOTIFY` and
# then asserts that the fail point never pauses again: a loop that keeps hashing would
# park at row 8192, while a loop that polls the cancellation right after the pause
# throws before reaching it. A positive control below proves that this second pause is
# really reachable when the query is not killed, so the negative assertion is not
# vacuous.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=creating_sets_transform_pause
QID_PREFIX="05325_${CLICKHOUSE_DATABASE}"

# `max_block_size = 100000` makes the `numbers(100000)` subquery arrive as a single,
# larger-than-4096 block, so the failpoint really fires inside the hashing loop.
# `use_index_for_in_with_subqueries = 0` forces the runtime `CreatingSetsTransform`
# instead of an in-place index build, `enable_add_distinct_to_in_subqueries = 0` keeps
# the produced block from being re-chunked, and `max_threads = 1` keeps a single stream.
SETTINGS="max_block_size = 100000, max_threads = 1, max_rows_in_set = 0, use_index_for_in_with_subqueries = 0, enable_add_distinct_to_in_subqueries = 0"

function cleanup()
{
    # A leaked enabled failpoint would park unrelated later work, and DISABLE is what
    # releases a thread already parked there, so it must run on every exit path.
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "
        KILL QUERY WHERE (query_id LIKE '${QID_PREFIX}%' OR initial_query_id LIKE '${QID_PREFIX}%')
        AND query NOT LIKE '%KILL QUERY%' FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

function wait_for_pause()
{
    local message="$1"
    if ! timeout 30 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
        echo "FAIL: $message"
        exit 1
    fi
}

# The set for a plain IN subquery must be built by `CreatingSetsTransform`, otherwise
# the failpoint and the kill below would be about a shape that never runs the loop.
explain_has_creating_sets=$($CLICKHOUSE_CLIENT -q "
    SELECT count() > 0
    FROM (
        EXPLAIN PIPELINE
        SELECT count() FROM numbers(100) WHERE number IN (SELECT number FROM numbers(100000))
        SETTINGS $SETTINGS
    )
    WHERE explain ILIKE '%CreatingSetsTransform%'
")
if [ "$explain_has_creating_sets" != "1" ]; then
    echo "FAIL: the plain IN query does not build its set through CreatingSetsTransform"
fi

# Positive control: without the failpoint the same query builds the set and completes.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM numbers(100) WHERE number IN (SELECT number FROM numbers(100000))
    SETTINGS $SETTINGS
" > /dev/null

# Positive control for the "no second pause" check below: the same query, left running,
# must park again at the next 4096-row boundary after the first pause is released with
# `NOTIFY`. Without it the negative `WAIT` would also pass for a fail point that can only
# ever pause once, and the test would prove nothing about the loop.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

CONTROL_OUT="${CLICKHOUSE_TMP}/05325_control_${CLICKHOUSE_DATABASE}.out"
rm -f "$CONTROL_OUT"

$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM numbers(100) WHERE number IN (SELECT number FROM numbers(100000))
    SETTINGS $SETTINGS
" > "$CONTROL_OUT" &
control_pid=$!

wait_for_pause "the un-killed control query did not park at the first 4096-row boundary"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $FP"
wait_for_pause "the un-killed control query did not park at the second 4096-row boundary after NOTIFY"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

control_status=0
wait "$control_pid" || control_status=$?
if [ "$control_status" != "0" ] || [ "$(cat "$CONTROL_OUT")" != "100" ]; then
    echo "FAIL: the un-killed control query did not complete after two pauses (status $control_status, result $(cat "$CONTROL_OUT"))"
fi
rm -f "$CONTROL_OUT"

# Plain IN: pause inside the hashing loop, kill, and let the loop resume into the
# cancellation check.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

KILL_QID="${QID_PREFIX}_plain"
CLIENT_ERR="${CLICKHOUSE_TMP}/05325_plain_${CLICKHOUSE_DATABASE}.err"
rm -f "$CLIENT_ERR"

$CLICKHOUSE_CLIENT --query_id="$KILL_QID" -q "
    SELECT count() FROM numbers(100) WHERE number IN (SELECT number FROM numbers(100000))
    SETTINGS $SETTINGS
" > /dev/null 2> "$CLIENT_ERR" &
client_pid=$!

wait_for_pause "the killed query never reached the failpoint inside the set build"

# The paused thread must belong to this test's query, or the kill below would target
# somebody else.
parked=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$KILL_QID'")
if [ "$parked" != "1" ]; then
    echo "FAIL: the paused set build does not belong to the test query"
fi

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$KILL_QID' ASYNC FORMAT Null"

# Wait until the kill is visible before releasing the paused thread, so the release
# cannot race ahead of the kill flag being set.
for _ in {1..300}; do
    cancelled=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$KILL_QID' AND is_cancelled")
    [ "$cancelled" = "1" ] && break
    sleep 0.1
done

# Release the thread parked at row 4096 without disarming the fail point. The
# cancellation poll is the statement right after the pause, so the loop must throw
# there and never reach row 8192. If it instead keeps hashing to the end of the block,
# it parks again at row 8192 and the bounded `WAIT` below returns successfully.
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $FP"

failed=0
if timeout 5 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    echo "FAIL: the killed set build paused again, so it kept hashing instead of stopping at the cancellation poll"
    failed=1
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

client_status=0
wait "$client_pid" || client_status=$?

if [ "$client_status" = "0" ]; then
    echo "FAIL: the killed query exited successfully"
    failed=1
fi
if ! grep -q -F "QUERY_WAS_CANCELLED" "$CLIENT_ERR"; then
    echo "FAIL: the killed query did not report QUERY_WAS_CANCELLED: $(cat "$CLIENT_ERR")"
    failed=1
fi
if [ "$failed" = "0" ]; then
    echo "cancelled-mid-set-build"
fi
rm -f "$CLIENT_ERR"

# GLOBAL IN: the subquery is materialized on the initiator and broadcast to the shards
# as an external table. Both shards of `test_cluster_two_shards` are loopback to the same
# test server, so this is a normal stateless test.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_gis_dist SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_gis_local SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_gis_local (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_gis_local SELECT number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_gis_dist (x UInt64) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t_gis_local, rand())"

# Sanity: the same query completes with the failpoint disabled.
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_gis_dist WHERE x GLOBAL IN (SELECT number FROM numbers(100000))
    SETTINGS $SETTINGS
" > /dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

GLOBAL_QID="${QID_PREFIX}_global"
GLOBAL_ERR="${CLICKHOUSE_TMP}/05325_global_${CLICKHOUSE_DATABASE}.err"
rm -f "$GLOBAL_ERR"

$CLICKHOUSE_CLIENT --query_id="$GLOBAL_QID" -q "
    SELECT count() FROM t_gis_dist WHERE x GLOBAL IN (SELECT number FROM numbers(100000))
    SETTINGS $SETTINGS
" > /dev/null 2> "$GLOBAL_ERR" &
global_pid=$!

wait_for_pause "the GLOBAL IN query never reached the failpoint inside the set build"

# The set build of a GLOBAL IN runs either on the initiator or, in the per-shard rewrite
# path, on the shard queries. A shard query carries the initiator's id in
# `initial_query_id`, so both cases are covered here.
parked=$($CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.processes
    WHERE query_id = '$GLOBAL_QID' OR initial_query_id = '$GLOBAL_QID'
")
if [ "$parked" = "0" ]; then
    echo "FAIL: the paused GLOBAL IN set build does not belong to the test query"
fi

$CLICKHOUSE_CLIENT -q "
    KILL QUERY WHERE (query_id = '$GLOBAL_QID' OR initial_query_id = '$GLOBAL_QID')
    AND query NOT LIKE '%KILL QUERY%' FORMAT Null
"

# Wait until every query of the tree has the killed flag set, so DISABLE cannot release
# a set build the kill has not reached yet.
for _ in {1..300}; do
    pending=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.processes
        WHERE (query_id = '$GLOBAL_QID' OR initial_query_id = '$GLOBAL_QID') AND NOT is_cancelled
    ")
    [ "$pending" = "0" ] && break
    sleep 0.1
done

# Unlike the plain IN arm above, this one is released with DISABLE rather than NOTIFY plus
# a "no second pause" check: the set may be built both on the initiator and on the shard
# queries, so several threads can park on the same fail point. `NOTIFY` bumps
# `resume_epoch` by exactly one, while `pause_epoch` may be higher, and `waitForPause`
# returns as soon as `pause_epoch > resume_epoch` - it would report a second pause that
# never happened. The in-loop cancellation is asserted on the single-threaded plain IN arm.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

global_status=0
wait "$global_pid" || global_status=$?

failed=0
if [ "$global_status" = "0" ]; then
    echo "FAIL: the killed GLOBAL IN query exited successfully"
    failed=1
fi
if ! grep -q -F "QUERY_WAS_CANCELLED" "$GLOBAL_ERR"; then
    echo "FAIL: the killed GLOBAL IN query did not report QUERY_WAS_CANCELLED: $(cat "$GLOBAL_ERR")"
    failed=1
fi
if [ "$failed" = "0" ]; then
    echo "cancelled-global-in-mid-set-build"
fi
rm -f "$GLOBAL_ERR"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_gis_dist SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_gis_local SYNC"