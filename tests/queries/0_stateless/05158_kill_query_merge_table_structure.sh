#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: the two deterministic 3-second waits below, plus the 4-second sleep, are most of
# a test that runs in about 13 seconds, which is a large share of the fast test per-test timeout.
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint, so a concurrent
# instance pausing or resuming the same channel would break the synchronisation.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FP="storage_merge_schema_inference_pause"
FP_TABLE="storage_merge_schema_inference_table_pause"
KILL_QID="merge_structure_kill_${CLICKHOUSE_DATABASE}_$$"
BREAK_QID="merge_structure_break_${CLICKHOUSE_DATABASE}_$$"
TABLE_QID="merge_structure_table_${CLICKHOUSE_DATABASE}_$$"

# The two channels are separate so that each arm can only be stopped by the checkpoint it is about:
# every ${FP} poll happens inside ONE column's substream walk, which a per-table checkpoint could not
# stop, and the ${FP_TABLE} arm's tables are far too cheap to reach a ${FP} poll. Depth 8 polls 181 times.
DEEP="Int32"
for _ in $(seq 1 8); do DEEP="Array(Map(String, Tuple(a ${DEEP}, b ${DEEP})))"; done
# A type whose substream tree is one long path yields a single stream callback, so it is the shape
# that catches accounting charged per callback rather than per unit of work. Depth 100 polls 17
# times and its 100 subcolumns cost one prefix walk each.
UNARY="Int32"
for _ in $(seq 1 100); do UNARY="Tuple(a ${UNARY})"; done
# Depth 6 still polls 33 times, and its part holds 317 streams, so the part-loading arm can reach
# the checkpoint while staying cheap enough to write a real part.
SHALLOWER="Int32"
for _ in $(seq 1 6); do SHALLOWER="Array(Map(String, Tuple(a ${SHALLOWER}, b ${SHALLOWER})))"; done

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        SYSTEM DISABLE FAILPOINT ${FP};
        SYSTEM DISABLE FAILPOINT ${FP_TABLE};
        KILL QUERY WHERE query_id IN ('${KILL_QID}', '${BREAK_QID}', '${TABLE_QID}') FORMAT Null;
    " 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

# ---- KILL QUERY has to land inside one column's walk ----
# Structure inference reads metadata only, so these tables deliberately hold no data: one row of the
# branching type writes 1277 streams in a wide part and would cover nothing extra. The count() is a
# positive control: with no deadline the inference completes and the read runs, returning no rows, and
# it has to run before the failpoint is enabled or it would pause too.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS deep0;
    DROP TABLE IF EXISTS unary0;
    CREATE TABLE deep0 (k UInt32, c ${DEEP}) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE unary0 (k UInt32, c ${UNARY}) ENGINE = MergeTree ORDER BY k;
    SELECT count() FROM merge(currentDatabase(), '^deep0$');
    SYSTEM ENABLE FAILPOINT ${FP};
"

$CLICKHOUSE_CLIENT --query_id="${KILL_QID}" \
    --query "DESCRIBE TABLE merge(currentDatabase(), '^unary0$') FORMAT Null" > /dev/null 2>&1 &
KILL_PID=$!

# The wait returns once the enumeration is paused at a poll inside the only matched table's column,
# the KILL sets is_killed while the thread sits inside that walk, and the NOTIFY resumes it WITHOUT
# disabling, so the failpoint channel survives for the second observation. Bound the wait: if it
# never polls, fail with a diagnostic instead of consuming the per-test timeout.
if ! timeout 30 $CLICKHOUSE_CLIENT --query "
        SYSTEM WAIT FAILPOINT ${FP} PAUSE;
        KILL QUERY WHERE query_id = '${KILL_QID}' FORMAT Null;
        SYSTEM NOTIFY FAILPOINT ${FP};
    " > /dev/null 2>&1; then
    echo "FAIL: structure inference never polled inside the enumeration in the kill arm"
fi

# A cancelled walk must stop at its next poll, and 16 further polls remain, so it must never pause
# again. A walk that ignores the cancellation pauses again within tens of milliseconds of the resume,
# so 3 seconds is a wide margin. Only status 124 means the wait timed out, the expected outcome.
# Status 0 means the walk kept going, and any other status means the wait itself failed.
kill_wait_rc=0
timeout 3 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || kill_wait_rc=$?
if [ "$kill_wait_rc" -eq 0 ]; then
    echo "FAIL: the enumeration polled again after the query was cancelled"
elif [ "$kill_wait_rc" -eq 124 ]; then
    echo "cancelled: structure inference stopped inside one column"
else
    echo "FAIL: the wait failed with status ${kill_wait_rc}"
fi

# Release before waiting: on an unpatched build the walk is paused at a later poll right now, and
# DISABLE is the only statement that wakes it. NOTIFY would leave the channel enabled and the walk
# would pause again at the next poll.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
timeout 30 tail --pid="${KILL_PID}" -f /dev/null
wait "${KILL_PID}" 2>/dev/null ||:

$CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT count() FROM system.query_log
    WHERE query_id = '${KILL_QID}' AND current_database = currentDatabase() AND exception_code = 394;
"

# ---- timeout_overflow_mode = 'break' must abort too ----
# In that mode checkTimeLimit returns false instead of throwing, and such a deadline never sets the
# killed flag. Unlike the child-plan loop in the same file, which keeps the plans it built, a
# truncated schema is a wrong structure rather than a smaller result: CREATE TABLE ... AS merge(...)
# writes the inferred structure into the table's metadata. So this must fail rather than return.
# The pause makes the elapsed time exceed max_execution_time without relying on timing.
#
# This arm reaches the walk through the widening branch instead of the first-column one: cheap0 sorts
# before deep0, so `c` enters the union as Int32 and deep0 widens it, which re-walks the supertype.
# cheap0's own column is far too cheap to poll, so every poll here is still inside one column's walk.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS cheap0;
    CREATE TABLE cheap0 (k UInt32, c Int32) ENGINE = MergeTree ORDER BY k;
    SYSTEM ENABLE FAILPOINT ${FP};
"

$CLICKHOUSE_CLIENT --query_id="${BREAK_QID}" --query "
    DESCRIBE TABLE merge(currentDatabase(), '^(cheap|deep)0$')
    SETTINGS max_execution_time = 3, timeout_overflow_mode = 'break'
    FORMAT Null
" > /dev/null 2> "${CLICKHOUSE_TMP}/05158_break.err" &
BREAK_PID=$!

if ! timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
    echo "FAIL: structure inference never polled inside the enumeration in the break arm"
fi
sleep 4
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${FP}"

break_wait_rc=0
timeout 3 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || break_wait_rc=$?
if [ "$break_wait_rc" -eq 0 ]; then
    echo "FAIL: the enumeration polled again in 'break' overflow mode"
elif [ "$break_wait_rc" -eq 124 ]; then
    echo "break mode: structure inference stopped inside one column"
else
    echo "FAIL: the wait failed with status ${break_wait_rc}"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
timeout 30 tail --pid="${BREAK_PID}" -f /dev/null
break_client_rc=0
wait "${BREAK_PID}" || break_client_rc=$?

# 159 is TIMEOUT_EXCEEDED. Match the message too, so the arm cannot pass because some other site hit
# the deadline first.
echo "break mode client status: ${break_client_rc}"
if grep -q 'elapsed time limit reached in function merge' "${CLICKHOUSE_TMP}/05158_break.err"; then
    echo "break mode failed inside the Merge structure inference"
else
    echo "FAIL: 'break' overflow mode failed somewhere else:"
    cat "${CLICKHOUSE_TMP}/05158_break.err"
fi
rm -f "${CLICKHOUSE_TMP}/05158_break.err"

# ---- in-range control: a structure that fits the deadline is described exactly as before ----
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS shallow0;
    CREATE TABLE shallow0 (k UInt32, c Array(Map(String, Tuple(a UInt8, b UInt8)))) ENGINE = MergeTree ORDER BY k;
    DESCRIBE TABLE merge(currentDatabase(), '^shallow0$') SETTINGS describe_include_subcolumns = 1;
"

# ---- part loading must not observe the checkpoint ----
# An always-on checkpoint in the enumeration reaches IMergeTreeDataPart::loadColumns, where
# TIMEOUT_EXCEEDED is not retryable, so the parts are renamed into detached/broken-on-start and the
# table comes back empty. Only Merge structure inference passes a budget, so an ATTACH must not
# reach the failpoint at all.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS parts0;
    CREATE TABLE parts0 (k UInt32, c ${SHALLOWER}) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0;
    SYSTEM STOP MERGES parts0;
    INSERT INTO parts0 VALUES (1, []);
    SYSTEM ENABLE FAILPOINT ${FP};
    DETACH TABLE parts0;
"
attach_rc=0
timeout 60 $CLICKHOUSE_CLIENT --query "ATTACH TABLE parts0" > /dev/null 2>&1 || attach_rc=$?
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
if [ "$attach_rc" -eq 0 ]; then
    echo "part loading did not reach the enumeration checkpoint"
else
    echo "FAIL: ATTACH observed the enumeration checkpoint (status ${attach_rc})"
fi
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM parts0;
    SELECT count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 'parts0';
"

# ---- KILL QUERY has to land between two source tables ----
# These two tables are identical, so the second one adds no column and widens none: the per-column
# checkpoint is unreachable for it and only the per-table one can stop the traversal. Their `Int32`
# column is also far too cheap to reach a per-column poll, so ${FP} stays out of this arm entirely.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS same0;
    DROP TABLE IF EXISTS same1;
    CREATE TABLE same0 (k UInt32, c Int32) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE same1 (k UInt32, c Int32) ENGINE = MergeTree ORDER BY k;
    SYSTEM ENABLE FAILPOINT ${FP_TABLE};
"

$CLICKHOUSE_CLIENT --query_id="${TABLE_QID}" --query "
    DESCRIBE TABLE merge(currentDatabase(), '^same[01]\$') FORMAT Null
" > /dev/null 2> "${CLICKHOUSE_TMP}/05158_table.err" &
TABLE_PID=$!

# The first pause is same0's, before its metadata is fetched. The KILL lands while the thread sits
# there and the NOTIFY resumes it without disabling, so same1's pause would still be observable.
if ! timeout 30 $CLICKHOUSE_CLIENT --query "
        SYSTEM WAIT FAILPOINT ${FP_TABLE} PAUSE;
        KILL QUERY WHERE query_id = '${TABLE_QID}' FORMAT Null;
        SYSTEM NOTIFY FAILPOINT ${FP_TABLE};
    " > /dev/null 2>&1; then
    echo "FAIL: structure inference never polled between source tables"
fi

# same1 is one iteration away, so a traversal that ignores the cancellation pauses again within
# microseconds of the resume. Only status 124 means it stopped, the expected outcome.
table_wait_rc=0
timeout 3 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP_TABLE} PAUSE" > /dev/null 2>&1 || table_wait_rc=$?
if [ "$table_wait_rc" -eq 0 ]; then
    echo "FAIL: the traversal reached the next source table after the query was cancelled"
elif [ "$table_wait_rc" -eq 124 ]; then
    echo "cancelled: structure inference stopped between source tables"
else
    echo "FAIL: the wait failed with status ${table_wait_rc}"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP_TABLE}"
timeout 30 tail --pid="${TABLE_PID}" -f /dev/null
wait "${TABLE_PID}" 2>/dev/null ||:

# 394 is QUERY_WAS_CANCELLED. Without this the arm would also pass if the traversal had finished the
# schema and the query had died of something else after it.
if grep -q 'Code: 394' "${CLICKHOUSE_TMP}/05158_table.err"; then
    echo "the cancelled DESCRIBE failed with QUERY_WAS_CANCELLED"
else
    echo "FAIL: the cancelled DESCRIBE did not fail with QUERY_WAS_CANCELLED:"
    cat "${CLICKHOUSE_TMP}/05158_table.err"
fi
rm -f "${CLICKHOUSE_TMP}/05158_table.err"
