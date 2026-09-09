#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: the two deterministic 10-second waits below, plus the 4-second sleep, are most of
# a test that runs in about 40 seconds, which is a large share of the fast test per-test timeout.
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint, so a concurrent
# instance pausing or resuming the same channel would break the synchronisation.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

FP="storage_merge_schema_inference_pause"
KILL_QID="merge_structure_kill_${CLICKHOUSE_DATABASE}_$$"
BREAK_QID="merge_structure_break_${CLICKHOUSE_DATABASE}_$$"

# Every arm matches exactly ONE source table, so a checkpoint that only ran between source tables
# could not stop any of them. Depth 10 polls 19 times inside that single column's substream walk,
# which is what makes the arms below observations about one column rather than about the union.
DEEP="Int32"
for _ in $(seq 1 10); do DEEP="Array(Map(String, Tuple(a ${DEEP}, b ${DEEP})))"; done
# Depth 8 still polls 4 times, and its parts hold a fifth as many stream files, so the part-loading
# arm can reach the checkpoint while staying cheap enough to write real parts.
SHALLOWER="Int32"
for _ in $(seq 1 8); do SHALLOWER="Array(Map(String, Tuple(a ${SHALLOWER}, b ${SHALLOWER})))"; done

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('${KILL_QID}', '${BREAK_QID}') FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

# Structure inference reads metadata only, so this table deliberately holds no data: one row of this
# type occupies about 8000 stream files, which would dominate the test's cost for no coverage.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS deep0;
    CREATE TABLE deep0 (k UInt32, c ${DEEP}) ENGINE = MergeTree ORDER BY k;
"

# Positive control: with no deadline the inference completes and the read runs, returning no rows.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM merge(currentDatabase(), '^deep0$')"

# ---- KILL QUERY has to land inside one column's walk ----
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"

$CLICKHOUSE_CLIENT --query_id="${KILL_QID}" \
    --query "DESCRIBE TABLE merge(currentDatabase(), '^deep0$') FORMAT Null" > /dev/null 2>&1 &
KILL_PID=$!

# The enumeration is now paused at a poll inside the only matched table's column. Bound the wait:
# if it never polls, fail with a diagnostic instead of consuming the per-test timeout.
if ! timeout 30 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
    echo "FAIL: structure inference never polled inside the enumeration in the kill arm"
fi

# Set is_killed while the thread sits inside the walk.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${KILL_QID}' FORMAT Null"

# Resume WITHOUT disabling, so the failpoint channel survives for the second observation.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${FP}"

# A cancelled walk must stop at its next poll, and 18 further polls remain, so it must never pause
# again. Only status 124 means the wait timed out, which is the expected outcome. Status 0 means the
# walk kept going, and any other status means the wait itself failed.
kill_wait_rc=0
timeout 10 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || kill_wait_rc=$?
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

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
    SELECT count() FROM system.query_log
    WHERE query_id = '${KILL_QID}' AND current_database = currentDatabase() AND exception_code = 394
"

# ---- timeout_overflow_mode = 'break' must abort too ----
# In that mode checkTimeLimit returns false instead of throwing, and such a deadline never sets the
# killed flag. Unlike the child-plan loop in the same file, which keeps the plans it built, a
# truncated schema is a wrong structure rather than a smaller result: CREATE TABLE ... AS merge(...)
# writes the inferred structure into the table's metadata. So this must fail rather than return.
# The pause makes the elapsed time exceed max_execution_time without relying on timing.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"

$CLICKHOUSE_CLIENT --query_id="${BREAK_QID}" --query "
    DESCRIBE TABLE merge(currentDatabase(), '^deep0$')
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
timeout 10 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1 || break_wait_rc=$?
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
"
$CLICKHOUSE_CLIENT --query "
    DESCRIBE TABLE merge(currentDatabase(), '^shallow0$') SETTINGS describe_include_subcolumns = 1
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
    INSERT INTO parts0 VALUES (2, []);
    INSERT INTO parts0 VALUES (3, []);
"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT --query "DETACH TABLE parts0"
attach_rc=0
timeout 60 $CLICKHOUSE_CLIENT --query "ATTACH TABLE parts0" > /dev/null 2>&1 || attach_rc=$?
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
if [ "$attach_rc" -eq 0 ]; then
    echo "part loading did not reach the enumeration checkpoint"
else
    echo "FAIL: ATTACH observed the enumeration checkpoint (status ${attach_rc})"
fi
$CLICKHOUSE_CLIENT --query "SELECT count() FROM parts0"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 'parts0'
"
