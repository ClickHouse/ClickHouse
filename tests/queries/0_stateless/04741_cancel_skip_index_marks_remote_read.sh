#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: uses failpoints that would intersect with concurrent tests
# no-fasttest: needs the s3 disk (minio) for a remote read

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=remote_fs_gather_pause_in_read

# A leaked enabled failpoint would block every later copy of this test and fake a deadlock in
# unrelated work, so disable it on every exit path. DISABLE on an inactive failpoint is a no-op.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT '"$FP"'" 2>/dev/null || true' EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_marks_cancel SYNC;

    CREATE TABLE t_marks_cancel
    (
        id UInt64,
        s String,
        INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS storage_policy = 's3_cache', index_granularity = 512, min_bytes_for_wide_part = 0;

    INSERT INTO t_marks_cancel
    SELECT number, 'tok' || toString(number % 5000) || ' filler text to make the part wide'
    FROM numbers(200000);
"

# Cold caches, so the index marks are really read from object storage.
$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP MARK CACHE;
    SYSTEM DROP INDEX MARK CACHE;
    SYSTEM DROP FILESYSTEM CACHE;
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

query_id="04741_${CLICKHOUSE_DATABASE}"

# A small read buffer makes the marks read span several buffer fills, so a second fill exists for
# the assertion below to be about cancellation rather than about the read simply having finished.
$CLICKHOUSE_CLIENT --query_id "$query_id" \
    --max_read_buffer_size 500 \
    --query "SELECT count() FROM t_marks_cancel WHERE hasToken(s, 'tok4242')" > /dev/null 2>&1 &
query_pid=$!

# The failpoint is inside the opt-in branch, so reaching it also proves the marks reader asked
# for interruptible reads. Bounded, because a build without the opt-in never pauses at all and
# an unbounded wait would spend the whole test timeout discovering that.
if timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" > /dev/null 2>&1; then
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"

    # Single-step exactly one fill: NOTIFY resumes the paused thread and leaves the failpoint
    # armed, so the next fill would pause again.
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP"

    # The discriminator, as a race between the two possible outcomes so that both resolve fast.
    # With the cancellation check the resumed read unwinds and the query leaves system.processes.
    # Without it the read fetches another buffer and pauses again, which this backgrounded wait
    # reports by creating the marker.
    paused_again_marker="${CLICKHOUSE_TMP:-/tmp}/04741_paused_again_${CLICKHOUSE_DATABASE}"
    rm -f "$paused_again_marker"
    (
        $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" > /dev/null 2>&1 \
            && touch "$paused_again_marker"
    ) &
    waiter_pid=$!

    verdict="FAIL: the read neither unwound nor paused again"
    for _ in {1..600}; do
        if [ -e "$paused_again_marker" ]; then
            verdict="FAIL: the read paused again after cancellation"
            break
        fi
        running=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
        if [ "$running" = "0" ]; then
            verdict="cancelled"
            break
        fi
        sleep 0.1
    done
    echo "$verdict"
else
    echo "FAIL: the marks read never reached the interruption point"
fi

# DISABLE also releases the backgrounded waiter above, whether or not it ever saw a pause.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait ${waiter_pid:-} 2>/dev/null || true
wait $query_pid 2>/dev/null || true

# Regression guard for the opt-in's scope, not a live oracle: it passes today because the flag is
# never set on the part-validation path. It exists to redden if that opt-in is ever widened, since
# a cancellation exception there is reported as a corrupt part.
start_time=$($CLICKHOUSE_CLIENT --query "SELECT now64(6)")
check_query_id="04741_check_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$check_query_id" --query "CHECK TABLE t_marks_cancel" > /dev/null 2>&1 &
check_pid=$!
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$check_query_id' ASYNC FORMAT Null"
wait $check_pid 2>/dev/null || true

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.text_log
    WHERE event_time_microseconds >= toDateTime64('$start_time', 6)
      AND (message LIKE '%looks broken%' OR message LIKE '%broken-on-start%');
"

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_marks_cancel' AND active;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_marks_cancel SYNC"
