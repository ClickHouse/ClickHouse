#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper
# no-parallel: uses failpoints that would intersect with concurrent tests
# no-fasttest: needs the s3 disk (minio) for a remote read
# zookeeper: the second stanza needs ReplicatedMergeTree for the broken-part callback

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
    # Assert the precondition rather than inferring it: the query must be parked at the failpoint,
    # so it is present in system.processes before the KILL can mean anything.
    parked=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")
    if [ "$parked" != "1" ]; then
        echo "FAIL: the paused read does not belong to the test query"
    fi

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

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_marks_cancel' AND active;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_marks_cancel SYNC"

# --------------------------------------------------------------------------------------------
# Second stanza: the opt-in must NOT reach the patch-part minmax read.
#
# That read runs under MergeTreeSequentialSource, whose handler reports a non-retryable exception
# as a broken part, and QUERY_WAS_CANCELLED is not retryable. OPTIMIZE ... DRY RUN carries the
# user query's QueryStatus into an inline merge, so if the opt-in were set there, killing the
# query would enqueue a healthy Active source part for a check.
# --------------------------------------------------------------------------------------------

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_patch_cancel SYNC"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_patch_cancel (id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_patch_cancel', 'r1')
    ORDER BY id
    SETTINGS storage_policy = 's3_cache', index_granularity = 512, min_bytes_for_wide_part = 0,
             enable_block_number_column = 1, enable_block_offset_column = 1,
             apply_patches_on_merge = 0;

    INSERT INTO t_patch_cancel SELECT number, 0 FROM numbers(2000);
    INSERT INTO t_patch_cancel SELECT number + 2000, 0 FROM numbers(2000);
    INSERT INTO t_patch_cancel SELECT number + 4000, 0 FROM numbers(2000);
"

# A patch whose source parts are then merged away can only be applied in Join mode, which is the
# mode that reads the patch part's implicit minmax index.
$CLICKHOUSE_CLIENT --query "
    SYSTEM STOP MERGES t_patch_cancel;
    SET enable_lightweight_update = 1;
    UPDATE t_patch_cancel SET v = 7 WHERE id < 100;
"
$CLICKHOUSE_CLIENT --query "
    SYSTEM START MERGES t_patch_cancel;
    OPTIMIZE TABLE t_patch_cancel PARTITION tuple() FINAL SETTINGS optimize_throw_if_noop = 0;
    ALTER TABLE t_patch_cancel MODIFY SETTING apply_patches_on_merge = 1;
    SYSTEM STOP MERGES t_patch_cancel;
"

# Assert the fixture instead of assuming it: exactly one merged data part covering the patch's
# source parts, plus the patch part itself.
merged_part=$($CLICKHOUSE_CLIENT --query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 't_patch_cancel' AND active
      AND NOT startsWith(name, 'patch')
")
patch_parts=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_patch_cancel' AND active
      AND startsWith(name, 'patch')
")
if [ "$merged_part" != "all_0_2_1" ] || [ "$patch_parts" != "1" ]; then
    echo "FAIL: patch fixture not as expected (part='$merged_part', patches=$patch_parts)"
fi

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP MARK CACHE;
    SYSTEM DROP INDEX MARK CACHE;
    SYSTEM DROP FILESYSTEM CACHE;
"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

patch_query_id="04741_patch_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$patch_query_id" \
    --max_read_buffer_size 500 \
    --query "OPTIMIZE TABLE t_patch_cancel DRY RUN PARTS '$merged_part'" > /dev/null 2>&1 &
patch_pid=$!

# Not reaching the failpoint is the CORRECT post-fix outcome, so it is a pass with its own token.
if timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" > /dev/null 2>&1; then
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$patch_query_id' ASYNC FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
    wait $patch_pid 2>/dev/null || true

    # The part is healthy, so it is reported as "looks good" and never as "looks broken": the
    # observable harm is that a check was requested for it at all.
    for _ in {1..300}; do
        checked=$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.replication_queue
            WHERE database = currentDatabase() AND table = 't_patch_cancel'
        " 2>/dev/null || echo 0)
        [ "$checked" != "0" ] && break
        sleep 0.1
    done
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    spurious=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.text_log
        WHERE logger_name LIKE concat(currentDatabase(), '.t_patch_cancel%PartCheckThread%')
          AND message LIKE 'Checking part %'
    ")
    echo "patch-read-interruptible spurious-part-checks=$spurious"
else
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
    wait $patch_pid 2>/dev/null || true
    echo "patch-read-not-interruptible"
fi

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_patch_cancel' AND active
      AND NOT startsWith(name, 'patch');
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_patch_cancel SYNC"
