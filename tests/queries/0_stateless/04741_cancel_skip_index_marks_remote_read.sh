#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper, no-shared-merge-tree
# no-parallel: uses failpoints that would intersect with concurrent tests
# no-fasttest: needs the s3 disk (minio) for a remote read
# zookeeper: the second stanza needs ReplicatedMergeTree for the broken-part callback
# no-shared-merge-tree: the second stanza pins storage_policy='s3_cache' on a replicated table, and SharedMergeTree requires a keeper-metadata disk

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
#
# query_plan_direct_read_from_text_index reads a text index in the query plan instead of through
# MergeTreeIndexReader, so with it enabled this query never performs the marks read the test is
# about. It is pinned off in every arm that has to reach the interruption point.
$CLICKHOUSE_CLIENT --query_id "$query_id" \
    --max_read_buffer_size 500 \
    --query_plan_direct_read_from_text_index 0 \
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

# The same read with the executor requested. `use_reader_executor` needs a non-threadpool read
# method to be taken at all (a threadpool method adds async prefetch, which the executor does not
# support and already falls back for), so both settings are required to exercise it.
exec_query_id="04741_exec_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP MARK CACHE;
    SYSTEM DROP INDEX MARK CACHE;
    SYSTEM DROP FILESYSTEM CACHE;
"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

$CLICKHOUSE_CLIENT --query_id "$exec_query_id" \
    --max_read_buffer_size 500 \
    --use_reader_executor 1 \
    --remote_filesystem_read_method read \
    --query_plan_direct_read_from_text_index 0 \
    --query "SELECT count() FROM t_marks_cancel WHERE hasToken(s, 'tok4242')" > /dev/null 2>&1 &
exec_query_pid=$!

# The executor has no interruption point, so reaching the failpoint proves the opt-in made the
# read fall back to the path that honors it.
if timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" > /dev/null 2>&1; then
    # WAIT FAILPOINT is global, so assert the pause belongs to this query: otherwise a query
    # that already finished without the fallback would leave system.processes empty and be
    # reported below as a success.
    parked=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$exec_query_id'")
    if [ "$parked" != "1" ]; then
        echo "FAIL: the paused read does not belong to the executor test query"
    fi

    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$exec_query_id' ASYNC FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP"

    verdict="FAIL: the read did not unwind after cancellation"
    for _ in {1..600}; do
        running=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$exec_query_id'")
        if [ "$running" = "0" ]; then
            verdict="cancelled-with-reader-executor"
            break
        fi
        sleep 0.1
    done
    echo "$verdict"
else
    echo "FAIL: the marks read never reached the interruption point with use_reader_executor"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $exec_query_pid 2>/dev/null || true

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
             apply_patches_on_merge = 0,
             -- Only a v1 patch is applied in Join mode, the mode that reads the patch part's
             -- implicit minmax index. A v2 patch is applied in MergeOnKey mode, which performs
             -- no such read.
             patch_parts_version = 'v1';

    -- A retried insert consumes ZooKeeper block numbers, so part ids stop being
    -- deterministic and the fixture assertion below no longer knows the merged part's name.
    SET insert_keeper_fault_injection_probability = 0;

    -- Each insert schedules merge selection, so without this the first parts can merge before
    -- the patch is written and the final merge then produces a different merge level.
    SYSTEM STOP MERGES t_patch_cancel;

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
# The merge level depends on how many merges ran, so match the block range the part has to cover
# rather than a particular level.
case "$merged_part" in
    all_0_2_*) ;;
    *) echo "FAIL: patch fixture not as expected (part='$merged_part', patches=$patch_parts)" ;;
esac
if [ "$patch_parts" != "1" ]; then
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
# Bound: a cold-cache run of this OPTIMIZE measures ~0.12 s, so 15 s is ample.
if timeout 15 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" > /dev/null 2>&1; then
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$patch_query_id' ASYNC FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
    wait $patch_pid 2>/dev/null || true

    # The part is healthy, so it is reported as "looks good" and never as "looks broken": the
    # observable harm is that a check was requested for it at all. ReplicatedPartChecks counts
    # requested checks; the private parts_queue of the check thread has no system table.
    for _ in {1..300}; do
        checked=$($CLICKHOUSE_CLIENT --query "
            SELECT value FROM system.events WHERE event = 'ReplicatedPartChecks'
        " 2>/dev/null || echo 0)
        [ -n "$checked" ] && [ "$checked" != "0" ] && break
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
    # A timed-out WAIT does not by itself prove the read never paused, and DISABLE FAILPOINT
    # RELEASES a thread that is already parked. Disabling before the query's fate is known would
    # therefore let a read that paused just after the bound finish with status 0 and be reported
    # as a pass. So establish that the query is gone while the failpoint is still ARMED, where a
    # parked read provably cannot exit, and make "still running" its own failure.
    # Bound: a cold-cache run of this OPTIMIZE measures ~0.13 s and the WAIT above already gave
    # it 15 s, so 10 s more is ample.
    reaped=0
    for _ in {1..100}; do
        kill -0 "$patch_pid" 2>/dev/null || { reaped=1; break; }
        sleep 0.1
    done
    if [ "$reaped" = "0" ]; then
        echo "FAIL: the patch-read query is still running, so it paused at the failpoint"
    fi

    # After the verdict above, so that releasing a parked read can no longer hide it. Also lets
    # the wait below return instead of blocking until the test timeout in the parked case.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

    # Not reaching the failpoint is only meaningful if the read actually ran, so keep the
    # query's outcome instead of discarding it: an uncancelled DRY RUN must succeed.
    patch_status=0
    wait $patch_pid || patch_status=$?
    if [ "$patch_status" != "0" ]; then
        echo "FAIL: the patch-read query failed with status $patch_status instead of completing"
    fi

    # Positive control. Without one, this branch is a proof of a negative: any other reason the
    # read never reached the failpoint (a parse error, a missing patch part, a read that never
    # started) would print the same token. PatchesJoinRowsAddedToHashTable is incremented in
    # PatchJoinCache::Entry::addBlock, reached only via getEntries, which runs after the
    # getStatsEntry call that performs the patch minmax read - so a non-zero value proves the
    # governed read executed. AnalyzePatchRangesMicroseconds would NOT do: it is also
    # incremented by addPart/optimize/getRanges, which run for a Merge-mode patch that never
    # reads the minmax index at all (measured: 14 microseconds with no such read).
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    join_rows=$($CLICKHOUSE_CLIENT --query "
        SELECT sum(ProfileEvents['PatchesJoinRowsAddedToHashTable'])
        FROM system.query_log
        WHERE query_id = '$patch_query_id' AND current_database = currentDatabase()
          AND type != 'QueryStart'
    ")
    if [ -z "$join_rows" ] || [ "$join_rows" = "0" ]; then
        echo "FAIL: the patch minmax read never ran, so the arm proves nothing (join_rows='$join_rows')"
    fi

    echo "patch-read-not-interruptible"
fi

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_patch_cancel' AND active
      AND NOT startsWith(name, 'patch');
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_patch_cancel SYNC"
