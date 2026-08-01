#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that makes every fetch throw after commit
# no-fasttest: starts background merges/fetches across two replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=rmt_fetch_throw_after_commit_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# Fetch-path counterpart of 04652. A fetch whose post-commit work throws must still wake the merge
# selecting task, so the next merge in a scheduled chain is assigned immediately instead of waiting
# for the selector's backoff timer.
#
# StorageReplicatedMergeTree::fetchPart commits the fetched part active and only then does the quorum
# updates, which talk to Keeper and can throw. merge_selecting_task->schedule() used to run after
# them. On a throw it was skipped, and the retry hit checkExistingPart (the part is already
# committed) and retired the queue entry through removeProcessedEntry, which does not wake the
# selector, so nothing rescheduled it.
#
# Replica 'b' is the one under test: it satisfies each scheduled merge by FETCHING the result part
# from 'a' (always_fetch_merged_part = 1) and throws in the post-commit window. Both replicas use the
# Manual selector so neither invents merges of its own, and only 'b' is given the SCHEDULE MERGE
# pushes, so 'b' creates the log entries and 'a' picks them up from the shared log and merges locally
# (always_fetch_merged_part = 0).
#
# Both merges of the chain are queued on 'b' up front, so the dependent merge is already pending when
# the first fetch throws. It only becomes selectable once all_0_1_1 exists, i.e. exactly in the window
# under test. 'b's backoff is pinned at 300s (with the slowdown factor neutralised) so within this
# test's runtime a selector pass on 'b' can only happen via an explicit wakeup, never via the timer.
# The initial wakeup for the first merge comes from an INSERT on 'b', which schedules the selecting
# task directly (ReplicatedMergeTreeSink); after that only the fetch's own wakeup can produce one.
# insert_keeper_fault_injection_probability = 0 keeps part names deterministic.
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS sm_fa SYNC;
    DROP TABLE IF EXISTS sm_fb SYNC;

    CREATE TABLE sm_fa (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm_f', 'a')
    ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', always_fetch_merged_part = 0, old_parts_lifetime = 600;

    CREATE TABLE sm_fb (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm_f', 'b')
    ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', always_fetch_merged_part = 1, old_parts_lifetime = 600,
             merge_selecting_sleep_ms = 300000, max_merge_selecting_sleep_ms = 300000,
             merge_selecting_sleep_slowdown_factor = 1;

    INSERT INTO sm_fa VALUES (1);
    INSERT INTO sm_fa VALUES (2);
    INSERT INTO sm_fa VALUES (3);
    SYSTEM SYNC REPLICA sm_fb;
"

# Part names are deterministic with keeper fault injection off: three single-row parts, and merging
# the first two yields all_0_1_1. Assert the sources rather than assuming them.
got_parts=$($CLICKHOUSE_CLIENT -q "SELECT groupArray(name) FROM (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'sm_fb' AND active ORDER BY min_block_number)")
if [ "$got_parts" != "['all_0_0_0','all_1_1_0','all_2_2_0']" ]; then
    echo "FAIL: unexpected initial parts $got_parts"
    exit 1
fi

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Queue the whole chain on 'b'. The second entry names all_0_1_1, which does not exist yet, so 'b's
# selector cannot match it until the first merge's result part has been fetched and committed.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_fb PARTS 'all_0_0_0', 'all_1_1_0'"
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_fb PARTS 'all_0_1_1', 'all_2_2_0'"

# The one allowed wakeup: an INSERT on 'b' schedules its selecting task immediately, which creates the
# first merge's log entry. Its extra part is never part of any scheduled merge.
$CLICKHOUSE_CLIENT -q "INSERT INTO sm_fb SETTINGS insert_keeper_fault_injection_probability = 0 VALUES (4)"

# SYNC MERGES on 'b' waits for the whole chain: its snapshot contains all four scheduled source
# parts, so it returns only once all_0_1_1 is itself covered by the dependent merge's result and both
# part_log rows are queued. With the fix the throwing first fetch wakes 'b's selector before
# unwinding, so the dependent merge is created at once (SYNC_OK). Without it 'b's selector sleeps for
# 300s and the command times out (SYNC_TIMEOUT).
if $CLICKHOUSE_CLIENT --max_execution_time 60 -q "SYSTEM SYNC MERGES sm_fb" 2>/dev/null; then
    echo SYNC_OK
else
    echo SYNC_TIMEOUT
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

# SYNC_OK on its own would also be produced if the failpoint silently stopped firing, because then
# both fetches just succeed and each reaches the wakeup on its normal path. So assert that both
# fetches of the chain really did throw in the post-commit window (the failpoint is server-wide, so it
# fires for the dependent fetch too): each leaves exactly one DownloadPart row on 'b' and it carries a
# non-zero error. Neither part gets a successful row, because the retry finds the part already
# committed and retires the queue entry via checkExistingPart without re-fetching -- which is exactly
# why the wakeup has to happen before the throw rather than after it.
#
# The dependent merge's result part being active on 'b' is the independent confirmation that the chain
# completed rather than SYNC MERGES returning on a partial state.
#
# enable_parallel_replicas is pinned off only for these part_log counts: reading system.part_log over
# parallel replicas changes the aggregation and is unrelated to what is under test.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS part_log;
    SELECT
        countIf(part_name = 'all_0_1_1' AND error > 0) AS first_fetch_threw,
        countIf(part_name = 'all_0_1_1' AND error = 0) AS first_fetch_success_rows,
        countIf(part_name = 'all_0_2_2' AND error > 0) AS dependent_fetch_threw,
        countIf(part_name = 'all_0_2_2' AND error = 0) AS dependent_fetch_success_rows
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_fb' AND event_type = 'DownloadPart'
    SETTINGS enable_parallel_replicas = 0;

    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'sm_fb' AND active AND level > 0;
"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE sm_fa SYNC;
    DROP TABLE sm_fb SYNC;
"
