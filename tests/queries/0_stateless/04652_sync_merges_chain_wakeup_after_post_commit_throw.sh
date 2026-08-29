#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that makes every merge throw after commit
# no-fasttest: relies on background merge execution

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=merge_throw_after_commit_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# A merge whose post-commit work throws must still wake the merge selecting task, so the next merge
# in a scheduled chain is assigned immediately instead of waiting for the selector's backoff timer.
#
# MergeFromLogEntryTask::finalize commits the result part active and only then does post-commit work
# (zero-copy unlock, cache prewarming) that can throw. `finish_callback`, which schedules
# merge_selecting_task, used to be armed after that work. On a throw it stayed unset, and the retry
# hit `checkExistingPart` (the part is already committed) and retired the queue entry without ever
# reaching the arming point, so nothing rescheduled the selector.
#
# Both merges of the chain are queued in ManualMergeSelector up front, so the dependent merge is
# already pending when the first one throws. It only becomes selectable once the first merge's result
# part is committed, i.e. exactly in the window under test. The backoff is pinned at 300s (with the
# slowdown factor neutralised) so a selector pass can only happen via an explicit wakeup, never via
# the timer, within this test's runtime. The initial wakeup for the first merge comes from an INSERT,
# which schedules the selecting task directly (ReplicatedMergeTreeSink); after that only
# finish_callback can produce one.
#
# Only one replica exists and always_fetch_merged_part = 0, so prepare() returns
# prepared_successfully and the merges run locally, reaching finalize().
# insert_keeper_fault_injection_probability = 0 keeps part names deterministic.
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS sm_chain SYNC;
    CREATE TABLE sm_chain (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm_chain', 'a')
    ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', always_fetch_merged_part = 0, old_parts_lifetime = 600,
             merge_selecting_sleep_ms = 300000, max_merge_selecting_sleep_ms = 300000,
             merge_selecting_sleep_slowdown_factor = 1;

    INSERT INTO sm_chain VALUES (1);
    INSERT INTO sm_chain VALUES (2);
    INSERT INTO sm_chain VALUES (3);
"

# Part names are deterministic with keeper fault injection off: three single-row parts, and merging
# the first two yields all_0_1_1. Assert the sources rather than assuming them.
got_parts=$($CLICKHOUSE_CLIENT -q "SELECT groupArray(name) FROM (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'sm_chain' AND active ORDER BY min_block_number)")
if [ "$got_parts" != "['all_0_0_0','all_1_1_0','all_2_2_0']" ]; then
    echo "FAIL: unexpected initial parts $got_parts"
    exit 1
fi

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Queue the whole chain. The second entry names all_0_1_1, which does not exist yet, so the selector
# cannot match it until the first merge has committed its result part.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_chain PARTS 'all_0_0_0', 'all_1_1_0'"
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_chain PARTS 'all_0_1_1', 'all_2_2_0'"

# The one allowed wakeup: an INSERT schedules the selecting task immediately, which assigns the first
# merge of the chain. Its extra part is never part of any scheduled merge.
$CLICKHOUSE_CLIENT -q "INSERT INTO sm_chain SETTINGS insert_keeper_fault_injection_probability = 0 VALUES (4)"

# SYNC MERGES waits for the whole chain: its snapshot contains all four scheduled source parts, so it
# returns only once all_0_1_1 is itself covered by the dependent merge's result and both part_log rows
# are queued. With the fix the throwing first merge wakes the selector before unwinding, so the
# dependent merge is assigned at once (SYNC_OK). Without it the selector sleeps for 300s and the
# command times out (SYNC_TIMEOUT).
if $CLICKHOUSE_CLIENT --max_execution_time 30 -q "SYSTEM SYNC MERGES sm_chain" 2>/dev/null; then
    echo SYNC_OK
else
    echo SYNC_TIMEOUT
fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

# SYNC_OK on its own would also be produced if the failpoint silently stopped firing, because then
# both merges just succeed and each arms the wakeup on its normal success path. So assert that both
# merges of the chain really did throw in the post-commit window (the failpoint is server-wide, so it
# fires for the dependent merge too): each leaves exactly one MergeParts row and it carries a non-zero
# error. Neither part gets a successful row, because the retry finds the part already committed and
# retires the queue entry via checkExistingPart without re-running the merge -- which is exactly why
# the wakeup has to be armed before the throw rather than after it.
#
# The dependent merge's result part being active is the independent confirmation that the chain
# completed rather than SYNC MERGES returning on a partial state.
#
# enable_parallel_replicas is pinned off only for these part_log counts: reading system.part_log over
# parallel replicas changes the aggregation and is unrelated to what is under test.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS part_log;
    SELECT
        countIf(part_name = 'all_0_1_1' AND error > 0) AS first_merge_threw,
        countIf(part_name = 'all_0_1_1' AND error = 0) AS first_merge_success_rows,
        countIf(part_name = 'all_0_2_2' AND error > 0) AS dependent_merge_threw,
        countIf(part_name = 'all_0_2_2' AND error = 0) AS dependent_merge_success_rows
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_chain' AND event_type = 'MergeParts'
    SETTINGS enable_parallel_replicas = 0;

    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'sm_chain' AND active AND level > 0;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE sm_chain SYNC"
