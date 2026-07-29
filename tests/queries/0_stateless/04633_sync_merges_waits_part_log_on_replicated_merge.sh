#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# no-shared-merge-tree: the merge/fetch process differs from RMT
# no-parallel: uses a server-wide failpoint that makes every merge throw after commit
# no-fasttest: relies on background merge execution and the part_log

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=merge_throw_after_commit_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# Replicated counterpart of 04413. A scheduled merge on a ReplicatedMergeTree table runs through
# MergeFromLogEntryTask, which commits the result part active before it queues its part_log row.
# If the post-commit work throws (memory limit, cache prewarming), the entry is retried and the
# merge list entry is destroyed with no part_log row while the result part stays active. SYSTEM SYNC
# MERGES must still not return until that (failed) MergeParts row is queued, otherwise a following
# FLUSH LOGS part_log can drain the queue before the row is pushed.
#
# Only one replica exists and always_fetch_merged_part = 0, so no other replica can have the result
# part: MergeFromLogEntryTask::prepare returns prepared_successfully = true and the merge is
# executed locally, reaching MergeFromLogEntryTask::finalize. 04412 / 04414 / 04415 / 04416 all use
# a second replica that already holds the merged part, so they take the fetch path instead.
# insert_keeper_fault_injection_probability = 0 keeps part names deterministic.
$CLICKHOUSE_CLIENT -m -q "
    SET insert_keeper_fault_injection_probability = 0;

    DROP TABLE IF EXISTS sm_rmt SYNC;
    CREATE TABLE sm_rmt (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/sm_rmt', 'a')
    ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', always_fetch_merged_part = 0, old_parts_lifetime = 600;

    INSERT INTO sm_rmt VALUES (1);
    INSERT INTO sm_rmt VALUES (2);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Schedule the merge of the two parts. The merge commits all_0_1_1 active and then throws in the
# post-commit window; SYNC MERGES must wait until the failed-merge part_log row is queued.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_rmt PARTS 'all_0_0_0', 'all_1_1_0'"

# With the fix the task writes its part_log row before unwinding, so the row is present right after
# SYNC MERGES (count = 1). Without it the retried task leaves the part active with no part_log row
# and SYNC MERGES returns early (count = 0).
#
# enable_parallel_replicas is pinned off only for the part_log count: reading system.part_log over
# parallel replicas changes the aggregation and is unrelated to the part_log ordering under test.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SYNC MERGES sm_rmt;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_rmt'
      AND event_type = 'MergeParts' AND part_name = 'all_0_1_1' AND error > 0
    SETTINGS enable_parallel_replicas = 0;
"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "DROP TABLE sm_rmt SYNC"
