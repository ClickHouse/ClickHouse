#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: uses a server-wide failpoint that makes every merge throw after commit
# no-fasttest: relies on background merge execution and the part_log

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP=merge_throw_after_commit_before_part_log

cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true; }
trap cleanup EXIT

# A scheduled merge makes its result part active by committing the transaction, which happens
# before the task queues its part_log row. If the post-commit work throws (memory limit, cache
# prewarming), the background executor cancels the task and destroys its merge list entry with no
# part_log row, while the result part stays active. SYSTEM SYNC MERGES must still not return until
# the merge has queued its (failed) part_log row, otherwise a following FLUSH LOGS part_log can
# drain the queue before the row is pushed.
#
# merge_throw_after_commit_before_part_log forces exactly that: the merge commits the result part
# and then throws in the post-commit/pre-part_log window. With the fix the task writes its part_log
# row before unwinding, so the row is present right after SYNC MERGES (count = 1). Without the fix
# the task is cancelled with no part_log row and SYNC MERGES returns early (count = 0).
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS sm_exc SYNC;
    CREATE TABLE sm_exc (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', old_parts_lifetime = 600;
    INSERT INTO sm_exc VALUES (1);
    INSERT INTO sm_exc VALUES (2);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"

# Schedule the merge of the two parts. The merge commits all_1_2_1 active and then throws in the
# post-commit window; SYNC MERGES must wait until the failed-merge part_log row is queued.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE sm_exc PARTS 'all_1_1_0', 'all_2_2_0'"

# enable_parallel_replicas is pinned off only for the part_log count: reading system.part_log over
# parallel replicas changes the aggregation and is unrelated to the part_log ordering under test.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM SYNC MERGES sm_exc;
    SYSTEM FLUSH LOGS part_log;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 'sm_exc'
      AND event_type = 'MergeParts' AND part_name = 'all_1_2_1' AND error > 0
    SETTINGS enable_parallel_replicas = 0;
"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "DROP TABLE sm_exc SYNC"
