#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the `marks_loader_hold_task_until_canceled` fail point is global and would hold the
#   marks-loading tasks of every concurrent query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every query goes through the plain single-replica path so that the mark cache is warmed and read as expected.
CLIENT="$CLICKHOUSE_CLIENT --enable_parallel_replicas 0"

# A reader that is dropped before its asynchronously loaded marks are needed cancels the queued
# marks-loading task. The cancellation is an internal control-flow signal that nobody observes, so
# it must be counted in `LoadingMarksTasksCanceled` but must not be recorded as `ASYNC_LOAD_CANCELED`
# in `system.errors`.
#
# The `PREWHERE` below filters out every row, and no index can prune the part for it, so the reader
# of `v` is created (its marks task is queued) but never asked for any rows, and it is destroyed
# with the marks unread. The fail point holds the task until the destructor cancels it, which makes
# the race deterministic. The marks of `k` are put into the mark cache first (and the part is not
# prewarmed on insert), so the query itself never waits for a held task.

$CLIENT -n -q "
    DROP TABLE IF EXISTS t_marks_canceled;
    CREATE TABLE t_marks_canceled (k UInt64, v String) ENGINE = MergeTree ORDER BY ()
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, prewarm_mark_cache = 0;
    INSERT INTO t_marks_canceled SELECT number, toString(number) FROM numbers(100000);
    SYSTEM STOP MERGES t_marks_canceled;
    SELECT sum(k) FROM t_marks_canceled FORMAT Null;
"

errors_before=$($CLIENT -q "SELECT sum(value) FROM system.errors WHERE name = 'ASYNC_LOAD_CANCELED'")

query_id="${CLICKHOUSE_DATABASE}_marks_canceled_$RANDOM"

$CLIENT -q "SYSTEM ENABLE FAILPOINT marks_loader_hold_task_until_canceled"

$CLIENT --query_id "$query_id" -q "
    SELECT v FROM t_marks_canceled PREWHERE k % 7 = 100
    SETTINGS load_marks_asynchronously = 1,
        allow_prefetched_read_pool_for_local_filesystem = 0,
        allow_prefetched_read_pool_for_remote_filesystem = 0,
        use_query_condition_cache = 0
"

$CLIENT -q "SYSTEM DISABLE FAILPOINT marks_loader_hold_task_until_canceled"

errors_after=$($CLIENT -q "SELECT sum(value) FROM system.errors WHERE name = 'ASYNC_LOAD_CANCELED'")

$CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLIENT -q "
    SELECT 'canceled tasks', ProfileEvents['LoadingMarksTasksCanceled'] >= 1
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'
"

echo "new ASYNC_LOAD_CANCELED errors $((errors_after - errors_before))"

$CLIENT -q "DROP TABLE t_marks_canceled"
