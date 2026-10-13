#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer, no-parallel
# no-old-analyzer: make_distributed_plan requires the analyzer.
# no-parallel: enables a global failpoint that would disrupt other distributed-plan queries.

# Regression test: a worker status-check re-enqueue that throws (e.g. CANNOT_SCHEDULE_TASK on
# shutdown, or MEMORY_LIMIT_EXCEEDED under fuzzing) must fail the query, not the server. Before
# the fix the throw escaped the TaskTracker worker lambda, was stored in the pool, and rethrown by
# thread_pool.wait() in ~TaskTracker, which std::terminated the server (Received signal 6).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dp_reenqueue (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dp_reenqueue SELECT number FROM numbers(100000)"

# Instant captured (server-side, same clock as text_log.event_time_microseconds) before the
# failpoint is enabled. The flaky check reruns this test on the same server, so the assertion below
# must count only THIS run's recordFailure lines and ignore any left by an earlier run. Microseconds
# since epoch is timezone-independent (the flaky check randomizes session_timezone).
RUN_START=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))")

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT distributed_plan_status_check_reenqueue_fault"

# Distributed reads go through the remote executor, whose TaskTracker polls worker task status on a
# background pool. The failpoint throws while re-enqueueing the next status check; the worker lambda
# must catch it (recordFailure: log + store as the query's first_exception) so the query fails with
# a clean CANNOT_SCHEDULE_TASK instead of letting the throw escape and terminate the server.
#
# Whether the client sees the error is racy (the query can finish before the stored exception is
# observed), so the result is discarded. The deterministic proof that the fault path ran is the
# recordFailure() log line: it is emitted on the pool thread on every re-enqueue, independent of the
# client-side completion race. A few queries are issued so at least one re-enqueue cycle happens.
for _ in {1..3}; do
    $CLICKHOUSE_CLIENT -q "
        SELECT x, count() FROM t_dp_reenqueue GROUP BY x FORMAT Null
        SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 0,
            distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3,
            distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0
    " > /dev/null 2>&1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT distributed_plan_status_check_reenqueue_fault"

# The injected fault was handled by TaskTracker::recordFailure (SOURCE fix): the worker lambda's
# re-enqueue throw was caught and stored instead of escaping the thread. >= 1 log line proves it ran.
# Filtered on event_time_microseconds > RUN_START so only THIS run's lines count (the flaky check
# reruns the test on the same server). event_date >= yesterday() only prunes the partition scan; the
# microsecond filter does the run-scoping (and never excludes a valid this-run row at a day boundary).
# enable_parallel_replicas=0: the randomized parallel-replicas cluster need not exist for this
# auxiliary system.text_log read.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'reenqueue fault handled: ', count() >= 1
    FROM system.text_log
    WHERE event_date >= yesterday()
      AND toUnixTimestamp64Micro(event_time_microseconds) > ${RUN_START}
      AND logger_name LIKE '%TaskTracker::recordFailure%'
      AND message LIKE '%Injected re-enqueue fault%'
    SETTINGS enable_parallel_replicas = 0
"

# The point of the test: the server is still alive (DEFENSE fix: ~TaskTracker swallows any rethrow
# from thread_pool.wait() instead of std::terminate-ing the server).
$CLICKHOUSE_CLIENT -q "SELECT 'server alive'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dp_reenqueue"
