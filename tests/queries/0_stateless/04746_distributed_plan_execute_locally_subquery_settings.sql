-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t1_04746;
DROP TABLE IF EXISTS t2_04746;
CREATE TABLE t1_04746 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2_04746 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1_04746 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2_04746 SELECT number, toString(number) FROM numbers(100);

-- A jointly-scoped SETTINGS clause gives the subquery its own context. Every consumer of
-- `distributed_plan_execute_locally` must observe the value the plan was built with; a consumer that
-- re-read it from the ambient context used to select the remote executor with no worker hosts.

-- max_rows_to_group_by is reset because the test profile sets it to a nonzero value and distributed
-- aggregation rejects any limit; enable_parallel_replicas is pinned off because the test runner
-- randomizes it.

-- Local execution requested only inside the scalar subquery. Used to crash the server.
SELECT (
    SELECT count()
    FROM t1_04746 INNER JOIN t2_04746 ON intDiv(t1_04746.key, 2) = t2_04746.key
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
        distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
        enable_parallel_replicas = 0, max_rows_to_group_by = 0
) SETTINGS log_comment = 'local_direction_04746';

-- The opposite direction: the subquery turns local execution off while the outer query has it on.
-- The subquery's value must win, i.e. the plan is dispatched to workers.
SELECT (
    SELECT count()
    FROM t1_04746 INNER JOIN t2_04746 ON intDiv(t1_04746.key, 2) = t2_04746.key
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, serialize_query_plan = 1,
        distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
        enable_parallel_replicas = 0, max_rows_to_group_by = 0
) SETTINGS distributed_plan_execute_locally = 1, log_comment = 'jointly_scoped_04746';

-- Both modes return the same rows, so assert the execution mode itself. Aggregates keep exactly one
-- output row whichever mode ran, so a wrong mode shows up as wrong values rather than a missing row.
SYSTEM FLUSH LOGS query_log;
SELECT
    max(ProfileEvents['DistributedPlanLocalExecution']) AS ran_in_process,
    max(ProfileEvents['DistributedPlanRemoteTasks']) > 0 AS dispatched_to_workers
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'local_direction_04746'
    AND type = 'QueryFinish';
SELECT
    max(ProfileEvents['DistributedPlanLocalExecution']) AS ran_in_process,
    max(ProfileEvents['DistributedPlanRemoteTasks']) > 0 AS dispatched_to_workers
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'jointly_scoped_04746'
    AND type = 'QueryFinish';

-- Control: the same shape with the settings applied to the whole query instead of a nested
-- subquery, so nothing diverges. Runs in process; a mode oracle guards against a blanket change
-- that would dispatch it.
SELECT count() FROM (
    SELECT DISTINCT t2_04746.val
    FROM t1_04746 INNER JOIN t2_04746 ON intDiv(t1_04746.key, 2) = t2_04746.key
) SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, serialize_query_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0, max_rows_to_group_by = 0,
    log_comment = 'control_04746';

SYSTEM FLUSH LOGS query_log;
SELECT
    max(ProfileEvents['DistributedPlanLocalExecution']) AS ran_in_process,
    max(ProfileEvents['DistributedPlanRemoteTasks']) > 0 AS dispatched_to_workers
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'control_04746'
    AND type = 'QueryFinish';

DROP TABLE t1_04746;
DROP TABLE t2_04746;
