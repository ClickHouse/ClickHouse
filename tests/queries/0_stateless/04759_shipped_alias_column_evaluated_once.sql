-- Tags: shard

-- An ALIAS column of a table expression shipped as a GLOBAL JOIN temporary table must be computed
-- once per row, and the same number of times whichever way the query is dispatched. A larger count
-- means it is computed both where the shipped side is materialized and again where the outer query
-- reads it back. `sleepEachRow` makes each evaluation countable through `SleepFunctionCalls`.

-- The shipping path under test only exists in the analyzer.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS pr_lazy;
DROP TABLE IF EXISTS pr_lazy_join;

CREATE TABLE pr_lazy (x UInt32, y Int64 ALIAS sleepEachRow(0.05)) ENGINE = MergeTree ORDER BY x
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO pr_lazy(x) SELECT number FROM numbers(20);

CREATE TABLE pr_lazy_join (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO pr_lazy_join SELECT number FROM numbers(20);

-- Arm 1: no parallel replicas, the control.
SELECT r.y FROM pr_lazy_join AS l GLOBAL INNER JOIN pr_lazy AS r ON l.x = r.x FORMAT Null
SETTINGS log_comment = '04759_no_parallel_replicas', enable_parallel_replicas = 0;

-- Arm 2: parallel replicas shipping a plan. This is the arm that used to throw.
SELECT r.y FROM pr_lazy_join AS l GLOBAL INNER JOIN pr_lazy AS r ON l.x = r.x FORMAT Null
SETTINGS log_comment = '04759_parallel_replicas_plan', enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 1;

-- Arm 3: parallel replicas shipping SQL.
SELECT r.y FROM pr_lazy_join AS l GLOBAL INNER JOIN pr_lazy AS r ON l.x = r.x FORMAT Null
SETTINGS log_comment = '04759_parallel_replicas_ast', enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- `log_comment` travels to the replicas, and which entry carries the counter differs by dispatch path
-- (the initiator aggregates the replicas' events on one path but not on the other), so sum over every
-- entry of one execution - they share `initial_query_id`. Keeping only the latest execution per arm
-- makes the test re-runnable in one database, as the flaky check does. Every arm must report one
-- evaluation per row.
SELECT log_comment, sleep_calls
FROM
(
    SELECT
        log_comment,
        sum(ProfileEvents['SleepFunctionCalls']) AS sleep_calls,
        max(event_time_microseconds) AS finished_at
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND log_comment LIKE '04759\_%'
    GROUP BY log_comment, initial_query_id
)
ORDER BY log_comment ASC, finished_at DESC
LIMIT 1 BY log_comment;

DROP TABLE pr_lazy_join;
DROP TABLE pr_lazy;
