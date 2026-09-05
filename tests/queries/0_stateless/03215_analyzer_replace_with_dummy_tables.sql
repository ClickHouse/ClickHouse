create table t (number UInt64) engine MergeTree order by number;

SET automatic_parallel_replicas_mode = 0;
SELECT 1
FROM
(
    SELECT number IN (
            SELECT number
            FROM view(
                SELECT number
                FROM numbers(1)
            )
        )
    FROM t
)
-- `parallel_replicas_plan_based = 0`: the plan-based implementation does not distribute a plan that
-- builds an `IN` set, so it never resolves `cluster_for_parallel_replicas` and the query succeeds
-- instead of failing. See https://github.com/ClickHouse/ClickHouse/issues/118264.
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas='not_exists', max_parallel_replicas = 2, enable_analyzer = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 0; -- { serverError CLUSTER_DOESNT_EXIST }
