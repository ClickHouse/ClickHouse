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
-- Pinned to query-based: only it walks the query tree with dummy tables, which this test covers.
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas='not_exists', max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 0; -- { serverError CLUSTER_DOESNT_EXIST }
