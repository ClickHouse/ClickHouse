create table t (number UInt64) engine MergeTree order by number;
-- The table has to hold data, and parallel replicas has to be worth it for the number of rows, or
-- the read is served locally and the cluster name is never resolved - the error expected below is
-- what the subquery traversal reaches once parallel replicas is actually planned.
insert into t select number from numbers(10);

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
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas='not_exists', max_parallel_replicas = 2, enable_analyzer = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0; -- { serverError CLUSTER_DOESNT_EXIST }
