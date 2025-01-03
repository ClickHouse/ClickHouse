create table t (number UInt64) engine MergeTree order by number;

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
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 2, allow_experimental_analyzer = 1; -- { serverError CLUSTER_DOESNT_EXIST }
