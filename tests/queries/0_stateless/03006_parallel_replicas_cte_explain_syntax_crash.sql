DROP TABLE IF EXISTS numbers_1e6__fuzz_34;
DROP TABLE IF EXISTS numbers_1e6__fuzz_33;

CREATE TABLE numbers_1e6__fuzz_34
(
    `n` LowCardinality(Nullable(UInt8))
)
ENGINE = MergeTree
ORDER BY n
SETTINGS allow_nullable_key = 1
AS SELECT *
FROM numbers(1000000)
SETTINGS allow_suspicious_low_cardinality_types = 1;


CREATE TABLE numbers_1e6__fuzz_33
(
    `n` LowCardinality(Nullable(UInt8))
)
ENGINE = MergeTree
ORDER BY n
SETTINGS allow_nullable_key = 1
AS SELECT *
FROM numbers(1000000)
SETTINGS allow_suspicious_low_cardinality_types = 1;

SET allow_experimental_analyzer = 0;
SET allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
EXPLAIN SYNTAX
WITH
    cte1 AS
    (
        SELECT n
        FROM numbers_1e6__fuzz_34
    ),
    cte2 AS
    (
        SELECT n
        FROM numbers_1e6__fuzz_33
        PREWHERE n IN (cte1)
    )
SELECT count()
FROM cte2;
-- SETTINGS allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
