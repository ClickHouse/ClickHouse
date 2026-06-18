DROP TABLE IF EXISTS pr_1;
DROP TABLE IF EXISTS pr_2;
DROP TABLE IF EXISTS numbers_1e6;

CREATE TABLE pr_1 (`a` UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 10 AS
SELECT 10 * intDiv(number, 10) + 1 FROM numbers(1_000);

CREATE TABLE pr_2 (`a` UInt32) ENGINE = MergeTree ORDER BY a AS
SELECT * FROM numbers(1_000);

WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 100)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a;

WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 100)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

-- Testing that it is disabled for enable_analyzer=0. With analyzer it will be supported (with correct result)
WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 100)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer=0,
enable_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3; -- { serverError SUPPORT_IS_DISABLED }

-- Disabled for any value of enable_parallel_replicas != 1, not just 2
WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 100)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
SETTINGS enable_analyzer = 0, parallel_replicas_only_with_analyzer=0,
enable_parallel_replicas = 512, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3; -- { serverError SUPPORT_IS_DISABLED }

-- Sanitizer
SELECT count() FROM pr_2 JOIN numbers(10) as pr_1 ON pr_2.a = pr_1.number
SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

-- Parallel replicas detection should work inside subqueries
SELECT *
FROM
(
    WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 100)
    SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

-- Subquery + subquery
SELECT count()
FROM
(
    SELECT c + 1
    FROM
    (
        WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 100)
        SELECT count() as c FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
    )
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

CREATE TABLE numbers_1e3
(
    `n` UInt64
)
ENGINE = MergeTree
ORDER BY n
AS SELECT * FROM numbers(1_000);

-- Same but nested CTE's
WITH
    cte1 AS
    (
        SELECT n
        FROM numbers_1e3
    ),
    cte2 AS
    (
        SELECT n
        FROM numbers_1e3
        WHERE n IN (cte1)
    )
SELECT count()
FROM cte2
SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

DROP TABLE IF EXISTS numbers_1e6;
DROP TABLE IF EXISTS pr_1;
DROP TABLE IF EXISTS pr_2;
