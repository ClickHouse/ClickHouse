DROP TABLE IF EXISTS pr_1;
DROP TABLE IF EXISTS pr_2;

CREATE TABLE pr_1 (`a` UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 10 AS
SELECT 10 * intDiv(number, 10) + 1 FROM numbers(1_000_000);

CREATE TABLE pr_2 (`a` UInt32) ENGINE = MergeTree ORDER BY a AS
SELECT * FROM numbers(1_000_000);

WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 10000)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a;

WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 10000)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_two_shards', max_parallel_replicas = 3;

-- Testing that it is disabled for allow_experimental_analyzer=0. With analyzer it will be supported (with correct result)
WITH filtered_groups AS (SELECT a FROM pr_1 WHERE a >= 10000)
SELECT count() FROM pr_2 INNER JOIN filtered_groups ON pr_2.a = filtered_groups.a
SETTINGS allow_experimental_analyzer = 0, allow_experimental_parallel_reading_from_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_two_shards', max_parallel_replicas = 3; -- { serverError SUPPORT_IS_DISABLED }

-- Sanitizer
SELECT count() FROM pr_2 JOIN numbers(10) as pr_1 ON pr_2.a = pr_1.number
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_two_shards', max_parallel_replicas = 3;

DROP TABLE IF EXISTS pr_1;
DROP TABLE IF EXISTS pr_2;
