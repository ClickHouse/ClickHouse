-- Tags: no-parallel-replicas
-- Coverage gap for PR #99228 (Replace Block to Chunk in Aggregator).
-- Verifies GROUPING SETS correctness when external aggregation and two-level
-- threshold are both triggered (MergingAggregatedTransform::addChunk,
-- mergeBlocks(BucketToChunks), convertBlockToTwoLevel paths).

-- Compare GROUPING SETS with/without external aggregation
SELECT 'gs_external';
SELECT key1, key2, sum(val) AS s, count() AS c
FROM (SELECT number % 3 AS key1, number % 5 AS key2, number AS val FROM numbers(1000))
GROUP BY GROUPING SETS ((key1), (key2))
ORDER BY key1, key2, s, c
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1;

SELECT 'gs_normal';
SELECT key1, key2, sum(val) AS s, count() AS c
FROM (SELECT number % 3 AS key1, number % 5 AS key2, number AS val FROM numbers(1000))
GROUP BY GROUPING SETS ((key1), (key2))
ORDER BY key1, key2, s, c;

-- Single grouping set with reordering (exercises single-set path in addChunk)
SELECT 'gs_single_external';
SELECT key1, sum(val) AS s
FROM (SELECT number % 7 AS key1, number AS val FROM numbers(500))
GROUP BY GROUPING SETS ((key1))
ORDER BY key1
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1;

SELECT 'gs_single_normal';
SELECT key1, sum(val) AS s
FROM (SELECT number % 7 AS key1, number AS val FROM numbers(500))
GROUP BY GROUPING SETS ((key1))
ORDER BY key1;
