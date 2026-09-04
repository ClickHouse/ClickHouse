-- Merge-time two-level promotion (`enable_two_level_promotion_for_parallel_merge`): when no
-- per-thread hash table crosses the two-level conversion thresholds during execution, the final
-- merge takes the single-level path and the multi-way keyed merge never engages. With the
-- promotion on, `prepareVariantsToMerge` converts all single-level variants to two-level so the
-- merge takes the per-bucket path where the multi-way waves run.
--
-- Gate retry: the promotion gate now also requires the parallelizable aggregate's per-key states
-- to be heavy on average (mean single-level uniqExact set size above a floor), so light-state
-- COUNT(DISTINCT) queries are left on the unchanged path. This test therefore uses a uniform
-- heavy-state shape - every group accumulates ~2000 distinct values, so the sampled mean is far
-- above the floor - standing in for the Q10/Q11 class that the promotion is meant to help. The
-- promotion changes only the layout of the data being merged, never the per-key merge order:
-- results must be identical with the setting on and off.

SET max_threads = 4;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
-- Disable the automatic two-level conversion (both thresholds off) so every per-thread table
-- stays single-level during execution: the promotion is then the only route to the per-bucket
-- merge path, making the on/off contrast deterministic (mirror of 05060 forcing two-level with
-- group_by_two_level_threshold = 1).
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;

SET enable_multi_way_keyed_merge = 1;

-- 2000 groups, each accumulating ~2000 distinct values from 4M rows over numbers_mt (plain
-- numbers() aggregates single-stream and leaves nothing to merge). The group count (2000) and the
-- per-set size (~2000) both stay below the two-level conversion thresholds, so every per-thread
-- table stays single-level and only the promotion routes the merge onto the per-bucket path.

SET enable_two_level_promotion_for_parallel_merge = 1;

SELECT g, uniqExact(x) AS u, count() AS c
FROM (SELECT toUInt64(number % 2000) AS g, number AS x FROM numbers_mt(4000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(c)
FROM
(
    SELECT g, uniqExact(x) AS u, count() AS c
    FROM (SELECT toUInt64(number % 2000) AS g, number AS x FROM numbers_mt(4000000))
    GROUP BY g
);

SELECT g, uniqExact(x) AS u, sum(x) AS s, uniqExactIf(x, x % 2 = 0) AS ue
FROM (SELECT toUInt64(number % 2000) AS g, number AS x FROM numbers_mt(4000000))
GROUP BY g
ORDER BY g
LIMIT 3 OFFSET 41;

SET enable_two_level_promotion_for_parallel_merge = 0;

SELECT g, uniqExact(x) AS u, count() AS c
FROM (SELECT toUInt64(number % 2000) AS g, number AS x FROM numbers_mt(4000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(c)
FROM
(
    SELECT g, uniqExact(x) AS u, count() AS c
    FROM (SELECT toUInt64(number % 2000) AS g, number AS x FROM numbers_mt(4000000))
    GROUP BY g
);

SELECT g, uniqExact(x) AS u, sum(x) AS s, uniqExactIf(x, x % 2 = 0) AS ue
FROM (SELECT toUInt64(number % 2000) AS g, number AS x FROM numbers_mt(4000000))
GROUP BY g
ORDER BY g
LIMIT 3 OFFSET 41;
