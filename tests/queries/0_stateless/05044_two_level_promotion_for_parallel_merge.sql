-- Merge-time two-level promotion (`enable_two_level_promotion_for_parallel_merge`): when no
-- per-thread hash table crosses the two-level conversion thresholds during execution, the final
-- merge takes the single-level path and the multi-way keyed merge never engages. With the
-- promotion on, `prepareVariantsToMerge` converts all single-level variants to two-level (gate:
-- multi-way merge on, a parallelizable aggregate present, >= 3 non-empty variants, >= 1024 groups
-- summed across them), so the merge takes the per-bucket path where the multi-way waves run. The
-- promotion changes only the layout of the data being merged, never the per-key merge order:
-- results must be identical with the setting on and off.

SET max_threads = 4;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SET enable_multi_way_keyed_merge = 1;

-- Unlike 05043, the two-level conversion thresholds keep their defaults and are never reached:
-- ~2000 groups and modest state bytes per thread keep every per-thread table single-level, so only
-- the promotion (when on) routes the merge down the per-bucket path. The key is widened to UInt64
-- because the small fixed-key methods (key8/key16) have no two-level form to promote to. The
-- heavy key 42 collects a million distinct values, so each thread carries a two-level uniqExact
-- state for it and the multi-way wave has real work; every group is present in all 4 threads, so
-- each destination state collects 3 collided sources (the wave minimum).

SET enable_two_level_promotion_for_parallel_merge = 1;

SELECT g, uniqExact(x) AS u, count() AS c
FROM (SELECT toUInt64(if(number % 2 = 0, 42, intDiv(number, 2) % 2000)) AS g,
             if(number % 2 = 0, number, number % 100000) AS x
      FROM numbers_mt(2000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(c)
FROM
(
    SELECT g, uniqExact(x) AS u, count() AS c
    FROM (SELECT toUInt64(if(number % 2 = 0, 42, intDiv(number, 2) % 2000)) AS g,
                 if(number % 2 = 0, number, number % 100000) AS x
          FROM numbers_mt(2000000))
    GROUP BY g
);

SELECT g, uniqExact(x) AS u, sum(x) AS s, uniqExactIf(x, x % 2 = 0) AS ue
FROM (SELECT toUInt64(if(number % 2 = 0, 42, intDiv(number, 2) % 2000)) AS g,
             if(number % 2 = 0, number, number % 100000) AS x
      FROM numbers_mt(2000000))
GROUP BY g
ORDER BY g
LIMIT 3 OFFSET 41;

SET enable_two_level_promotion_for_parallel_merge = 0;

SELECT g, uniqExact(x) AS u, count() AS c
FROM (SELECT toUInt64(if(number % 2 = 0, 42, intDiv(number, 2) % 2000)) AS g,
             if(number % 2 = 0, number, number % 100000) AS x
      FROM numbers_mt(2000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(c)
FROM
(
    SELECT g, uniqExact(x) AS u, count() AS c
    FROM (SELECT toUInt64(if(number % 2 = 0, 42, intDiv(number, 2) % 2000)) AS g,
                 if(number % 2 = 0, number, number % 100000) AS x
          FROM numbers_mt(2000000))
    GROUP BY g
);

SELECT g, uniqExact(x) AS u, sum(x) AS s, uniqExactIf(x, x % 2 = 0) AS ue
FROM (SELECT toUInt64(if(number % 2 = 0, 42, intDiv(number, 2) % 2000)) AS g,
             if(number % 2 = 0, number, number % 100000) AS x
      FROM numbers_mt(2000000))
GROUP BY g
ORDER BY g
LIMIT 3 OFFSET 41;
