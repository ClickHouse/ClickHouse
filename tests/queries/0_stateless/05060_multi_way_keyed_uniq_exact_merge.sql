-- The multi-way keyed merge of the two-level aggregation final merge
-- (`enable_multi_way_keyed_merge`) only changes how each destination key's states are folded
-- together (one grouped wave instead of pairwise links, per-key merge order preserved), so the
-- result must be identical with the setting on and off.

SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_threads = 4;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SET enable_multi_way_keyed_merge = 1;
-- Pinned off explicitly (it defaults to on) so the test exercises exactly one variable: the
-- promotion is irrelevant here anyway, because group_by_two_level_threshold = 1 already makes
-- every per-thread table two-level during execution.
SET enable_two_level_promotion_for_parallel_merge = 0;

-- Skewed uniqExact states: group 42 collects a million distinct values (every even number), so
-- each merging thread carries a two-level state for it, while every other group stays tiny and
-- single-level. Every group is present in all 4 threads, so the final merge collects 3 collided
-- sources per destination state: the heavy key takes the multi-way wave, the tiny keys take the
-- wave's internal pairwise fallback.
SELECT g, uniqExact(x) AS u, count() AS c
FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
             if(number % 2 = 0, number, number % 1000) AS x
      FROM numbers(2000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(c)
FROM
(
    SELECT g, uniqExact(x) AS u, count() AS c
    FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
                 if(number % 2 = 0, number, number % 1000) AS x
          FROM numbers(2000000))
    GROUP BY g
);

-- Multiple aggregate functions dispatch independently: uniqExact takes the wave, sum and the
-- combinator-wrapped uniqExactIf (combinators do not forward the parallel-merge capability)
-- keep the pairwise path.
SELECT g, uniqExact(x) AS u, sum(x) AS s, uniqExactIf(x, x % 2 = 0) AS ue
FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
             if(number % 2 = 0, number, number % 1000) AS x
      FROM numbers(2000000))
GROUP BY g
ORDER BY g
LIMIT 3 OFFSET 41;

SET enable_multi_way_keyed_merge = 0;

SELECT count(), sum(u), sum(c)
FROM
(
    SELECT g, uniqExact(x) AS u, count() AS c
    FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
                 if(number % 2 = 0, number, number % 1000) AS x
          FROM numbers(2000000))
    GROUP BY g
);

SELECT g, uniqExact(x) AS u, sum(x) AS s, uniqExactIf(x, x % 2 = 0) AS ue
FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
             if(number % 2 = 0, number, number % 1000) AS x
      FROM numbers(2000000))
GROUP BY g
ORDER BY g
LIMIT 3 OFFSET 41;
