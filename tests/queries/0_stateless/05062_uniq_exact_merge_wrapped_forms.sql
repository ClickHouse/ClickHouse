-- Differential coverage for wrapped forms of uniqExact under the multi-way keyed merge
-- (`enable_multi_way_keyed_merge`) and the merge-time two-level promotion
-- (`enable_two_level_promotion_for_parallel_merge`). Combinator and outer-type wrappers do not
-- forward the parallel-merge capability virtuals, so `uniqExactIf` must take the pairwise
-- fallback of the grouped dispatch, while plain `uniqExact`, `uniqExact` over a Nullable
-- argument (the -Null wrapper skips NULLs but the inner function still parallelizes when the
-- factory wires it so) and `uniqExact` over a Tuple argument (generic serialized set) ride the
-- same merge. Whatever path each form takes, the merge only regroups identical pairwise merges:
-- results must be byte-identical with the settings on and off. Each SELECT below is executed
-- with the settings on and then off; the reference asserts the outputs are identical.

SET max_threads = 4;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

-- Shape 1: forced two-level tables (mirror of 05060): the final merge takes the per-bucket
-- path, where the multi-way keyed merge dispatches per destination key.
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

SET enable_multi_way_keyed_merge = 1;
SET enable_two_level_promotion_for_parallel_merge = 1;

SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
             if(number % 2 = 0, number, number % 1000) AS x,
             if(number % 11 = 0, NULL, if(number % 2 = 0, number, number % 1000)) AS xn,
             number % 7 AS r
      FROM numbers(1000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(ui), sum(un), sum(ut)
FROM
(
    SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
    FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
                 if(number % 2 = 0, number, number % 1000) AS x,
                 if(number % 11 = 0, NULL, if(number % 2 = 0, number, number % 1000)) AS xn,
                 number % 7 AS r
          FROM numbers(1000000))
    GROUP BY g
);

SET enable_multi_way_keyed_merge = 0;
SET enable_two_level_promotion_for_parallel_merge = 0;

SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
             if(number % 2 = 0, number, number % 1000) AS x,
             if(number % 11 = 0, NULL, if(number % 2 = 0, number, number % 1000)) AS xn,
             number % 7 AS r
      FROM numbers(1000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(ui), sum(un), sum(ut)
FROM
(
    SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
    FROM (SELECT if(number % 2 = 0, 42, intDiv(number, 2) % 100) AS g,
                 if(number % 2 = 0, number, number % 1000) AS x,
                 if(number % 11 = 0, NULL, if(number % 2 = 0, number, number % 1000)) AS xn,
                 number % 7 AS r
          FROM numbers(1000000))
    GROUP BY g
);

-- Shape 2: single-level tables + merge-time promotion (mirror of 05061): both two-level
-- conversion thresholds off, so every per-thread table stays single-level during execution and
-- only the promotion routes the merge onto the per-bucket path. numbers_mt, because plain
-- numbers() aggregates single-stream and leaves nothing to merge. ~2000 distinct values per
-- group keep the state-weight component of the promotion gate satisfied.
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;

SET enable_multi_way_keyed_merge = 1;
SET enable_two_level_promotion_for_parallel_merge = 1;

SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
FROM (SELECT toUInt64(number % 2000) AS g, number AS x,
             if(number % 11 = 0, NULL, number) AS xn, number % 7 AS r
      FROM numbers_mt(4000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(ui), sum(un), sum(ut)
FROM
(
    SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
    FROM (SELECT toUInt64(number % 2000) AS g, number AS x,
                 if(number % 11 = 0, NULL, number) AS xn, number % 7 AS r
          FROM numbers_mt(4000000))
    GROUP BY g
);

SET enable_multi_way_keyed_merge = 0;
SET enable_two_level_promotion_for_parallel_merge = 0;

SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
FROM (SELECT toUInt64(number % 2000) AS g, number AS x,
             if(number % 11 = 0, NULL, number) AS xn, number % 7 AS r
      FROM numbers_mt(4000000))
GROUP BY g
ORDER BY g
LIMIT 5 OFFSET 40;

SELECT count(), sum(u), sum(ui), sum(un), sum(ut)
FROM
(
    SELECT g, uniqExact(x) AS u, uniqExactIf(x, x % 3 = 0) AS ui, uniqExact(xn) AS un, uniqExact((x, r)) AS ut
    FROM (SELECT toUInt64(number % 2000) AS g, number AS x,
                 if(number % 11 = 0, NULL, number) AS xn, number % 7 AS r
          FROM numbers_mt(4000000))
    GROUP BY g
);
