-- `histogram` merges bins while values arrive: once a single aggregation state has
-- received more than `2 * number_of_bins` values it compresses mid-stream, so the
-- result depends on the order in which the values are processed. Therefore
-- `query_plan_remove_redundant_sorting` must keep an `ORDER BY` that feeds `histogram`.

-- The optimizer must not change the result. The left side needs the subquery's
-- `ORDER BY x DESC` to survive; the right side delivers the same values in descending
-- order without an `ORDER BY` for the optimizer to remove.
SELECT
    (SELECT histogram(3)(x) FROM (SELECT number + 1 AS x FROM numbers(10) ORDER BY x DESC))
  = (SELECT histogram(3)(x) FROM (SELECT 10 - number AS x FROM numbers(10)))
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- The property is resolved by stripping combinator suffixes, so it must hold for `histogramIf` too.
SELECT
    (SELECT histogramIf(3)(x, x > 0) FROM (SELECT number + 1 AS x FROM numbers(10) ORDER BY x DESC))
  = (SELECT histogramIf(3)(x, x > 0) FROM (SELECT 10 - number AS x FROM numbers(10)))
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- The bins the preserved `ORDER BY` produces. Without the fix these are the bins of the
-- ascending input, `[(1,4,3.75),(4,7,2.5),(7,10,3.75)]`.
SELECT histogram(3)(x) FROM (SELECT number + 1 AS x FROM numbers(10) ORDER BY x DESC)
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- The sort must still be present in the plan.
SELECT countIf(explain LIKE '%Sorting%') AS sorting_steps
FROM (EXPLAIN actions = 0, compact = 0, pretty = 0
      SELECT histogram(3)(x) FROM (SELECT number + 1 AS x FROM numbers(10) ORDER BY x DESC)
      SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1);
