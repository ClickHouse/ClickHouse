SET max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- `histogram` merges bins as values arrive, so with more distinct values than bins
-- the result depends on input order. `numbers` yields ascending values, so if the
-- subquery's `ORDER BY x DESC` were removed, the bins would change.
SELECT histogram(3)(x)
FROM (SELECT number + 1 AS x FROM numbers(10) ORDER BY x DESC);

SELECT countIf(explain LIKE '%Sorting%') AS sorting_steps
FROM (EXPLAIN actions = 0, compact = 0, pretty = 0 SELECT histogram(3)(x)
      FROM (SELECT number + 1 AS x FROM numbers(10) ORDER BY x DESC));
