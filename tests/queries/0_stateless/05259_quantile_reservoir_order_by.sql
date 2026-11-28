-- Checks that the `query_plan_remove_redundant_sorting` optimization keeps an inner `ORDER BY` that
-- feeds `quantile`, because it samples the values as they arrive and so returns a different result
-- for a different input order. Each query compares the sorted-subquery form against a generator
-- emitting the same values in the same order with no `ORDER BY` to remove, so it asserts equality of
-- two results rather than any particular quantile value.
-- One quantile per query on purpose: the optimization gives up on the whole aggregation step as soon
-- as one of its functions is order dependent, so a shared query would hide the other assertions.

SELECT
    (SELECT quantile(0.5)(x) FROM (SELECT toFloat64(number) * toFloat64(number) AS x FROM numbers(100000) ORDER BY x DESC))
  = (SELECT quantile(0.5)(x) FROM (SELECT toFloat64(99999 - number) * toFloat64(99999 - number) AS x FROM numbers(100000)))
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

SELECT
    (SELECT quantiles(0.5)(x) FROM (SELECT toFloat64(number) * toFloat64(number) AS x FROM numbers(100000) ORDER BY x DESC))
  = (SELECT quantiles(0.5)(x) FROM (SELECT toFloat64(99999 - number) * toFloat64(99999 - number) AS x FROM numbers(100000)))
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- a combinator suffix must inherit the behaviour
SELECT
    (SELECT quantileIf(0.5)(x, x > 0) FROM (SELECT toFloat64(number) * toFloat64(number) AS x FROM numbers(100000) ORDER BY x DESC))
  = (SELECT quantileIf(0.5)(x, x > 0) FROM (SELECT toFloat64(99999 - number) * toFloat64(99999 - number) AS x FROM numbers(100000)))
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- and so must an alias
SELECT
    (SELECT median(x) FROM (SELECT toFloat64(number) * toFloat64(number) AS x FROM numbers(100000) ORDER BY x DESC))
  = (SELECT median(x) FROM (SELECT toFloat64(99999 - number) * toFloat64(99999 - number) AS x FROM numbers(100000)))
SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1;

-- the sort that feeds the quantile must survive in the plan
SELECT countIf(explain LIKE '%Sorting%') FROM (EXPLAIN actions = 0, compact = 0, pretty = 0
  SELECT quantile(0.5)(x) FROM (SELECT toFloat64(number) AS x FROM numbers(100000) ORDER BY x DESC)
  SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1);

-- while a redundant sort feeding a sum of integers is still removed
SELECT countIf(explain LIKE '%Sorting%') FROM (EXPLAIN actions = 0, compact = 0, pretty = 0
  SELECT sum(number) FROM (SELECT number FROM numbers(100000) ORDER BY number DESC)
  SETTINGS max_threads = 1, query_plan_remove_redundant_sorting = 1);
