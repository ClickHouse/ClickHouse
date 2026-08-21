-- Plan shape produced by `query_plan_top_k_through_array_join`. The rewrite grafts
-- `Limit -> Sorting -> [Filter]` onto the input of the `ARRAY JOIN`, so the plan gains one
-- `Sorting`, one `Limit` and (for an inner `ARRAY JOIN`) one guard `Filter`.
--
-- The assertions count steps instead of dumping the whole plan, because many unrelated settings
-- that the test runner randomizes (lazy materialization, TopK dynamic filtering, read-in-order
-- buffering) change how the plan is rendered without changing these counts.

DROP TABLE IF EXISTS t_aj_explain;

CREATE TABLE t_aj_explain (ts UInt64, x UInt64, arr Array(UInt32), arr2 Array(UInt32))
ENGINE = MergeTree ORDER BY ts;

INSERT INTO t_aj_explain SELECT number, number, range(number % 5), range(number % 3) FROM numbers(100);

-- Pin everything the rewrite consults, so the decisions below do not depend on randomization.
SET query_plan_read_in_order = 1;
SET query_plan_max_limit_for_top_k_optimization = 0;

SELECT '-- sortings limits guards';

SELECT '-- inner ARRAY JOIN, optimization off';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 0);

SELECT '-- inner ARRAY JOIN, optimization on';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- LEFT ARRAY JOIN needs no guard';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain LEFT ARRAY JOIN arr ORDER BY x LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- ORDER BY a joined column: not rewritten';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT arr FROM t_aj_explain ARRAY JOIN arr ORDER BY arr LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- LIMIT WITH TIES: not rewritten';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10 WITH TIES
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- exact_rows_before_limit: not rewritten';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10
      SETTINGS exact_rows_before_limit = 1, query_plan_top_k_through_array_join = 1);

SELECT '-- no ORDER BY: not rewritten by this optimization';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- ORDER BY the primary key: deferred to read-in-order, not rewritten';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY ts LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- ORDER BY the primary key with read-in-order disabled: rewritten';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY ts LIMIT 10
      SETTINGS query_plan_read_in_order = 0, query_plan_top_k_through_array_join = 1);

SELECT '-- the guard spans every joined column';
SELECT trim(replaceRegexpOne(explain, '^.*Filter column: ', ''))
FROM (EXPLAIN PLAN SELECT x, arr, arr2 FROM t_aj_explain ARRAY JOIN arr, arr2 ORDER BY x LIMIT 10
      SETTINGS enable_unaligned_array_join = 1, query_plan_top_k_through_array_join = 1)
WHERE explain LIKE '%Filter column:%';

SELECT '-- the rewrite is applied at most once';
SELECT countIf(explain LIKE '%Sorting%'), countIf(explain LIKE '%Limit%'), countIf(explain LIKE '%Non-empty arrays for ARRAY JOIN%')
FROM (EXPLAIN PLAN SELECT x FROM t_aj_explain ARRAY JOIN arr ARRAY JOIN arr2 ORDER BY x LIMIT 10
      SETTINGS query_plan_top_k_through_array_join = 1);

SELECT '-- the rewritten plan survives serialization';
SELECT count() FROM (SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10)
SETTINGS serialize_query_plan = 1, query_plan_top_k_through_array_join = 1;

DROP TABLE t_aj_explain;
