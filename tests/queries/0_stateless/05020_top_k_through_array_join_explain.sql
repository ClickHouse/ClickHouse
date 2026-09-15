DROP TABLE IF EXISTS t_aj_explain;

CREATE TABLE t_aj_explain (pk UInt64, x UInt64, payload String, arr Array(UInt32), arr2 Array(UInt32))
ENGINE = MergeTree ORDER BY pk;

INSERT INTO t_aj_explain
SELECT number, sipHash64(number), toString(number), range(number % 5), range(number % 3)
FROM numbers(100);

SET query_plan_read_in_order = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET query_plan_optimize_prewhere = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_push_down_limit_through_array_join = 0;
SET query_plan_max_limit_for_top_k_optimization = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;

SELECT '-- disabled: Sorting remains above ArrayJoin';
EXPLAIN PLAN description = 0
SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10
SETTINGS query_plan_top_k_through_array_join = 0;

SELECT '-- enabled: Sorting and guard move below ArrayJoin';
EXPLAIN PLAN description = 0
SELECT payload FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10
SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- LEFT ARRAY JOIN: Sorting moves without a guard';
EXPLAIN PLAN description = 0
SELECT x FROM t_aj_explain LEFT ARRAY JOIN arr ORDER BY x LIMIT 10
SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- guard spans every joined column';
SELECT trim(replaceRegexpOne(explain, '^.*Filter column: ', ''))
FROM
(
    EXPLAIN PLAN
    SELECT x, arr, arr2 FROM t_aj_explain ARRAY JOIN arr, arr2 ORDER BY x LIMIT 10
    SETTINGS enable_unaligned_array_join = 1, query_plan_top_k_through_array_join = 1
)
WHERE explain LIKE '%Filter column:%';

SELECT '-- chained ARRAY JOIN: one sort moves below both steps and gets one guard per step';
EXPLAIN PLAN description = 0
SELECT x FROM t_aj_explain ARRAY JOIN arr ARRAY JOIN arr2 ORDER BY x LIMIT 10
SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- joined sort key: not rewritten';
EXPLAIN PLAN description = 0
SELECT arr FROM t_aj_explain ARRAY JOIN arr ORDER BY arr LIMIT 10
SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- LIMIT WITH TIES: not rewritten';
EXPLAIN PLAN description = 0
SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10 WITH TIES
SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- exact rows before limit: not rewritten';
EXPLAIN PLAN description = 0
SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10
SETTINGS exact_rows_before_limit = 1, query_plan_top_k_through_array_join = 1;

SELECT '-- LIMIT BY blocks the rewrite';
EXPLAIN PLAN description = 0
SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 1 BY x LIMIT 10
SETTINGS query_plan_top_k_through_array_join = 1;

SELECT '-- serialized moved plan';
SELECT count() FROM (SELECT x FROM t_aj_explain ARRAY JOIN arr ORDER BY x LIMIT 10)
SETTINGS serialize_query_plan = 1, query_plan_top_k_through_array_join = 1;

DROP TABLE t_aj_explain;
