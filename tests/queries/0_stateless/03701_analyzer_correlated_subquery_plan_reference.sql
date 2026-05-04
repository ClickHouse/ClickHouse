SET enable_analyzer = 1;
SET query_plan_join_swap_table = false;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_default_join_kind = 'left';
SET correlated_subqueries_use_in_memory_buffer = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1; -- CI may inject False, keeping LEFT ANY instead of converting to LEFT SEMI
SET query_plan_remove_unused_columns = 1; -- CI may inject False, keeping __table1.y/exists(__table2) in plan instead of pruning to __join_result_dummy

CREATE TABLE t(x Int, y Int) ORDER BY ()
AS SELECT number as x, number % 2 as y FROM numbers(100);

EXPLAIN actions = 1
SELECT
    count()
FROM
    t n
WHERE
  EXISTS (
    SELECT
        *
    FROM
        numbers(10)
    WHERE
        number != n.x
  )
  AND n.y = 1;
