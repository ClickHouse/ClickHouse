SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_default_join_kind = 'left';
SET correlated_subqueries_use_in_memory_buffer = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

-- Disable table swaps during query planning
SET query_plan_join_swap_table = false;
SET execute_exists_as_scalar_subquery = 0; -- test is about correlated EXISTS join planning; scalar rewrite changes INNER JOIN → CROSS JOIN + Filter
SET query_plan_remove_unused_columns = 1; -- unused join result columns leak into actions/positions when disabled
SET query_plan_optimize_join_order_limit = 10; -- CI may inject 0, skipping chooseJoinOrder which normally eliminates intermediate "Project only used columns" Expression steps
SET query_plan_convert_any_join_to_semi_or_anti_join = 1; -- CI may inject False; LEFT ANY stays as LEFT ANY instead of converting to LEFT SEMI, changing plan structure and WHERE fusion

DROP TABLE IF EXISTS test;
CREATE TABLE test(
    i1 Int64,
    i2 Int64,
    i3 Int64,
    i4 Int64,
    i5 Int64,
    i6 Int64,
    i7 Int64,
    i8 Int64,
    i9 Int64,
    i10 Int64
)
ENGINE = MergeTree()
ORDER BY ();

INSERT INTO test VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

SET correlated_subqueries_substitute_equivalent_expressions = 0;

EXPLAIN actions = 1
SELECT 1 FROM test AS t1
WHERE EXISTS (
    SELECT * FROM test AS t2
    WHERE t1.i1 = t2.i2
)
SETTINGS enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 1; -- CI may inject False; correlated subquery WHERE condition not pushed into inner JOIN clause → stays CROSS with Filter above instead of INNER, losing the ' + ' label suffix

SELECT 1 FROM test AS t1
WHERE EXISTS (
    SELECT * FROM test AS t2
    WHERE t1.i1 = t2.i2
);
