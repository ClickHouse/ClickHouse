-- A `ConstantJoin` right side that carries a lazily replicated column used to read a freed
-- `ColumnReplicated` while materializing the selected right row.
-- `enable_lazy_columns_replication` is pinned in every query because the test runner randomizes it.
-- Join swapping and join-order optimization are pinned too: reversing `LEFT SEMI` into `RIGHT SEMI`
-- would move the query off the selected-right-row path that materializes the replicated column.

SELECT 'witness: reporter repro, SEMI LEFT, Nullable(UInt8) right column';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT toUInt8(moduloOrNull(number, 10)) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON not(materialize(1))
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'witness: non-Nullable right column, not fixed and contiguous';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT toString(number) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON not(materialize(1))
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'witness: runtime-constant ON that evaluates to true';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT toUInt8(moduloOrNull(number, 10)) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON materialize(1)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'witness: ANY INNER with any_join_distinct_right_table_keys';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
ANY INNER JOIN (SELECT toUInt8(moduloOrNull(number, 10)) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON not(materialize(1))
SETTINGS enable_lazy_columns_replication = 1, any_join_distinct_right_table_keys = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'witness: fixed right column wider than 8 bytes';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT toDecimal128(number, 2) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON not(materialize(1))
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'control: parse-time constant ON returns early before the replicated right row is materialized';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT toUInt8(moduloOrNull(number, 10)) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON 1 = 2
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'control: lazy replication disabled';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT toUInt8(moduloOrNull(number, 10)) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON not(materialize(1))
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'control: fixed 8-byte right column is not lazily replicated';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
SEMI LEFT JOIN (SELECT number AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
ON not(materialize(1))
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;

SELECT 'control: CROSS JOIN stores the right rows instead of selecting one';
SELECT l, r
FROM (SELECT 1::UInt8 AS l) AS t1
CROSS JOIN (SELECT toUInt8(moduloOrNull(number, 10)) AS r FROM numbers(1) ARRAY JOIN [1]) AS t2
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 0;
