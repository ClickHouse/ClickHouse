-- A NOT NULL filter derived from a join condition is resolved into the filter step below the join.
-- When the conjunct that survives that rewrite names a column of the step's own output header, the
-- column must still reach the join: the join's expressions hold an input node for it.

-- One side carries the bare filter column.
SELECT is_active, name, label
FROM (SELECT 'alpha' AS name, toNullable('k1') AS join_key, 1 AS is_active) AS left_side
INNER JOIN (SELECT 'k1' AS join_key, 'beta' AS label) AS right_side
    ON left_side.join_key = right_side.join_key
WHERE is_active AND (label != name)
SETTINGS query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_optimize_join_order_limit = 10;

-- Same query without the join reorder, which reaches the broken plan at execution instead.
SELECT is_active, name, label
FROM (SELECT 'alpha' AS name, toNullable('k1') AS join_key, 1 AS is_active) AS left_side
INNER JOIN (SELECT 'k1' AS join_key, 'beta' AS label) AS right_side
    ON left_side.join_key = right_side.join_key
WHERE is_active AND (label != name)
SETTINGS query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_optimize_join_order_limit = 0;

-- Both sides carry a column of that name, so both lose one.
SELECT left_value, right_value
FROM (SELECT toNullable('k1') AS join_key, 1 AS is_active, 'left' AS left_value) AS left_side
NATURAL INNER JOIN (SELECT 'k1' AS join_key, 1 AS is_active, 'right' AS right_value) AS right_side
WHERE is_active AND (left_value != right_value)
SETTINGS query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_optimize_join_order_limit = 10;

SELECT left_value, right_value
FROM (SELECT toNullable('k1') AS join_key, 1 AS is_active, 'left' AS left_value) AS left_side
NATURAL INNER JOIN (SELECT 'k1' AS join_key, 1 AS is_active, 'right' AS right_value) AS right_side
WHERE is_active AND (left_value != right_value)
SETTINGS query_plan_filter_push_down = 1, query_plan_merge_filters = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_optimize_join_order_limit = 0;
