-- Tags: no-parallel-replicas

-- A NOT NULL filter derived from a null-rejecting join condition is a planner-internal marker that
-- `resolvePlannerOnlyFilters` resolves later, so it has to stay a conjunct of the filter step that owns it.
-- `query_plan_lift_up_array_join` moved such a marker below an `ARRAY JOIN` by data dependency alone, and
-- the resolver then hit `!new_actions_dag.hasPlannerOnlyFilters()` (an abort in debug and sanitizer builds).
-- Every session setting `clickhouse-test` randomizes (188 when this test was written) was measured one at a
-- time against a binary without the fix; the three that suppress the abort are pinned in every query below,
-- so CI randomization cannot decouple the test from the mechanism it exercises.
-- The oracle is that the query runs at all and returns the right rows; the three controls tie it to the
-- mechanism, and the last pair is a liveness canary: `EXPLAIN header = 1` shows the derived `isNotNull`
-- computed below the `ARRAY JOIN` only while the lift-up still reaches this shape.

DROP TABLE IF EXISTS o_05218;
DROP TABLE IF EXISTS i_05218;
DROP TABLE IF EXISTS o_notnull_05218;
DROP TABLE IF EXISTS i_notnull_05218;

CREATE TABLE o_05218 (id UInt32, c0 Nullable(Int32)) ENGINE = Memory;
CREATE TABLE i_05218 (c0 Nullable(Int32)) ENGINE = Memory;
INSERT INTO o_05218 VALUES (1, 10), (2, 20);
INSERT INTO i_05218 VALUES (10);

-- Non-nullable copies: a join condition on them is not null-rejecting, so no marker is ever derived.
CREATE TABLE o_notnull_05218 (id UInt32, c0 Int32) ENGINE = Memory;
CREATE TABLE i_notnull_05218 (c0 Int32) ENGINE = Memory;
INSERT INTO o_notnull_05218 VALUES (1, 10), (2, 20);
INSERT INTO i_notnull_05218 VALUES (10);

SELECT '-- ARRAY JOIN inside a decorrelated subquery --';
SELECT id FROM o_05218
WHERE NOT EXISTS (SELECT 1 FROM i_05218 ARRAY JOIN [1] WHERE i_05218.c0 = o_05218.c0)
ORDER BY id
SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
         correlated_subqueries_substitute_equivalent_expressions = 0,
         query_plan_lift_up_array_join = 1, query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
         query_plan_merge_expressions = 1, query_plan_filter_push_down = 1;

SELECT '-- LEFT ARRAY JOIN inside a decorrelated subquery --';
SELECT id FROM o_05218
WHERE NOT EXISTS (SELECT 1 FROM i_05218 LEFT ARRAY JOIN [1] WHERE i_05218.c0 = o_05218.c0)
ORDER BY id
SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
         correlated_subqueries_substitute_equivalent_expressions = 0,
         query_plan_lift_up_array_join = 1, query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
         query_plan_merge_expressions = 1, query_plan_filter_push_down = 1;

SELECT '-- control: same rows with the lift-up disabled (the marker was never moved) --';
SELECT id FROM o_05218
WHERE NOT EXISTS (SELECT 1 FROM i_05218 ARRAY JOIN [1] WHERE i_05218.c0 = o_05218.c0)
ORDER BY id
SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
         correlated_subqueries_substitute_equivalent_expressions = 0,
         query_plan_lift_up_array_join = 0, query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
         query_plan_merge_expressions = 1, query_plan_filter_push_down = 1;

SELECT '-- control: same rows with the derivation disabled (no marker exists) --';
SELECT id FROM o_05218
WHERE NOT EXISTS (SELECT 1 FROM i_05218 ARRAY JOIN [1] WHERE i_05218.c0 = o_05218.c0)
ORDER BY id
SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
         correlated_subqueries_substitute_equivalent_expressions = 0,
         query_plan_lift_up_array_join = 1, query_plan_derive_not_null_filters_from_joins = 0,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
         query_plan_merge_expressions = 1, query_plan_filter_push_down = 1;

SELECT '-- control: non-nullable keys derive no NOT NULL filter --';
SELECT id FROM o_notnull_05218
WHERE NOT EXISTS (SELECT 1 FROM i_notnull_05218 ARRAY JOIN [1] WHERE i_notnull_05218.c0 = o_notnull_05218.c0)
ORDER BY id
SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
         correlated_subqueries_substitute_equivalent_expressions = 0,
         query_plan_lift_up_array_join = 1, query_plan_derive_not_null_filters_from_joins = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
         query_plan_merge_expressions = 1, query_plan_filter_push_down = 1;

SELECT '-- liveness: the lift-up computes the derived isNotNull below the ARRAY JOIN --';
SELECT countIf(explain ILIKE '%isNotNull(%') > 0
FROM (
    EXPLAIN header = 1
    SELECT id FROM o_05218
    WHERE NOT EXISTS (SELECT 1 FROM i_05218 ARRAY JOIN [1] WHERE i_05218.c0 = o_05218.c0)
    ORDER BY id
    SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
             correlated_subqueries_substitute_equivalent_expressions = 0,
             query_plan_lift_up_array_join = 1, query_plan_derive_not_null_filters_from_joins = 1,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
             query_plan_merge_expressions = 1, query_plan_filter_push_down = 1,
             serialize_query_plan = 0
);

SELECT '-- liveness control: with the lift-up disabled it is not computed there --';
SELECT countIf(explain ILIKE '%isNotNull(%') > 0
FROM (
    EXPLAIN header = 1
    SELECT id FROM o_05218
    WHERE NOT EXISTS (SELECT 1 FROM i_05218 ARRAY JOIN [1] WHERE i_05218.c0 = o_05218.c0)
    ORDER BY id
    SETTINGS enable_analyzer = 1, allow_experimental_correlated_subqueries = 1,
             correlated_subqueries_substitute_equivalent_expressions = 0,
             query_plan_lift_up_array_join = 0, query_plan_derive_not_null_filters_from_joins = 1,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 1, query_plan_merge_filters = 1,
             query_plan_merge_expressions = 1, query_plan_filter_push_down = 1,
             serialize_query_plan = 0
);

DROP TABLE o_05218;
DROP TABLE i_05218;
DROP TABLE o_notnull_05218;
DROP TABLE i_notnull_05218;
