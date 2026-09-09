-- Test outer to inner join conversion triggered by the predicate
-- of an another join higher in the plan.

SET max_threads = 2;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1; -- A test case relies on this.
SET query_plan_merge_filter_into_join_condition = 1; -- A test case relies on this.
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_derive_not_null_filters_from_joins = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS mid;
DROP TABLE IF EXISTS mid_nullable;
DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS other;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 20, number FROM numbers(100);
CREATE TABLE mid (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, number % 5 FROM numbers(10);
CREATE TABLE mid_nullable (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 2 = 0, NULL, number % 5) FROM numbers(10);
CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 2 * number + 1 FROM numbers(2);
CREATE TABLE other (id UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number FROM numbers(20);

SELECT '-- The enclosing INNER JOIN rejects the NULL-extended rows of the LEFT JOIN under join_use_nulls = 1 and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A natively Nullable join key is rejected even with join_use_nulls = 0 and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0, join_use_nulls = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS join_use_nulls = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_nullable AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A non-Nullable key with join_use_nulls = 0 does not convert.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS join_use_nulls = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
    SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- IS NOT DISTINCT FROM matches NULLs and does not convert.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val IS NOT DISTINCT FROM s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val IS NOT DISTINCT FROM s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A FULL enclosing join preserves non-matching rows and does not convert.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id FULL JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id FULL JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- Inequality conditions reject NULLs and convert.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val < s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val < s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val < s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- The join providing the IS NOT NULL filter can be a not direct parent of the outer join and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN other AS o ON f.id = o.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A RIGHT outer join whose NULL-extended left side is rejected by the enclosing join converts.';
SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM mid AS m RIGHT JOIN fact AS f ON f.id = m.id INNER JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An enclosing RIGHT join drops non-matching rows of its left input and converts the LEFT join below.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- ANY strictness on the enclosing join also drops non-matching rows and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER ANY JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A LEFT SEMI join also rejects null-extended rows.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT SEMI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- A RIGHT SEMI join also rejects null-extended rows.';
SELECT count(), sum(s.val) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT SEMI JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(s.val) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT SEMI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(s.val) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT SEMI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- A RIGHT ANTI join can not use null keys to exclude an rows, null-extended rows can be dropped.';
SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT ANTI JOIN small AS s ON m.val = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT ANTI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id RIGHT ANTI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- A LEFT ANTI join needs the rows from the preserved side, the below join does not convert.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT ANTI JOIN small AS s ON m.val = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT ANTI JOIN small AS s ON m.val = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- An ANY join converted to SEMI makes the other side droppable, the below join converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT ANY JOIN small AS s ON m.val = s.val WHERE s.val = 1
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT ANY JOIN small AS s ON m.val = s.val WHERE s.val = 1;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id LEFT ANY JOIN small AS s ON m.val = s.val WHERE s.val = 1
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

SELECT '-- A join condition merged into the join makes a side droppable, the below join converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN mid_nullable AS n ON f.id = n.id WHERE m.val = n.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN mid_nullable AS n ON f.id = n.id WHERE m.val = n.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN mid_nullable AS n ON f.id = n.id WHERE m.val = n.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL', 'Strictness: SEMI', 'Strictness: ANTI');

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE mid_nullable;
DROP TABLE small;
DROP TABLE other;
