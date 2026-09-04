-- Test deriving NOT NULL filters from join conditions whose sides are expressions rather than plain columns.

SET max_threads = 2;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_derive_not_null_filters_from_joins = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS mid;
DROP TABLE IF EXISTS mid_two;
DROP TABLE IF EXISTS small;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 20, number FROM numbers(100);
CREATE TABLE mid (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 2 = 0, NULL, number % 5) FROM numbers(10);
CREATE TABLE mid_two (id UInt64, val1 Nullable(UInt64), val2 Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 2 = 0, NULL, number % 5), if(number % 3 = 0, NULL, number % 4) FROM numbers(10);
CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 2 * number + 1 FROM numbers(3);

SELECT '-- An arithmetic expression propagates NULLs, so the filter is derived and the LEFT join converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A chain of NULL-propagating functions is followed to the column.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON abs(m.val + 1) = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON abs(m.val + 1) = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON abs(m.val + 1) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An expression over two Nullable columns derives a filter for both of them.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_two AS m ON f.id = m.id INNER JOIN small AS s ON m.val1 + m.val2 = s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_two AS m ON f.id = m.id INNER JOIN small AS s ON m.val1 + m.val2 = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_two AS m ON f.id = m.id INNER JOIN small AS s ON m.val1 + m.val2 = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An inequality condition over an expression also rejects NULLs and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 < s.val
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 < s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 < s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `coalesce` does not propagate NULLs, so no filter is derived and the LEFT join stays.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON coalesce(m.val, 1) = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON coalesce(m.val, 1) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `if` does not propagate NULLs of the branch that is not taken, so the LEFT join stays.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON if(m.id % 2 = 0, 1, m.val) = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON if(m.id % 2 = 0, 1, m.val) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A natively Nullable column under an expression is rejected with join_use_nulls = 0 as well.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
SETTINGS join_use_nulls = 0, query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
SETTINGS join_use_nulls = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
    SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An explicit CAST to a Nullable type propagates NULLs and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Nullable(Int64)) = CAST(s.val AS Nullable(Int64))
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Nullable(Int64)) = CAST(s.val AS Nullable(Int64));

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Nullable(Int64)) = CAST(s.val AS Nullable(Int64))
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `toNullable` propagates NULLs and converts.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toNullable(m.val) = toNullable(s.val)
SETTINGS query_plan_derive_not_null_filters_from_joins = 0;

SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toNullable(m.val) = toNullable(s.val);

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toNullable(m.val) = toNullable(s.val)
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A CAST to a non-Nullable type throws on a NULL instead of returning one, so no filter is derived.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Int64) = CAST(s.val AS Int64)
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE mid_two;
DROP TABLE small;
