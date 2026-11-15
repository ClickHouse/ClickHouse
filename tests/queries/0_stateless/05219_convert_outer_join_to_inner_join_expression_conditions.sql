-- Tags: long
-- Test outer to inner join conversion driven by a join condition whose sides are expressions
-- rather than plain columns, and by expressions that do not propagate NULLs at all.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_outer_join_to_inner_join_transitively = 1;

DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS mid;
DROP TABLE IF EXISTS mid_two;
DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS fact_typed;
DROP TABLE IF EXISTS mid_typed;
DROP TABLE IF EXISTS small_typed;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT number % 20, number FROM numbers(100);
CREATE TABLE mid (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 2 = 0, NULL, number % 5) FROM numbers(10);
CREATE TABLE mid_two (id UInt64, val1 Nullable(UInt64), val2 Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple() AS SELECT number, if(number % 2 = 0, NULL, number % 5), if(number % 3 = 0, NULL, number % 4) FROM numbers(10);
CREATE TABLE small (val UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 2 * number + 1 FROM numbers(3);
CREATE TABLE fact_typed (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE mid_typed (id UInt64, num Nullable(UInt8), str Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE small_typed (arr Array(UInt8), str_arr Array(String), m Map(String, String), val UInt16) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO fact_typed VALUES (1, 10), (2, 20), (3, 30); -- `id` 3 has no match in `mid_typed`, so the LEFT join null-extends it.
INSERT INTO mid_typed VALUES (1, 1, 'a:1'), (2, NULL, NULL);
INSERT INTO small_typed VALUES ([], [''], {}, 0), ([1], ['a:1'], {'a':'1'}, 1);

SELECT '-- An arithmetic expression allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A chain of NULL-propagating functions allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON abs(m.val + 1) = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON abs(m.val + 1) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An expression over two Nullable columns allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_two AS m ON f.id = m.id INNER JOIN small AS s ON m.val1 + m.val2 = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid_two AS m ON f.id = m.id INNER JOIN small AS s ON m.val1 + m.val2 = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- An inequality over an expression allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 < s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 < s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `coalesce` does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON coalesce(m.val, 1) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `if` does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON if(m.id % 2 = 0, 1, m.val) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A Nullable column under an expression allows converting under join_use_nulls = 0.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
SETTINGS join_use_nulls = 0;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON m.val + 1 = s.val
    SETTINGS join_use_nulls = 0
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- CAST to a Nullable type allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Nullable(Int64)) = CAST(s.val AS Nullable(Int64));

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Nullable(Int64)) = CAST(s.val AS Nullable(Int64))
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `toNullable` allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toNullable(m.val) = toNullable(s.val);

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toNullable(m.val) = toNullable(s.val)
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `toLowCardinality` allows converting.';
SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toLowCardinality(m.val) = s.val;

SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON toLowCardinality(m.val) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- CAST to a non-Nullable type does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count(), sum(f.v) FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON CAST(m.val AS Int64) = CAST(s.val AS Int64)
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '---- Expressions whose result type can not hold a NULL. ----';

SELECT '-- `bitmaskToArray` does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact_typed AS f LEFT JOIN mid_typed AS m ON f.id = m.id INNER JOIN small_typed AS s ON bitmaskToArray(m.num) = s.arr
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `splitByChar` does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact_typed AS f LEFT JOIN mid_typed AS m ON f.id = m.id INNER JOIN small_typed AS s ON splitByChar(',', m.str) = s.str_arr
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `extractKeyValuePairs` does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact_typed AS f LEFT JOIN mid_typed AS m ON f.id = m.id INNER JOIN small_typed AS s ON extractKeyValuePairs(m.str) = s.m
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A wrapper above such a function does not allow converting.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact_typed AS f LEFT JOIN mid_typed AS m ON f.id = m.id INNER JOIN small_typed AS s ON length(bitmaskToArray(m.num)) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE mid_two;
DROP TABLE small;
DROP TABLE fact_typed;
DROP TABLE mid_typed;
DROP TABLE small_typed;
