-- A join condition may pass a Nullable column through a function whose result type can not hold a
-- NULL (`Array`, `Tuple`, `Map`). Such a function does not propagate NULLs.

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
DROP TABLE IF EXISTS small;

CREATE TABLE fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE mid (id UInt64, num Nullable(UInt8), str Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE small (arr Array(UInt8), str_arr Array(String), m Map(String, String), val UInt16) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO fact VALUES (1, 10), (2, 20), (3, 30); -- `id` 3 has no match in `mid`, so the LEFT join null-extends it.
INSERT INTO mid VALUES (1, 1, 'a:1'), (2, NULL, NULL);
INSERT INTO small VALUES ([], [''], {}, 0), ([1], ['a:1'], {'a':'1'}, 1);

SELECT '-- `bitmaskToArray` returns `Array(UInt8)`, and of a NULL it returns `[]`, so the LEFT join stays.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON bitmaskToArray(m.num) = s.arr
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- `splitByChar` of a NULL returns an array holding one empty string, so the LEFT join stays.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON splitByChar(',', m.str) = s.str_arr
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A `Map` result can not hold a NULL, so the LEFT join stays.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON extractKeyValuePairs(m.str) = s.m
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

SELECT '-- A wrapper above such a function does not propagate NULLs, so the LEFT join stays.';
SELECT trim(explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM fact AS f LEFT JOIN mid AS m ON f.id = m.id INNER JOIN small AS s ON length(bitmaskToArray(m.num)) = s.val
) WHERE trim(explain) IN ('Type: INNER', 'Type: LEFT', 'Type: RIGHT', 'Type: FULL');

DROP TABLE fact;
DROP TABLE mid;
DROP TABLE small;
