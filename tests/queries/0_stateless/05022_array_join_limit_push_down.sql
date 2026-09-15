DROP TABLE IF EXISTS t_array_join_limit;

CREATE TABLE t_array_join_limit
(
    x UInt64,
    arr Array(UInt32),
    arr2 Array(UInt32),
    m Map(String, UInt32)
)
ENGINE = MergeTree ORDER BY x;

INSERT INTO t_array_join_limit
SELECT
    number,
    if(number % 3 = 0, [], [toUInt32(number), toUInt32(number + 100)]),
    if(number % 3 = 0, [], [toUInt32(number * 2), toUInt32(number * 3)]),
    if(number % 3 = 0, map(), map('a', toUInt32(number), 'b', toUInt32(number + 1)))
FROM numbers(30);

SET max_threads = 1;
SET query_plan_top_k_through_array_join = 0;

SELECT '-- inner';
SELECT x, arr FROM t_array_join_limit ARRAY JOIN arr LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, arr FROM t_array_join_limit ARRAY JOIN arr LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- LEFT';
SELECT x, arr FROM t_array_join_limit LEFT ARRAY JOIN arr LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, arr FROM t_array_join_limit LEFT ARRAY JOIN arr LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- offset';
SELECT x, arr FROM t_array_join_limit ARRAY JOIN arr LIMIT 7 OFFSET 5
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, arr FROM t_array_join_limit ARRAY JOIN arr LIMIT 7 OFFSET 5
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- aligned arrays';
SELECT x, arr, arr2 FROM t_array_join_limit ARRAY JOIN arr, arr2 LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, arr, arr2 FROM t_array_join_limit ARRAY JOIN arr, arr2 LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- unaligned arrays';
SELECT x, arr, arr2 FROM t_array_join_limit ARRAY JOIN arr, arraySlice(arr2, 1, 1) AS arr2 LIMIT 9
SETTINGS enable_unaligned_array_join = 1, query_plan_push_down_limit_through_array_join = 0;
SELECT x, arr, arr2 FROM t_array_join_limit ARRAY JOIN arr, arraySlice(arr2, 1, 1) AS arr2 LIMIT 9
SETTINGS enable_unaligned_array_join = 1, query_plan_push_down_limit_through_array_join = 1;

SELECT '-- Map';
SELECT x, m FROM t_array_join_limit ARRAY JOIN m LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, m FROM t_array_join_limit ARRAY JOIN m LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- constant array';
SELECT x, elem FROM t_array_join_limit ARRAY JOIN [1, 2] AS elem LIMIT 7
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, elem FROM t_array_join_limit ARRAY JOIN [1, 2] AS elem LIMIT 7
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- chained ARRAY JOIN';
SELECT x, arr, arr2 FROM t_array_join_limit ARRAY JOIN arr ARRAY JOIN arr2 LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT x, arr, arr2 FROM t_array_join_limit ARRAY JOIN arr ARRAY JOIN arr2 LIMIT 9
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- all empty';
SELECT count() FROM
(
    SELECT x
    FROM t_array_join_limit
    ARRAY JOIN CAST([], 'Array(UInt32)') AS elem
    LIMIT 7
)
SETTINGS query_plan_push_down_limit_through_array_join = 0;
SELECT count() FROM
(
    SELECT x
    FROM t_array_join_limit
    ARRAY JOIN CAST([], 'Array(UInt32)') AS elem
    LIMIT 7
)
SETTINGS query_plan_push_down_limit_through_array_join = 1;

SELECT '-- exact rows before limit is not rewritten';
SELECT countIf(match(explain, '^[^A-Za-z]*Limit$'))
FROM
(
    EXPLAIN PLAN description = 0
    SELECT x FROM t_array_join_limit ARRAY JOIN arr LIMIT 7
    SETTINGS exact_rows_before_limit = 1, query_plan_push_down_limit_through_array_join = 1
);

SELECT '-- plan shape off/on: Limit Filter';
SELECT
    countIf(match(explain, '^[^A-Za-z]*Limit$')),
    countIf(match(explain, '^[^A-Za-z]*Filter$'))
FROM
(
    EXPLAIN PLAN description = 0
    SELECT x FROM t_array_join_limit ARRAY JOIN arr LIMIT 7
    SETTINGS query_plan_push_down_limit_through_array_join = 0
);
SELECT
    countIf(match(explain, '^[^A-Za-z]*Limit$')),
    countIf(match(explain, '^[^A-Za-z]*Filter$'))
FROM
(
    EXPLAIN PLAN description = 0
    SELECT x FROM t_array_join_limit ARRAY JOIN arr LIMIT 7
    SETTINGS query_plan_push_down_limit_through_array_join = 1
);

SELECT '-- chained plan shape: one inner Limit and guard per ARRAY JOIN';
EXPLAIN PLAN description = 0
SELECT x FROM t_array_join_limit ARRAY JOIN arr ARRAY JOIN arr2 LIMIT 7
SETTINGS
    query_plan_push_down_limit_through_array_join = 1,
    query_plan_optimize_lazy_materialization = 0,
    query_plan_optimize_prewhere = 0,
    optimize_move_to_prewhere = 0;

SELECT '-- serialized plan';
SELECT count()
FROM (SELECT x FROM t_array_join_limit ARRAY JOIN arr LIMIT 7)
SETTINGS serialize_query_plan = 1, query_plan_push_down_limit_through_array_join = 1;

DROP TABLE t_array_join_limit;
