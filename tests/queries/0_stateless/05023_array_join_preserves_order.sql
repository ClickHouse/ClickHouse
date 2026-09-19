DROP TABLE IF EXISTS t_array_join_order;

CREATE TABLE t_array_join_order
(
    pk UInt64,
    a UInt64,
    b Array(UInt64),
    c UInt64
)
ENGINE = MergeTree ORDER BY pk;

INSERT INTO t_array_join_order
SELECT number, number % 5, [number % 3, number % 7], 100 - number
FROM numbers(20);

-- Keep an explicit sorting step below the `ARRAY JOIN`, and avoid unrelated rewrites that can
-- obscure the sorting property under test.
SET query_plan_read_in_order = 0;
SET query_plan_top_k_through_array_join = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET optimize_sorting_by_input_stream_properties = 1;

SELECT '-- property disabled: no prefix';
SELECT countIf(explain LIKE '%Prefix sort description:%')
FROM
(
    EXPLAIN PLAN
    SELECT a, b, c
    FROM (SELECT * FROM t_array_join_order ORDER BY a, b LIMIT 1000)
    ARRAY JOIN b
    ORDER BY a, b, c
    SETTINGS query_plan_preserve_order_through_array_join = 0
);

SELECT '-- property enabled: prefix stops before joined column b';
SELECT trim(replaceRegexpOne(explain, '^.*Prefix sort description: ', ''))
FROM
(
    EXPLAIN PLAN
    SELECT a, b, c
    FROM (SELECT * FROM t_array_join_order ORDER BY a, b LIMIT 1000)
    ARRAY JOIN b
    ORDER BY a, b, c
    SETTINGS query_plan_preserve_order_through_array_join = 1
)
WHERE explain LIKE '%Prefix sort description:%';

SELECT '-- a non-joined prefix is preserved completely';
SELECT trim(replaceRegexpOne(explain, '^.*Prefix sort description: ', ''))
FROM
(
    EXPLAIN PLAN
    SELECT a, b, c
    FROM (SELECT * FROM t_array_join_order ORDER BY a, c LIMIT 1000)
    ARRAY JOIN b
    ORDER BY a, c, b
    SETTINGS query_plan_preserve_order_through_array_join = 1
)
WHERE explain LIKE '%Prefix sort description:%';

SELECT '-- joined column first: no prefix';
SELECT countIf(explain LIKE '%Prefix sort description:%')
FROM
(
    EXPLAIN PLAN
    SELECT a, b, c
    FROM (SELECT * FROM t_array_join_order ORDER BY b, a LIMIT 1000)
    ARRAY JOIN b
    ORDER BY b, a, c
    SETTINGS query_plan_preserve_order_through_array_join = 1
);

SELECT '-- results are unchanged';
SELECT a, b, c
FROM (SELECT * FROM t_array_join_order ORDER BY a, b LIMIT 1000)
ARRAY JOIN b
ORDER BY a, b, c
SETTINGS query_plan_preserve_order_through_array_join = 0;
SELECT a, b, c
FROM (SELECT * FROM t_array_join_order ORDER BY a, b LIMIT 1000)
ARRAY JOIN b
ORDER BY a, b, c
SETTINGS query_plan_preserve_order_through_array_join = 1;

DROP TABLE t_array_join_order;
