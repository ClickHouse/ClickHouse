-- Tags: no-random-settings

SET explain_query_plan_default = 'legacy';

SET enable_analyzer = 1;
DROP TABLE IF EXISTS t_prewhere_pushdown;

CREATE TABLE t_prewhere_pushdown
(
    a UInt32,
    b UInt32,
    c String,
    d UInt32
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO t_prewhere_pushdown
    SELECT number, number % 100, toString(number), number * 2
    FROM numbers(10000);

SELECT count() FROM t_prewhere_pushdown PREWHERE a > 5000 WHERE b = 7
SETTINGS optimize_prewhere_after_pushdown = 0;

SELECT count() FROM t_prewhere_pushdown PREWHERE a > 5000 WHERE b = 7
SETTINGS optimize_prewhere_after_pushdown = 1;

SELECT countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Prewhere%') > 0
FROM
(
    EXPLAIN PLAN actions = 0
    SELECT a FROM t_prewhere_pushdown PREWHERE a > 5000 WHERE b = 7
    SETTINGS optimize_prewhere_after_pushdown = 0
);

SELECT countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Prewhere%')
FROM
(
    EXPLAIN PLAN actions = 0
    SELECT a FROM t_prewhere_pushdown PREWHERE a > 5000 WHERE b = 7
    SETTINGS optimize_prewhere_after_pushdown = 1
);

DROP TABLE t_prewhere_pushdown;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (k UInt32, payload String, extra UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_right (k UInt32, x UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_left SELECT number, toString(number), number % 4 FROM numbers(10000);
INSERT INTO t_right VALUES (42, 1), (777, 10), (5000, 100), (7777, 200);

SELECT count()
FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
PREWHERE l.payload != ''
SETTINGS enable_join_runtime_filters = 1, optimize_prewhere_after_pushdown = 0;

SELECT count()
FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
PREWHERE l.payload != ''
SETTINGS enable_join_runtime_filters = 1, optimize_prewhere_after_pushdown = 1;

SELECT countIf(match(trim(explain), '^Filter($| )')) > 0
FROM (
    EXPLAIN PLAN actions = 0
    SELECT l.k FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
    PREWHERE l.payload != ''
    SETTINGS enable_join_runtime_filters = 1, optimize_prewhere_after_pushdown = 0
);

SELECT countIf(match(trim(explain), '^Filter($| )'))
FROM (
    EXPLAIN PLAN actions = 0
    SELECT l.k FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
    PREWHERE l.payload != ''
    SETTINGS enable_join_runtime_filters = 1, optimize_prewhere_after_pushdown = 1
);

SELECT countIf(explain LIKE '%Prewhere filter column%' AND explain LIKE '%applyFilter%') > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.k FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
    PREWHERE l.payload != ''
    SETTINGS enable_join_runtime_filters = 1, optimize_prewhere_after_pushdown = 1
);

SELECT count()
FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
PREWHERE l.payload != ''
WHERE l.extra = 2
SETTINGS enable_join_runtime_filters = 0, optimize_prewhere_after_pushdown = 0;

SELECT count()
FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
PREWHERE l.payload != ''
WHERE l.extra = 2
SETTINGS enable_join_runtime_filters = 0, optimize_prewhere_after_pushdown = 1;

SELECT countIf(match(trim(explain), '^Filter($| )')) > 0
FROM (
    EXPLAIN PLAN actions = 0
    SELECT l.k FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
    PREWHERE l.payload != ''
    WHERE l.extra = 2
    SETTINGS enable_join_runtime_filters = 0, optimize_prewhere_after_pushdown = 0
);

SELECT countIf(match(trim(explain), '^Filter($| )'))
FROM (
    EXPLAIN PLAN actions = 0
    SELECT l.k FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
    PREWHERE l.payload != ''
    WHERE l.extra = 2
    SETTINGS enable_join_runtime_filters = 0, optimize_prewhere_after_pushdown = 1
);

DROP TABLE t_left;
DROP TABLE t_right;
