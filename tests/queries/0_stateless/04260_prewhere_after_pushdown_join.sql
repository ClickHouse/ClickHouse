-- Tags: no-random-settings
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (k UInt32, payload String, extra UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_right (k UInt32, x UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_left SELECT number, toString(number), number % 4 FROM numbers(10000);
INSERT INTO t_right VALUES (42, 1), (777, 10), (5000, 100), (7777, 200);

-- Case 1: JOIN WITH runtime filters

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

-- Case 2: JOIN WITHOUT runtime filters, predicate pushdown only

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
