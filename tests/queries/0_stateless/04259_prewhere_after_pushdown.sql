-- Tags: no-random-settings

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
