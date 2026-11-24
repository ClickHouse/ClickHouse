-- Tags: no-random-settings
-- ORDER BY pushdown into a simple VIEW must be skipped when the outer ORDER BY
-- item is an expression rather than a plain column reference. Pushdown adds an
-- inner "Sorting for ORDER BY" step inside the view subquery, so a fired pushdown
-- shows 2 such steps and a skipped one shows only the outer step (1).

SET allow_experimental_analyzer = 1;

DROP VIEW IF EXISTS v_obexpr;
DROP TABLE IF EXISTS t_obexpr;

CREATE TABLE t_obexpr (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY (id, ts);
INSERT INTO t_obexpr SELECT number, toDateTime(1600000000 + number) FROM numbers(100);
CREATE VIEW v_obexpr AS SELECT id, ts FROM t_obexpr;

SELECT 'plain column pushdown sorting steps:', countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%')
FROM (EXPLAIN SELECT id FROM v_obexpr ORDER BY ts DESC LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 0);

SELECT 'expression ORDER BY sorting steps:', countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%')
FROM (EXPLAIN SELECT id FROM v_obexpr ORDER BY id % 10 DESC LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 0);

SELECT 'expression ORDER BY result count:', count()
FROM (SELECT id FROM v_obexpr ORDER BY id % 10 DESC LIMIT 10);

DROP VIEW v_obexpr;
DROP TABLE t_obexpr;
