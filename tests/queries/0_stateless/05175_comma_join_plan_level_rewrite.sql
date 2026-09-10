-- Comma joins are kept as plain JOIN nodes in the query tree; the conversion to INNER JOIN
-- with the equalities from WHERE happens in the query plan.

-- The test runner randomizes this optimization; the plan shapes below depend on it.
SET query_plan_merge_filter_into_join_condition = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;

CREATE TABLE t1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t3 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t4 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t5 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 VALUES (1, 2), (3, 4), (5, 6);
INSERT INTO t2 VALUES (3, 4), (5, 6), (7, 8);
INSERT INTO t3 VALUES (5, 6), (7, 8), (9, 10);
INSERT INTO t4 VALUES (7, 8), (9, 10), (11, 12);
INSERT INTO t5 VALUES (9, 10), (11, 12), (13, 14);

SELECT '-- query tree keeps the comma join';
SELECT countIf(explain LIKE '%kind: COMMA%'), countIf(explain LIKE '%kind: INNER%')
FROM (EXPLAIN QUERY TREE SELECT * FROM t1, t2 WHERE t1.a = t2.a);

SELECT '-- N connected tables give N-1 INNER joins';
SELECT countIf(explain ILIKE '%Type: CROSS%' OR explain ILIKE '%Type: COMMA%'), countIf(explain ILIKE '%Type: INNER%')
FROM (EXPLAIN actions = 1 SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t2.a = t3.a);

SELECT countIf(explain ILIKE '%Type: CROSS%' OR explain ILIKE '%Type: COMMA%'), countIf(explain ILIKE '%Type: INNER%')
FROM (EXPLAIN actions = 1 SELECT * FROM t1, t2, t3, t4, t5 WHERE t1.a = t2.a AND t2.a = t3.a AND t3.a = t4.a AND t4.a = t5.a);

SELECT '-- two connected components give one cross join between them';
SELECT countIf(explain ILIKE '%Type: CROSS%' OR explain ILIKE '%Type: COMMA%'), countIf(explain ILIKE '%Type: INNER%')
FROM (EXPLAIN actions = 1 SELECT * FROM t1, t2, t3, t4, t5 WHERE t1.a = t3.a AND t3.b = t4.b AND t1.a = t4.a AND t2.a = t5.a);

SELECT '-- one cross join stays for a disconnected table';
SELECT countIf(explain ILIKE '%Type: CROSS%' OR explain ILIKE '%Type: COMMA%'), countIf(explain ILIKE '%Type: INNER%')
FROM (EXPLAIN actions = 1 SELECT * FROM t1, t2, t3 WHERE t1.a = t3.a);

SELECT '-- results';
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t2.a = t3.a ORDER BY ALL;
SELECT * FROM t1, t2, t3 WHERE t1.a = t3.a ORDER BY ALL;

SELECT '-- force mode: a comma join without an equi-join condition is an error';
SELECT count() FROM t1, t2, t3 WHERE t1.a = t3.a SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }
SELECT count() FROM t1, t2 WHERE t1.a > t2.a SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }
SELECT count() FROM t1, t2, t3 WHERE t1.a = t2.a AND t2.a = t3.a SETTINGS cross_to_inner_join_rewrite = 2;
SELECT '-- force mode implies the rewrite even when the optimization is disabled';
SELECT count() FROM t1, t2 WHERE t1.a = t2.a SETTINGS cross_to_inner_join_rewrite = 2, query_plan_merge_filter_into_join_condition = 0;
SELECT '-- explicit CROSS JOIN is never forced';
SELECT count() FROM t1 CROSS JOIN t2 WHERE t1.a > t2.a SETTINGS cross_to_inner_join_rewrite = 2;

SELECT '-- GLOBAL is kept for a cross join';
SELECT countIf(explain LIKE '%GLOBAL CROSS JOIN%')
FROM (EXPLAIN QUERY TREE dump_ast = 1 SELECT * FROM t1 GLOBAL CROSS JOIN t2);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
