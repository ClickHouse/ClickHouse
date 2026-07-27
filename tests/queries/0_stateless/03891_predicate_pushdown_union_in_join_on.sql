-- Regression test for a crash (vector out-of-bounds abort) in
-- PredicateRewriteVisitor::rewriteSubquery. With the legacy analyzer and
-- enable_optimize_predicate_expression, the predicate pushdown visitor used to
-- descend into a UNION subquery that lives inside the JOIN ON condition and
-- treat it as the joined table's defining subquery, mapping outer-table column
-- positions onto the unrelated inner columns (out-of-bounds read). The outer
-- predicate (t2.a = 20) must still be pushed into the t2 subquery while the
-- ON-clause UNION is left untouched, so the result is correct under both analyzers.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (key UInt32, a UInt32) ENGINE = Memory;
CREATE TABLE t2 (key UInt32, a UInt32) ENGINE = Memory;
CREATE TABLE t3 (v UInt32) ENGINE = Memory;

INSERT INTO t1 VALUES (1, 10), (2, 20);
INSERT INTO t2 VALUES (1, 10), (2, 20);
INSERT INTO t3 VALUES (10), (20);

SET enable_optimize_predicate_expression = 1;

SET enable_analyzer = 0;
SELECT t2.key, t2.a
FROM t1 INNER JOIN t2
ON (t2.a IN (SELECT v FROM t3 UNION DISTINCT SELECT v FROM t3)) AND (t2.key = t1.key)
WHERE t2.a = 20
ORDER BY t2.key;

SET enable_analyzer = 1;
SELECT t2.key, t2.a
FROM t1 INNER JOIN t2
ON (t2.a IN (SELECT v FROM t3 UNION DISTINCT SELECT v FROM t3)) AND (t2.key = t1.key)
WHERE t2.a = 20
ORDER BY t2.key;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
