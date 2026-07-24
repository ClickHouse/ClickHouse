-- Covers `buildJoinClauses` in `src/Planner/PlannerJoins.cpp`, which walks the `JOIN ON` expression
-- with a peek-don't-pop stack and combines per-node join clauses. The traversal runs only when
-- general join planning is enabled, so the expression shapes below are otherwise not exercised.

SET enable_analyzer = 1;
SET join_algorithm = 'hash';
SET allow_general_join_planning = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO t1 SELECT number, number % 5, number % 3 FROM numbers(20);
INSERT INTO t2 SELECT number, number % 5, number % 3 FROM numbers(20);

SELECT '-- single equality';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- n-ary AND, flattened by the analyzer into one node with many children';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- OR of equalities: each branch becomes a separate join clause';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- AND over OR: cross product of child clauses';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (t1.a = t2.a OR t1.b = t2.b) AND (t1.c = t2.c OR t1.a = t2.b) ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- alternating AND/OR nesting: the shape that survives analyzer flattening as a deep tree';
SELECT t1.a, t2.a FROM t1 JOIN t2
ON t1.a = t2.a AND (t1.b = t2.b OR (t1.c = t2.c AND (t1.a = t2.b OR t1.b = t2.c)))
ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- logical node touching only one side: must not be split into multiple clauses';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a AND (t1.b = 1 OR t1.b = 2) ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- residual inequality condition alongside key conditions';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b < t2.c ORDER BY t1.a, t2.a LIMIT 5;

SELECT '-- same results with general join planning disabled';
SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c
ORDER BY t1.a, t2.a LIMIT 5 SETTINGS allow_general_join_planning = 0;
SELECT t1.a, t2.a FROM t1 JOIN t2 ON (t1.a = t2.a OR t1.b = t2.b) AND (t1.c = t2.c OR t1.a = t2.b)
ORDER BY t1.a, t2.a LIMIT 5 SETTINGS allow_general_join_planning = 0;

DROP TABLE t1;
DROP TABLE t2;
