-- Tests that transitive predicate inference does NOT break outer joins.
--
-- Equivalences from LEFT/RIGHT/FULL JOIN predicates must not propagate
-- to parent joins, because outer joins produce NULLs for non-matching rows,
-- invalidating the equality.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (key UInt64, a UInt64, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (key UInt64, a UInt64, attr String) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t3 (key UInt64, a UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t4 (key UInt64, a UInt64) ENGINE = MergeTree ORDER BY key;

INSERT INTO t1 VALUES (1, 10, 'alpha'), (2, 15, 'beta'), (3, 20, 'gamma');
INSERT INTO t2 VALUES (1, 5, 'ALPHA'), (4, 25, 'DELTA');
INSERT INTO t3 VALUES (0, 10), (1, 100), (2, 1000);
INSERT INTO t4 VALUES (1, 1), (2, 2), (3, 3);

-- ==========================================================================
-- Case 1: LEFT JOIN chain -- t2.key = t3.key must NOT be removed
-- For t1.key=2: t2 has no match (LEFT JOIN), so t2.key is 0.
-- t3.key=2 should NOT match t2.key=0.
-- ==========================================================================

SELECT '-- Case 1: LEFT JOIN chain preserves t2.key = t3.key';
SELECT t1.key, t1.a, t2.key, t2.a, t3.key, t3.a
FROM t1
LEFT JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t1.key = t3.key AND t2.key = t3.key
ORDER BY t1.key;

-- Compare with setting off
SELECT t1.key, t1.a, t2.key, t2.a, t3.key, t3.a
FROM t1
LEFT JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t1.key = t3.key AND t2.key = t3.key
ORDER BY t1.key
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 2: RIGHT JOIN chain
-- For t2.key=4: t1 has no match (RIGHT JOIN), so t1.key is 0.
-- t3 should NOT match via transitive t1.key = t3.key when t1.key is 0.
-- ==========================================================================

SELECT '-- Case 2: RIGHT JOIN chain';
SELECT t1.key, t1.a, t2.key, t2.a, t3.key, t3.a
FROM t1
RIGHT JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t1.key = t3.key AND t2.key = t3.key
ORDER BY t2.key;

SELECT t1.key, t1.a, t2.key, t2.a, t3.key, t3.a
FROM t1
RIGHT JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t1.key = t3.key AND t2.key = t3.key
ORDER BY t2.key
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 3: Mixed INNER + LEFT JOIN
-- INNER equivalence t1.key = t2.key is valid (both sides always present),
-- but LEFT equivalence t2.key = t3.key should NOT propagate.
-- ==========================================================================

SELECT '-- Case 3: INNER then LEFT';
SELECT t1.key, t2.key, t3.key, t4.key
FROM t1
INNER JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t2.key = t3.key
LEFT JOIN t4 ON t3.key = t4.key AND t2.key = t4.key
ORDER BY t1.key;

SELECT t1.key, t2.key, t3.key, t4.key
FROM t1
INNER JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t2.key = t3.key
LEFT JOIN t4 ON t3.key = t4.key AND t2.key = t4.key
ORDER BY t1.key
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 4: FULL OUTER JOIN -- neither side is guaranteed non-NULL
-- ==========================================================================

SELECT '-- Case 4: FULL OUTER JOIN';
SELECT t1.key, t2.key, t3.key
FROM t1
FULL JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t1.key = t3.key AND t2.key = t3.key
ORDER BY ALL;

SELECT t1.key, t2.key, t3.key
FROM t1
FULL JOIN t2 ON t1.key = t2.key
LEFT JOIN t3 ON t1.key = t3.key AND t2.key = t3.key
ORDER BY ALL
SETTINGS enable_join_transitive_predicates = 0;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
