-- Edge case tests for transitive predicate inference.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;

DROP TABLE IF EXISTS e1;
DROP TABLE IF EXISTS e2;
DROP TABLE IF EXISTS e3;

CREATE TABLE e1 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE e2 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE e3 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;

INSERT INTO e1 SELECT number, number * 100 FROM numbers(10);
INSERT INTO e2 SELECT number, number * 200 FROM numbers(10);
INSERT INTO e3 SELECT number, number * 300 FROM numbers(10);

-- ==========================================================================
-- Case 7: Non-INPUT expression in predicate (e1.key + 1 = e2.key)
-- Should NOT form equivalence classes (only simple column refs do).
-- Must not crash or produce wrong results.
-- ==========================================================================

SELECT '-- Case 7: non-INPUT expression';
SELECT e1.key, e2.key, e3.key
FROM e1
INNER JOIN e2 ON e1.key + 1 = e2.key
INNER JOIN e3 ON e2.key = e3.key
ORDER BY e1.key LIMIT 5;

SELECT e1.key, e2.key, e3.key
FROM e1
INNER JOIN e2 ON e1.key + 1 = e2.key
INNER JOIN e3 ON e2.key = e3.key
ORDER BY e1.key LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 9: Chain with WHERE filter after join
-- The filter should still apply correctly after transitive optimization.
-- ==========================================================================

SELECT '-- Case 9: WHERE filter after join';
SELECT e1.key, e2.key, e3.key
FROM e1, e2, e3
WHERE e1.key = e2.key AND e2.key = e3.key AND e1.key > 5
ORDER BY e1.key;

SELECT e1.key, e2.key, e3.key
FROM e1, e2, e3
WHERE e1.key = e2.key AND e2.key = e3.key AND e1.key > 5
ORDER BY e1.key
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 10: Self-join transitivity
-- Same table aliased three times.
-- ==========================================================================

SELECT '-- Case 10: self-join';
SELECT a.key, b.key, c.key
FROM e1 AS a, e1 AS b, e1 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key LIMIT 5;

SELECT a.key, b.key, c.key
FROM e1 AS a, e1 AS b, e1 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 11: Subquery as join input
-- One relation is a filtered subquery. Equivalences should still work.
-- ==========================================================================

SELECT '-- Case 11: subquery input';
SELECT a.key, b.key, c.key
FROM e1 AS a, (SELECT key, val FROM e2 WHERE val < 1500) AS b, e3 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key;

SELECT a.key, b.key, c.key
FROM e1 AS a, (SELECT key, val FROM e2 WHERE val < 1500) AS b, e3 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key
SETTINGS enable_join_transitive_predicates = 0;

DROP TABLE e1;
DROP TABLE e2;
DROP TABLE e3;
