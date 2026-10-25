-- Tests for LEFT ANTI JOIN with multiple join keys and runtime filters.
-- The runtime filter uses a Tuple-based NOT IN filter for exact tuple membership.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

-- ==========================================================================
-- Test 1: Basic 2-key regression test
-- Per-column NOT IN filters combined with AND were incorrect: they dropped
-- rows where one key was in its per-column set but the full tuple had no match.
-- ==========================================================================

SELECT '--- Test 1: Basic 2-key LEFT ANTI JOIN ---';

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 SELECT number % 2, number % 3 FROM numbers(3);
INSERT INTO t2 SELECT number % 3, number % 2 FROM numbers(3);

-- t1 = {(0,0), (1,1), (0,2)}, t2 = {(0,0), (1,1), (2,0)}
-- Correct ANTI JOIN result: (0,2) — no row in t2 has aa=0 AND bb=2 simultaneously

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 2: 3-key LEFT ANTI JOIN
-- Verifies tuple filter generalizes beyond 2 keys.
-- ==========================================================================

SELECT '--- Test 2: 3-key LEFT ANTI JOIN ---';

CREATE TABLE t1 (a Int64, b Int64, c Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64, cc Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 VALUES (1, 2, 3), (1, 2, 4), (5, 6, 7);
INSERT INTO t2 VALUES (1, 2, 3), (5, 6, 7);

-- (1,2,3) matches, (5,6,7) matches, only (1,2,4) survives

SELECT t1.a, t1.b, t1.c FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb AND t1.c = t2.cc
ORDER BY t1.a, t1.b, t1.c
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b, t1.c FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb AND t1.c = t2.cc
ORDER BY t1.a, t1.b, t1.c
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 3: Mixed types requiring cast (Int32 vs Int64)
-- Verifies that per-key type casting works correctly with tuple filter.
-- ==========================================================================

SELECT '--- Test 3: Mixed types (Int32 vs Int64) ---';

CREATE TABLE t1 (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1, 10), (3, 30);

-- (1,10) and (3,30) match, only (2,20) survives

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 4: Empty right side — all left rows survive
-- ==========================================================================

SELECT '--- Test 4: Empty right side ---';

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 VALUES (1, 2), (3, 4);

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 5: All rows match — empty result
-- ==========================================================================

SELECT '--- Test 5: All rows match ---';

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 VALUES (1, 2), (3, 4);
INSERT INTO t2 VALUES (1, 2), (3, 4), (5, 6);

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 6: Single-key LEFT ANTI JOIN still works (uses per-column path)
-- ==========================================================================

SELECT '--- Test 6: Single-key LEFT ANTI JOIN ---';

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1), (3);

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 6b: Single-key nullable LEFT ANTI JOIN
-- NULL key rows must survive because NULL = NULL is false in SQL.
-- ==========================================================================

SELECT '--- Test 6b: Single-key nullable LEFT ANTI JOIN ---';

CREATE TABLE t1 (a Nullable(Int64), b Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (aa Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (1, 10), (NULL, 20), (3, 30);
INSERT INTO t2 VALUES (1), (NULL);

-- (1) matches -> filtered out
-- (NULL) from t1: NULL != NULL -> survives
-- (3) from t1: no match -> survives

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa
ORDER BY t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa
ORDER BY t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 7: Multi-key INNER JOIN still uses per-column path correctly
-- ==========================================================================

SELECT '--- Test 7: Multi-key INNER JOIN ---';

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 SELECT number % 2, number % 3 FROM numbers(3);
INSERT INTO t2 SELECT number % 3, number % 2 FROM numbers(3);

SELECT t1.a, t1.b FROM t1 INNER JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 INNER JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 8: Large right side exceeding exact_values_limit
-- When the set overflows, the filter is disabled and results must stay correct.
-- ==========================================================================

SELECT '--- Test 8: Large right side (filter overflow) ---';

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 VALUES (0, 999), (1, 1), (2, 2);
INSERT INTO t2 SELECT number, number FROM numbers(200);

-- (1,1) and (2,2) match, (0,999) does not since t2 has (0,0) not (0,999)

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 10;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 9: Nullable multi-key LEFT ANTI JOIN
-- NULL handling with tuple-based exclusion is subtle: tuples containing NULL
-- never match via equality, so rows with any NULL key must always survive.
-- ==========================================================================

SELECT '--- Test 9: Nullable multi-key LEFT ANTI JOIN ---';

CREATE TABLE t1 (a Nullable(Int64), b Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (aa Nullable(Int64), bb Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (1, 2), (NULL, 2), (1, NULL), (NULL, NULL), (3, 4);
INSERT INTO t2 VALUES (1, 2), (NULL, 2), (NULL, NULL);

-- (1,2) matches exactly -> filtered out
-- (NULL,2) from t1: NULL != NULL in equality, so no match -> survives
-- (1,NULL) from t1: NULL in second key, no match -> survives
-- (NULL,NULL) from t1: NULL != NULL, no match -> survives
-- (3,4) from t1: no match in t2 -> survives

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a NULLS LAST, t1.b NULLS LAST
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a NULLS LAST, t1.b NULLS LAST
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 10: Nullable multi-key with mixed types (Nullable(Int32) vs Nullable(Int64))
-- Ensures casting + NULL handling work together in tuple-based exclusion.
-- ==========================================================================

SELECT '--- Test 10: Nullable mixed types ---';

CREATE TABLE t1 (a Nullable(Int32), b Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (aa Nullable(Int64), bb Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (1, 2), (NULL, 5), (3, NULL);
INSERT INTO t2 VALUES (1, 2), (NULL, 5);

-- (1,2) matches -> filtered out
-- (NULL,5): NULL key -> survives
-- (3,NULL): NULL key -> survives

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a NULLS LAST, t1.b NULLS LAST
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a NULLS LAST, t1.b NULLS LAST
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 11: LowCardinality(Nullable(...)) single-key
-- isNullable() returns false for LowCardinality(Nullable(T)), but NULLs
-- still exist and must bypass the exclusion filter.
-- ==========================================================================

SELECT '--- Test 11: LowCardinality(Nullable) single-key ---';

CREATE TABLE t1 (a LowCardinality(Nullable(String)), b Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (aa LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES ('x', 1), (NULL, 2), ('z', 3);
INSERT INTO t2 VALUES ('x'), (NULL);

-- ('x') matches -> filtered out
-- (NULL): NULL != NULL -> survives
-- ('z'): no match -> survives

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa
ORDER BY t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa
ORDER BY t1.b
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;

-- ==========================================================================
-- Test 12: LowCardinality(Nullable(...)) multi-key
-- ==========================================================================

SELECT '--- Test 12: LowCardinality(Nullable) multi-key ---';

CREATE TABLE t1 (a LowCardinality(Nullable(String)), b LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (aa LowCardinality(Nullable(String)), bb LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES ('x', 'a'), (NULL, 'a'), ('x', NULL), (NULL, NULL), ('y', 'b');
INSERT INTO t2 VALUES ('x', 'a'), (NULL, 'a'), (NULL, NULL);

-- ('x','a') matches -> filtered out
-- (NULL,'a'): NULL key -> survives
-- ('x',NULL): NULL key -> survives
-- (NULL,NULL): NULL keys -> survives
-- ('y','b'): no match -> survives

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a NULLS LAST, t1.b NULLS LAST
SETTINGS enable_join_runtime_filters = 0;

SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a NULLS LAST, t1.b NULLS LAST
SETTINGS enable_join_runtime_filters = 1;

DROP TABLE t1;
DROP TABLE t2;
