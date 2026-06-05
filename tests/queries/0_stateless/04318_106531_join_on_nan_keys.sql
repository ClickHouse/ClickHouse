-- Regression test for issue #106531: `INNER JOIN ON` with float keys must treat
-- `NaN != NaN` per IEEE 754 / SQL JOIN ON semantics. Before the fix the hash table
-- compared float keys bitwise via `bitEquals`, so two `NaN`s with matching bit
-- patterns wrongly joined. The fix folds `NaN` rows into the JOIN-key null map so
-- they are skipped on both build and probe sides, matching how `NULL` keys behave.

DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t2 (c0 Int32, c1 Int32, c2 String) ENGINE = MergeTree() ORDER BY lcm(c0, c1);
CREATE TABLE t3 (c0 Int32) ENGINE = Memory();

INSERT INTO t2(c2, c1, c0) VALUES ('r-80l', 821753656, 1509704461), ('a0}', -823561908, -314051915), ('x9', 1545468958, -924334719);
INSERT INTO t3(c0) VALUES (-792862717);

-- Original minimised reproducer from issue #106531: probe-side `(t3.c0 + t3.c0) % pow(t3.c0, t3.c0)`
-- evaluates to `NaN` and so does `pow(-t2.c0, sqrt(t2.c0))` for two of the three rows in t2.
-- Expected: 0 rows. Before the fix: 2 rows.
SELECT t2.c0
FROM t3 INNER JOIN t2
  ON (t3.c0 + t3.c0) % pow(t3.c0, t3.c0) = pow(-t2.c0, sqrt(t2.c0))
WHERE NOT (sqrt(intDiv(t2.c0, t2.c0)) > 100);

-- Issue body's full reproducer with HAVING + non-default settings.
SELECT t2.c0, t3.c0, t2.c1
FROM t3 INNER JOIN t2
  ON (t3.c0 + t3.c0) % pow(t3.c0, t3.c0) = pow(-t2.c0, sqrt(t2.c0))
GROUP BY t2.c0, t3.c0, t2.c1
HAVING NOT (log(t2.c0 + 972947543) < sqrt(intDiv(t2.c0, t2.c0)))
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

-- ANSI-style sanity: small explicit `NaN` keys across all join kinds and algorithms.
DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k Float64, v String) ENGINE = Memory();
CREATE TABLE r (k Float64, w Int32) ENGINE = Memory();
INSERT INTO l VALUES (1.5, 'matched'), (nan, 'nan-l'), (2.5, 'unmatched');
INSERT INTO r VALUES (1.5, 100), (nan, 9999);

SELECT '--- hash INNER ---';
SELECT v, k FROM (SELECT l.v, l.k FROM l INNER JOIN r ON l.k = r.k) ORDER BY v;

SELECT '--- hash LEFT ---';
SELECT v, l_k, r_w FROM (SELECT l.v, l.k AS l_k, r.w AS r_w FROM l LEFT JOIN r ON l.k = r.k) ORDER BY v;

SELECT '--- hash RIGHT ---';
SELECT v, l_k, r_k, w FROM (SELECT l.v, l.k AS l_k, r.k AS r_k, r.w AS w FROM l RIGHT JOIN r ON l.k = r.k) ORDER BY w;

SELECT '--- hash FULL ---';
WITH x AS (SELECT l.v AS lv, l.k AS lk, r.k AS rk, r.w AS w FROM l FULL JOIN r ON l.k = r.k)
SELECT lv, lk, rk, w FROM x ORDER BY lv, rk;

SELECT '--- hash SEMI ---';
SELECT v FROM (SELECT l.v FROM l SEMI JOIN r ON l.k = r.k) ORDER BY v;

SELECT '--- hash ANTI ---';
SELECT v FROM (SELECT l.v FROM l ANTI JOIN r ON l.k = r.k) ORDER BY v;

-- Repeat the core kinds with `full_sorting_merge`. The fix also extends the cursor's
-- per-key null map with `NaN` positions (`MergeJoinTransform.cpp`).
SET join_algorithm = 'full_sorting_merge';

SELECT '--- full_sorting_merge INNER ---';
SELECT v, k FROM (SELECT l.v, l.k FROM l INNER JOIN r ON l.k = r.k) ORDER BY v;

SELECT '--- full_sorting_merge LEFT ---';
SELECT v, l_k FROM (SELECT l.v, l.k AS l_k FROM l LEFT JOIN r ON l.k = r.k) ORDER BY v;

SELECT '--- full_sorting_merge FULL ---';
WITH x AS (SELECT l.v AS lv, l.k AS lk, r.k AS rk FROM l FULL JOIN r ON l.k = r.k)
SELECT lv, lk, rk FROM x ORDER BY lv, rk;

SET join_algorithm = 'default';

-- `Float32`, `Nullable(Float64)` regression coverage. The fix path also covers
-- `Float32` and the inner column behind a `Nullable(...)` wrapper.
DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l32 (k Float32, v String) ENGINE = Memory();
CREATE TABLE r32 (k Float32) ENGINE = Memory();
INSERT INTO l32 VALUES (1.5, 'm'), (nan, 'nan-l'), (-nan, 'neg-nan-l');
INSERT INTO r32 VALUES (1.5), (nan), (-nan);

SELECT '--- Float32 INNER ---';
SELECT v FROM (SELECT l32.v FROM l32 INNER JOIN r32 ON l32.k = r32.k) ORDER BY v;

CREATE TABLE ln (k Nullable(Float64), v String) ENGINE = Memory();
CREATE TABLE rn (k Nullable(Float64)) ENGINE = Memory();
INSERT INTO ln VALUES (1.5, 'm'), (nan, 'nan-l'), (NULL, 'null-l');
INSERT INTO rn VALUES (1.5), (nan), (NULL);

SELECT '--- Nullable(Float64) INNER ---';
SELECT v FROM (SELECT ln.v FROM ln INNER JOIN rn ON ln.k = rn.k) ORDER BY v;
SELECT '--- Nullable(Float64) ANTI ---';
SELECT v FROM (SELECT ln.v FROM ln ANTI JOIN rn ON ln.k = rn.k) ORDER BY v;

DROP TABLE IF EXISTS l32;
DROP TABLE IF EXISTS r32;
DROP TABLE IF EXISTS ln;
DROP TABLE IF EXISTS rn;

-- `GROUP BY` and `DISTINCT` on `NaN` must remain unchanged (they intentionally
-- group all NaNs together, unlike `JOIN ON`).
SELECT '--- GROUP BY nan ---';
SELECT k, count() FROM (SELECT nan AS k UNION ALL SELECT nan AS k UNION ALL SELECT 1.5 AS k) GROUP BY k ORDER BY k;

SELECT '--- DISTINCT nan ---';
SELECT k FROM (SELECT DISTINCT k FROM (SELECT nan AS k UNION ALL SELECT nan AS k)) ORDER BY k;
