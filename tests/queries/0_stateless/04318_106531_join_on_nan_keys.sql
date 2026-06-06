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

-- `BFloat16` regression coverage. The fix dispatches on `BFloat16` in
-- `extendJoinKeyNullMapWithFloatNaNs` (`HashJoin`/`ConcurrentHashJoin`),
-- `foldFloatNaNsIntoNullMap` (`MergeJoinTransform`) and `filterOutNaNs`
-- (`BuildRuntimeFilterTransform`).
CREATE TABLE lbf (k BFloat16, v String) ENGINE = Memory();
CREATE TABLE rbf (k BFloat16) ENGINE = Memory();
INSERT INTO lbf VALUES (toBFloat16(1.5), 'm'), (toBFloat16(nan), 'nan-l'), (toBFloat16(-nan), 'neg-nan-l');
INSERT INTO rbf VALUES (toBFloat16(1.5)), (toBFloat16(nan)), (toBFloat16(-nan));

SELECT '--- BFloat16 hash INNER ---';
SELECT v FROM (SELECT lbf.v FROM lbf INNER JOIN rbf ON lbf.k = rbf.k) ORDER BY v;

SELECT '--- BFloat16 hash LEFT ---';
SELECT v FROM (SELECT lbf.v FROM lbf LEFT JOIN rbf ON lbf.k = rbf.k) ORDER BY v;

SELECT '--- BFloat16 hash ANTI ---';
SELECT v FROM (SELECT lbf.v FROM lbf ANTI JOIN rbf ON lbf.k = rbf.k) ORDER BY v;

SET join_algorithm = 'full_sorting_merge';

SELECT '--- BFloat16 full_sorting_merge INNER ---';
SELECT v FROM (SELECT lbf.v FROM lbf INNER JOIN rbf ON lbf.k = rbf.k) ORDER BY v;

SET join_algorithm = 'default';

DROP TABLE IF EXISTS lbf;
DROP TABLE IF EXISTS rbf;

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

-- Runtime-filter regression coverage for issue #106531. The runtime filter built
-- in `BuildRuntimeFilterTransform` was keyed by raw bytes, so a `NaN` row on the
-- probe side wrongly matched a `NaN` in the filter and either (a) let an INNER
-- probe through, or (b) excluded a probe `NaN` from a LEFT/ANTI result. The fix
-- recursively unwraps `Nullable`, `LowCardinality`, and `Tuple` wrappers around
-- the float key column and drops `NaN` rows before they are inserted.

DROP TABLE IF EXISTS lrf;
DROP TABLE IF EXISTS rrf;

-- `Nullable(Float64)` ANTI: probe-side `NaN` must remain in the result. Before the
-- runtime-filter fix, the build-side `NaN` from `rrf` would land in the exclusion
-- filter and pre-prune the probe-side `nan-l` row, so ANTI returned only `null-l`.
CREATE TABLE lrf (k Nullable(Float64), v String) ENGINE = Memory();
CREATE TABLE rrf (k Nullable(Float64)) ENGINE = Memory();
INSERT INTO lrf VALUES (1.5, 'm'), (nan, 'nan-l'), (NULL, 'null-l');
INSERT INTO rrf VALUES (1.5), (nan), (NULL);

SELECT '--- runtime-filter Nullable(Float64) ANTI ---';
SELECT v FROM (SELECT lrf.v FROM lrf ANTI JOIN rrf ON lrf.k = rrf.k)
ORDER BY v
SETTINGS enable_analyzer = 1, enable_join_runtime_filters = 1, join_algorithm = 'hash';

SELECT '--- runtime-filter Nullable(Float64) INNER ---';
SELECT v FROM (SELECT lrf.v FROM lrf INNER JOIN rrf ON lrf.k = rrf.k)
ORDER BY v
SETTINGS enable_analyzer = 1, enable_join_runtime_filters = 1, join_algorithm = 'hash';

DROP TABLE IF EXISTS lrf;
DROP TABLE IF EXISTS rrf;

-- `LowCardinality(Nullable(Float64))` ANTI: same shape with an extra LC wrapper.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE lrf_lc (k LowCardinality(Nullable(Float64)), v String) ENGINE = Memory();
CREATE TABLE rrf_lc (k LowCardinality(Nullable(Float64))) ENGINE = Memory();
INSERT INTO lrf_lc VALUES (1.5, 'm'), (nan, 'nan-l'), (NULL, 'null-l');
INSERT INTO rrf_lc VALUES (1.5), (nan), (NULL);

SELECT '--- runtime-filter LowCardinality(Nullable(Float64)) ANTI ---';
SELECT v FROM (SELECT lrf_lc.v FROM lrf_lc ANTI JOIN rrf_lc ON lrf_lc.k = rrf_lc.k)
ORDER BY v
SETTINGS enable_analyzer = 1, enable_join_runtime_filters = 1, join_algorithm = 'hash';

DROP TABLE IF EXISTS lrf_lc;
DROP TABLE IF EXISTS rrf_lc;

-- Multi-key LEFT ANTI: build path wraps both keys into one `Tuple` runtime-filter
-- key. A `NaN` in any tuple element must drop the row from the filter.
CREATE TABLE lrf_t (k1 Float64, k2 Float64, v String) ENGINE = Memory();
CREATE TABLE rrf_t (k1 Float64, k2 Float64) ENGINE = Memory();
INSERT INTO lrf_t VALUES (1.5, 2.5, 'm'), (nan, 2.5, 'nan-l'), (3.5, nan, 'nan-r-side'), (4.5, 5.5, 'unmatched');
INSERT INTO rrf_t VALUES (1.5, 2.5), (nan, 2.5), (3.5, nan);

SELECT '--- runtime-filter Tuple(Float64, Float64) LEFT ANTI ---';
SELECT v FROM (SELECT lrf_t.v FROM lrf_t LEFT ANTI JOIN rrf_t ON lrf_t.k1 = rrf_t.k1 AND lrf_t.k2 = rrf_t.k2)
ORDER BY v
SETTINGS enable_analyzer = 1, enable_join_runtime_filters = 1, join_algorithm = 'hash';

DROP TABLE IF EXISTS lrf_t;
DROP TABLE IF EXISTS rrf_t;

-- `GROUP BY` and `DISTINCT` on `NaN` must remain unchanged (they intentionally
-- group all NaNs together, unlike `JOIN ON`).
SELECT '--- GROUP BY nan ---';
SELECT k, count() FROM (SELECT nan AS k UNION ALL SELECT nan AS k UNION ALL SELECT 1.5 AS k) GROUP BY k ORDER BY k;

SELECT '--- DISTINCT nan ---';
SELECT k FROM (SELECT DISTINCT k FROM (SELECT nan AS k UNION ALL SELECT nan AS k)) ORDER BY k;
