-- https://github.com/ClickHouse/ClickHouse/issues/106956

SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION ALL
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION DISTINCT
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT t2.a, CAST(t1.b = t2.b, 'Nullable(UInt8)')
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION ALL
SELECT t2.a, CAST(t1.b = t2.b, 'Nullable(UInt8)')
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT DISTINCT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
UNION ALL
SELECT DISTINCT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

-- The same divergence reaches IntersectOrExceptStep.
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
INTERSECT
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE t1.b = t2.b
EXCEPT
SELECT t2.a, t1.b = t2.b
FROM (SELECT 1 AS a, 2 AS b) AS t1
INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
WHERE NOT (t1.b = t2.b);

-- Both branches keep rows but fold the same column to different constants (1 and 0).
SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    UNION ALL
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    UNION DISTINCT
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    INTERSECT
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

SELECT a, c FROM (
    SELECT t2.a AS a, (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
    EXCEPT
    SELECT t2.a AS a, NOT (t1.b = t2.b) AS c
    FROM (SELECT 1 AS a, 2 AS b) AS t1
    INNER JOIN (SELECT 1 AS a, 2 AS b) AS t2 ON t1.a = t2.a
    WHERE t1.b = t2.b
) ORDER BY a, c;

-- https://github.com/ClickHouse/ClickHouse/issues/107951 (site 11). A second path to the same
-- IntersectOrExceptStep divergence: one branch folds a column to Const(Nullable(Nothing)) (here via
-- QUALIFY + LIMIT 0 over a const-NULL CTE) while the sibling keeps the full Nullable(Nothing) column.
-- QUALIFY is only implemented in the Analyzer, so pin it (CI randomizes enable_analyzer).
WITH cte AS (SELECT DISTINCT NULL WHERE isNullable('') GROUP BY 1)
SELECT DISTINCT *, toNullable(NULL) FROM cte QUALIFY materialize(100) LIMIT 0
INTERSECT DISTINCT
SELECT toNullable(NULL), * FROM cte
SETTINGS enable_analyzer = 1;

-- Filter push-down rebuilds the UnionStep assuming it forwards every branch header
-- unchanged. With a divergent Const across shards (each remote shard folds randConstant()
-- to a different value, so the union materializes the column) a pushed-down branch filter
-- would still output Const and the mismatch would move into the filter-pushdown rewrite.
-- count() keeps the output deterministic (randConstant() varies per shard).
SELECT count() FROM (
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
    UNION ALL
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
) WHERE c >= 0
SETTINGS enable_analyzer = 0;

-- Parent set-operation interpreters under the old analyzer cache result_header from the child
-- sample blocks before any child plan is built. A child whose plan materializes the constant
-- (the remote(...) union above) must still pass the parent's "Conversion before UNION" cleanly:
-- branch headers only diverge during plan optimization, which runs after the parent interpreters
-- have decided their conversions, so no full-vs-Const conversion is requested at plan-build time.
SELECT count() FROM (
    SELECT serverUUID() AS c FROM remote('127.0.0.{1,2}', system.one)
    UNION ALL
    SELECT serverUUID() AS c FROM remote('127.0.0.{1,2}', system.one)
) SETTINGS enable_analyzer = 0;

SELECT count() FROM (
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
    UNION ALL
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
) SETTINGS enable_analyzer = 0;

-- The value of these set operations depends on random constants colliding or not, so only
-- assert that they execute (the plan-time header checks are what used to throw).
SELECT count() >= 0 FROM (
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
    INTERSECT ALL
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
) SETTINGS enable_analyzer = 0;

SELECT count() >= 0 FROM (
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
    EXCEPT ALL
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
) SETTINGS enable_analyzer = 0;

SELECT count() >= 1 FROM (
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
    UNION DISTINCT
    SELECT toUInt32(42) AS c FROM remote('127.0.0.{1,2}', system.one)
    UNION ALL
    SELECT randConstant() AS c FROM remote('127.0.0.{1,2}', system.one)
) SETTINGS enable_analyzer = 0;

-- AST fuzzer (STID 0993-2a62). Same Const-vs-full divergence, but the branch pipelines carry a
-- totals port (WITH TOTALS in the CTE). IntersectOrExceptStep::updatePipeline must apply the
-- converting expression whenever the branch header differs from the output header in constness
-- (strict blocksHaveEqualStructure, not isCompatibleHeader): addSimpleTransform converts the
-- totals port too, while the main-stream processors below it do not, so skipping the conversion
-- left a Const totals port next to materialized full main streams and the per-stream structure
-- check in a downstream DistinctStep threw a logical error.
WITH cte AS (SELECT DISTINCT NULL WHERE isNullable('') GROUP BY 1 WITH TOTALS)
SELECT DISTINCT *, toNullable(NULL) FROM cte QUALIFY materialize(100) LIMIT 0
INTERSECT DISTINCT
SELECT *, NULL FROM cte GROUP BY ALL
SETTINGS enable_analyzer = 1;

WITH cte AS (SELECT DISTINCT NULL WHERE isNullable('') GROUP BY 1 WITH TOTALS)
SELECT DISTINCT *, toNullable(NULL) FROM cte QUALIFY materialize(100) LIMIT 0
EXCEPT DISTINCT
SELECT *, NULL FROM cte GROUP BY ALL
SETTINGS enable_analyzer = 1;
