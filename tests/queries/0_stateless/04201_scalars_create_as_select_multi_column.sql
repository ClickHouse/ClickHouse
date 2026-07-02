-- Test: exercises only_analyze branch of `evaluateScalarSubqueryIfNeeded` for MULTI-COLUMN scalar
-- subquery in CREATE TABLE/VIEW AS WITH context. The PR's own test (03032) covers single-column
-- Array result; this test covers the tuple-creation path inside `wrap_with_nullable_or_tuple`
-- when the sample block has > 1 columns during analyzer-only evaluation.
-- Covers: src/Analyzer/Resolve/evaluateScalarSubqueryIfNeeded.cpp lines 195-209 (only_analyze
-- branch) and the multi-column branch at lines 180-192 of `wrap_with_nullable_or_tuple`.

DROP TABLE IF EXISTS t_multi_scalar;
DROP VIEW IF EXISTS v_multi_scalar;

-- CREATE TABLE AS WITH (multi-column scalar) — getSampleBlock runs with only_analyze=true.
-- The inner `(SELECT 1 AS a, 2 AS b)` is evaluated in only_analyze mode: sample block has 2
-- columns, insertDefault populates them, then wrap_with_nullable_or_tuple goes to the multi-
-- column branch and produces Tuple(a UInt8, b UInt8). The `_analyze` cache suffix prevents the
-- placeholder (0,0) from polluting the real-execution cache; real SELECT then yields (1,2).
CREATE TABLE t_multi_scalar ENGINE = Memory AS
    WITH (SELECT 1 AS a, 2 AS b) AS x SELECT x AS y;

SELECT y, y.1 AS y_a, y.2 AS y_b FROM t_multi_scalar ORDER BY y;

-- Same shape but with VIEW (also goes through getSampleBlock with only_analyze=true).
CREATE VIEW v_multi_scalar AS
    WITH (SELECT 10 AS a, 20 AS b) AS x SELECT x AS y;

SELECT y, y.1 AS y_a, y.2 AS y_b FROM v_multi_scalar ORDER BY y;

DROP VIEW v_multi_scalar;
DROP TABLE t_multi_scalar;
