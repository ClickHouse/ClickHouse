-- A single CTE must be formatted with the same newline+indent as multiple CTEs (issue #44531).
SELECT formatQuery('WITH a AS (SELECT 100) SELECT a') FORMAT TSVRaw;
SELECT formatQuery('WITH a AS (SELECT 100), b AS (SELECT 200) SELECT a') FORMAT TSVRaw;
-- Scalar (non-subquery) CTE, single and multiple.
SELECT formatQuery('WITH 1 AS x SELECT x') FORMAT TSVRaw;
SELECT formatQuery('WITH 1 AS x, 2 AS y SELECT x + y') FORMAT TSVRaw;
-- The single-element SELECT list must NOT be pushed onto its own line.
SELECT formatQuery('SELECT 1') FORMAT TSVRaw;
