-- Constant right-hand sides of `IN` that contain lambda-bound identifiers.
-- Before the row-wise rewrites for non-constant right-hand sides, such queries failed
-- in the old analyzer with BAD_ARGUMENTS ("not a constant expression"), because a lambda
-- cannot be folded by the constant-set path. Now they take the row-wise rewrite and agree
-- with the analyzer. Constant aliases are expanded by the query normalizer before the
-- rewrite is considered, so they keep the constant `Set` path, which canonicalizes negative
-- zero, so `-0.0 IN (0.0)` is 1, the same as the row-wise rewrite.

-- { echoOn }

SET enable_analyzer = 1;

SELECT toFloat64(-0.0) IN (arrayMap(x -> x, [toFloat64(0.0)]));
SELECT toFloat64(-0.0) IN (arraySum(x -> x, [toFloat64(0.0)]));
SELECT 3 IN (arrayMap(x -> x + 1, [1, 2]));
SELECT 5 IN (arrayMap(x -> x + 1, [1, 2]));
SELECT 3 NOT IN (arrayMap(x -> x + 1, [1, 2]));

WITH toFloat64(0.0) AS c SELECT toFloat64(-0.0) IN (c);
SELECT toFloat64(0.0) AS c, toFloat64(-0.0) IN (c);
