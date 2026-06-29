-- Negative distances must be rejected explicitly, not silently reinterpreted
-- as `distance == 0` (which used to be the case for the numeric/2D paths).

-- 1D numeric.
SELECT count() FROM VALUES('x UInt64', (1), (2))
GROUP BY x WITH CLUSTER -1; -- { serverError BAD_ARGUMENTS }

-- 2D Euclidean.
SELECT count() FROM VALUES('x Float64, y Float64', (0.0, 0.0), (1.0, 1.0))
GROUP BY (x, y) WITH CLUSTER -1; -- { serverError BAD_ARGUMENTS }

-- 1D String (Levenshtein).
SELECT count() FROM VALUES('s String', ('abc'), ('abd'))
GROUP BY s WITH CLUSTER -1; -- { serverError BAD_ARGUMENTS }
