-- `WITH CLUSTER` on a tuple is supported only for tuples of arity 2 (2D Euclidean).
-- Any other arity must be rejected.

-- Arity 3 — not supported.
SELECT count() FROM VALUES('x UInt64, y UInt64, z UInt64', (1, 1, 1))
GROUP BY (x, y, z) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

-- Arity 1 (single-element tuple) — not supported.
SELECT count() FROM VALUES('x UInt64', (1))
GROUP BY tuple(x) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
