-- Regression test: a comparison function with the wrong number of arguments used as a
-- constraint expression must not crash the server. It used to trigger an out-of-bounds
-- access in `ComparisonGraph::normalizeAtom`, reached via `ConstraintsDescription::buildGraph`,
-- because the unanalyzed AST `less(a)` (a unary `less`) was treated as a binary comparison and
-- its second argument was accessed unconditionally.

DROP TABLE IF EXISTS t_constraint_arity;

-- Unary `less` in a CHECK constraint at CREATE time.
CREATE TABLE t_constraint_arity (a UInt32, b UInt32, CONSTRAINT c0 CHECK less(a)) ENGINE = MergeTree ORDER BY a;

-- The same added via ALTER, as CHECK and as ASSUME.
ALTER TABLE t_constraint_arity ADD CONSTRAINT c1 CHECK less(a);
ALTER TABLE t_constraint_arity ADD CONSTRAINT c2 ASSUME less(a);

-- Other wrong arities and relations must be handled the same way.
ALTER TABLE t_constraint_arity ADD CONSTRAINT c3 CHECK lessOrEquals(a);
ALTER TABLE t_constraint_arity ADD CONSTRAINT c4 CHECK less();
ALTER TABLE t_constraint_arity ADD CONSTRAINT c5 CHECK greater(a, b, a);

SELECT 'ok';

DROP TABLE t_constraint_arity;
