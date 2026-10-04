-- Regression test: a comparison function with the wrong number of arguments used as a
-- constraint expression must not crash the server. It used to trigger an out-of-bounds
-- access in `ComparisonGraph::normalizeAtom`, reached via `ConstraintsDescription::buildGraph`,
-- because the unanalyzed AST `less(a)` (a unary `less`) was treated as a binary comparison and
-- its second argument was accessed unconditionally.

DROP TABLE IF EXISTS t_constraint_arity;

CREATE TABLE t_constraint_arity (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;

-- Unary `less` in a CHECK constraint, added directly.
ALTER TABLE t_constraint_arity ADD CONSTRAINT c1 CHECK less(a);

-- The same via MODIFY CONSTRAINT (the path the fuzzer found).
ALTER TABLE t_constraint_arity ADD CONSTRAINT c2 CHECK a < b;
ALTER TABLE t_constraint_arity MODIFY CONSTRAINT c2 CHECK less(a);

-- Other wrong arities and relations must be handled the same way.
ALTER TABLE t_constraint_arity ADD CONSTRAINT c3 CHECK lessOrEquals(a);
ALTER TABLE t_constraint_arity ADD CONSTRAINT c4 CHECK greater(a, b, a);

SELECT 'ok';

DROP TABLE t_constraint_arity;
