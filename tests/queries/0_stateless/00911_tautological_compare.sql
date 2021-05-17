-- TODO: Tautological optimization breaks JIT expression compilation, because it can return constant result
-- for non constant columns. And then sample blocks from same ActionsDAGs can be mismatched.
-- This optimization cannot be performed on AST rewrite level, because we does not have information about types
-- and equals(tuple(NULL), tuple(NULL)) have same hash code, but should not be optimized.
-- Return this test after refactoring of InterpreterSelectQuery.

-- SELECT count() FROM system.numbers WHERE number != number;
-- SELECT count() FROM system.numbers WHERE number < number;
-- SELECT count() FROM system.numbers WHERE number > number;

-- SELECT count() FROM system.numbers WHERE NOT (number = number);
-- SELECT count() FROM system.numbers WHERE NOT (number <= number);
-- SELECT count() FROM system.numbers WHERE NOT (number >= number);

-- SELECT count() FROM system.numbers WHERE SHA256(toString(number)) != SHA256(toString(number));
-- SELECT count() FROM system.numbers WHERE SHA256(toString(number)) != SHA256(toString(number)) AND rand() > 10;
