-- Regression test for AST fuzzer:
-- `Assertion right_argument.type->isNullable()` failed (STID: 2735-3ac4 / 2735-356b).
--
-- With the framework allowing non-Nullable result types for Nullable inputs (via `makeNullableSafe`),
-- `intDiv(Array(N), Nullable(N))` now reaches the executor. `FunctionBinaryArithmetic` had a
-- `division_by_nullable` short-circuit that asserted the right argument was still Nullable,
-- but it could be re-entered from `executeArrayWithNumericImpl` with already-stripped element types.

-- Non-null Nullable denominator: must compute without tripping the assertion.
SELECT intDiv([10, 20, 30], CAST(2, 'Nullable(UInt32)')) FORMAT Null;
SELECT modulo([10, 20, 30], CAST(7, 'Nullable(UInt32)')) FORMAT Null;
SELECT 'ok';
