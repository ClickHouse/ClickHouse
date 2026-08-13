-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100873
-- `executeArrayWithNumericImpl` passed the input element type as `result_type`
-- instead of the actual result element type. When all rows are NULL, the Nullable
-- default implementation short-circuits and creates a default column using the
-- (wrong) result_type, producing Float64 columns when Int64 was expected.

-- Original reproducer: intDiv on Array(Nullable(Tuple(Float64, Float64)))
SELECT intDiv([divide((1, 2), isZeroOrNull(isZeroOrNull(1)) AND NULL)], 2);

-- intDivOrZero: same type mismatch pattern
SELECT intDivOrZero([divide((1, 2), isZeroOrNull(isZeroOrNull(1)) AND NULL)], 2);

-- Non-NULL cases: these work because the tuple function ignores result_type
-- and computes correct column types via elem_func->getResultType().
-- They verify the fix doesn't break the non-NULL path.
SELECT intDiv([(1.5, 2.5)], 2);
SELECT intDiv([(1, 2)], 2);
SELECT intDivOrZero([(3.7, 4.2)], 3);

-- Other array-with-number operations (result type matches input for Float64,
-- so no mismatch even with the old bug, but verify they still work).
SELECT [(1.5, 2.5)] % 2;
SELECT [(6, 9)] / 3;
SELECT 2 * [(1.5, 2.5)];
SELECT [(1.5, 2.5)] * 3;
