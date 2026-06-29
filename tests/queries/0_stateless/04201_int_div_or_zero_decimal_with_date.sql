-- Test: exercises `intDivOrZero` and reverse-order `intDiv` with Decimal/Date arguments
-- Covers: src/Functions/FunctionBinaryArithmetic.h:189 — int_div_or_zero branch of the
--   IsDataTypeDecimalOrNumber<L> && IsDataTypeDecimalOrNumber<R> guard.
-- The PR's own test (03002_int_div_decimal_with_date_bug.sql) only exercises `intDiv`
-- with (Decimal, Date) ordering; `intDivOrZero` and reverse-order (Date, Decimal) are
-- never exercised even though the PR's predicate explicitly covers both functions and
-- both argument positions.

-- intDivOrZero with Decimal + Date/DateTime variants
SELECT intDivOrZero(CAST('1.0', 'Decimal256(3)'), today()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDivOrZero(CAST('1.0', 'Decimal256(3)'), toDate('2023-01-02')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDivOrZero(CAST('1.0', 'Decimal256(2)'), toDate32('2023-01-02 12:12:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDivOrZero(CAST('1.0', 'Decimal256(2)'), toDateTime('2023-01-02 12:12:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDivOrZero(CAST('1.0', 'Decimal256(2)'), toDateTime64('2023-01-02 12:12:12.002', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Reverse argument order: Date/DateTime + Decimal
SELECT intDiv(today(), CAST('1.0', 'Decimal256(3)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDiv(toDate('2023-01-02'), CAST('1.0', 'Decimal256(3)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDiv(toDateTime('2023-01-02 12:12:12'), CAST('1.0', 'Decimal256(2)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDivOrZero(toDate('2023-01-02'), CAST('1.0', 'Decimal256(3)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intDivOrZero(toDateTime64('2023-01-02 12:12:12.002', 3), CAST('1.0', 'Decimal256(2)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
