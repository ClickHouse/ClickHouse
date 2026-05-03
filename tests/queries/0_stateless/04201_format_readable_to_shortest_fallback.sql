-- Test: exercises the `ToShortest` fallback path in `formatReadable` when `ToFixed` fails for very large values.
-- Covers: src/Common/formatReadable.cpp:30-31 — `if (!result) result = converter.ToShortest(size, &builder);`
-- Mutation guard: removing the fallback at line 31 makes these queries throw CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER.
-- For very large doubles, `ToFixed` overflows its digit limit and returns false; `ToShortest` then produces scientific notation.

SELECT formatReadableSize(1e308);
SELECT formatReadableSize(1e200);
SELECT formatReadableQuantity(1e200);
