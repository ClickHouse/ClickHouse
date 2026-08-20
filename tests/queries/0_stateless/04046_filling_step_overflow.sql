-- Verify that extreme WITH FILL step values don't cause signed integer overflow (UBSan)
SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 MONTH; -- { serverError INVALID_WITH_FILL_EXPRESSION }
