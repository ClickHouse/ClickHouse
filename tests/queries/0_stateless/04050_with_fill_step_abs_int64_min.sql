-- Regression test: std::abs(INT64_MIN) is UB, caught by UBSan/MSan.
-- For SECOND with toAvgSeconds()==1, mulOverflow(INT64_MIN, 1) does not fire,
-- leaving avg_seconds = INT64_MIN and std::abs(INT64_MIN) as UB.
-- https://github.com/ClickHouse/ClickHouse/issues/100087

-- Negative step with DESC is valid; INT64_MIN seconds is a huge step (>= 1 day), so no error.
SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 SECOND;
SELECT toDate32('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 SECOND;

-- Positive step with DESC is invalid.
SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL 9223372036854775807 SECOND; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT toDate32('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL 9223372036854775807 SECOND; -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- Small step (< 1 day) on Date is invalid.
SELECT toDate('2020-01-01') AS d ORDER BY d WITH FILL STEP INTERVAL 3600 SECOND; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -3600 SECOND; -- { serverError INVALID_WITH_FILL_EXPRESSION }
