-- Regression test for std::abs(INT64_MIN) UB in getStepFunction (WITH FILL STEP on Date/Date32).
-- MONTH is caught by mulOverflow (INT64_MIN * 2629746 overflows).
-- SECOND bypasses mulOverflow (INT64_MIN * 1 does not overflow), then std::abs(INT64_MIN) is UB.
-- On UBSan builds this triggers a sanitizer error.

-- MONTH: mulOverflow fires → expected error
SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 MONTH; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT toDate32('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 MONTH; -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- SECOND: step magnitude is enormous (>> 1 day), no fill rows produced
SELECT toDate('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 SECOND;
SELECT toDate32('2020-01-01') AS d ORDER BY d DESC WITH FILL STEP INTERVAL -9223372036854775808 SECOND;
