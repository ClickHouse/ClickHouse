-- The result is unspecified but UBSan should not argue.
SELECT ignore(addHours(now64(3), inf)) FROM numbers(2); -- { serverError DECIMAL_OVERFLOW }
