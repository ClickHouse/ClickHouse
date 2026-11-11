-- Regression test for SIGABRT crash when using aggregate functions with
-- internal-only Null combinator and aggregate_functions_null_for_empty = 1.
-- The crash was caused by dereferencing an empty std::optional from
-- tryGetProperties() without checking has_value() first.

SELECT sumNull(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT avgNull(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT countNull(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
SELECT minNull(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

-- Normal aggregates with the setting should still work correctly
SELECT sum(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT sum(number) FROM numbers(10) WHERE 0 SETTINGS aggregate_functions_null_for_empty = 1;
