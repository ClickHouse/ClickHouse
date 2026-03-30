-- Regression test: setting count_distinct_implementation to an unknown function
-- name with aggregate_functions_null_for_empty caused a crash (optional deref on nullopt).
SELECT countDistinct(1) SETTINGS count_distinct_implementation = 'null', aggregate_functions_null_for_empty = 1; -- { serverError UNKNOWN_AGGREGATE_FUNCTION, UNKNOWN_FUNCTION }
