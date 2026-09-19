-- timeSeriesRange built its result point by point without a bound on the number of points, so a huge range (here a
-- start timestamp of 1970 from a failed JSONExtract and a sub-second step) ran for over a minute under sanitizers
-- before the memory limit stopped it. It is now bounded like range() by function_range_max_elements_in_block.
-- Found by json_ast_sql_execution_fuzzer.
SELECT length(timeSeriesRange(JSONExtract('2025-06-01 00:00:00.0', 'DateTime64(1)'), CAST('2025-06-01 00:00:01.00', 'DateTime64(2)'), CAST('0.123', 'Decimal64(3)'))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT length(timeSeriesRange(toDateTime64(0, 1, 'UTC'), toDateTime64('2025-06-01 00:00:01', 1, 'UTC'), toDecimal64(1.23, 2))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT length(timeSeriesRange(toDateTime64(0, 1, 'UTC'), toDateTime64('2025-06-01 00:00:01', 1, 'UTC'), toDecimal64(12.3, 1))) SETTINGS function_range_max_elements_in_block = 100000000; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT length(timeSeriesRange(toDateTime64(0, 1, 'UTC'), toDateTime64('2025-06-01 00:00:01', 1, 'UTC'), toDecimal64(12.3, 1))) SETTINGS function_range_max_elements_in_block = 200000000;
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:10', 'UTC'), 1));
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:10', 'UTC'), 1)) SETTINGS function_range_max_elements_in_block = 10; -- { serverError ARGUMENT_OUT_OF_BOUND }
-- The bound is over the whole block.
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:03', 'UTC'), 1)) FROM numbers(3) SETTINGS function_range_max_elements_in_block = 11; -- { serverError ARGUMENT_OUT_OF_BOUND }
