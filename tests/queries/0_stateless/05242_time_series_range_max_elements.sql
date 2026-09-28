-- timeSeriesRange built its result point by point without a bound on the number of points, so a huge range (here a
-- start timestamp of 1970 from a failed JSONExtract and a sub-second step) ran for over a minute under sanitizers
-- before the memory limit stopped it. It is now bounded like range() by function_range_max_elements_in_block.
-- Found by json_ast_sql_execution_fuzzer.
SELECT length(timeSeriesRange(JSONExtract('2025-06-01 00:00:00.0', 'DateTime64(1)'), CAST('2025-06-01 00:00:01.00', 'DateTime64(2)'), CAST('0.123', 'Decimal64(3)'))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT length(timeSeriesRange(toDateTime64(0, 1, 'UTC'), toDateTime64('2025-06-01 00:00:01', 1, 'UTC'), toDecimal64(1.23, 2))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT length(timeSeriesRange(toDateTime64(0, 1, 'UTC'), toDateTime64('2025-06-01 00:00:01', 1, 'UTC'), toDecimal64(12.3, 1))) SETTINGS function_range_max_elements_in_block = 100000000; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:10', 'UTC'), 1));
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:10', 'UTC'), 1)) SETTINGS function_range_max_elements_in_block = 10; -- { serverError ARGUMENT_OUT_OF_BOUND }
-- The bound is over the whole block.
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:03', 'UTC'), 1)) FROM numbers(3) SETTINGS function_range_max_elements_in_block = 11; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The bound is cheap to test on a small range: 11 points pass with the bound at 11 and fail at 10.
SELECT length(timeSeriesRange(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:10', 'UTC'), 1)) SETTINGS function_range_max_elements_in_block = 11;
-- The accumulation over the block is checked for overflow (a row of 1 point plus a row of 2^64 - 1 points wrapped to 0).
SELECT length(timeSeriesRange(
    if(number = 0, fromUnixTimestamp64Nano(0, 'UTC'), fromUnixTimestamp64Nano(-9223372036854775808, 'UTC')),
    if(number = 0, fromUnixTimestamp64Nano(0, 'UTC'), fromUnixTimestamp64Nano(9223372036854775806, 'UTC')),
    CAST('0.000000001' AS Decimal64(9))))
FROM numbers(2); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- timeSeriesFromGrid skips NULL values, and only the appended points count against the bound.
SELECT timeSeriesFromGrid(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:04', 'UTC'), 1, [NULL, NULL, NULL, NULL, NULL]) SETTINGS function_range_max_elements_in_block = 2;
SELECT timeSeriesFromGrid(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:04', 'UTC'), 1, [1., NULL, 3., NULL, 5.]) SETTINGS function_range_max_elements_in_block = 3;
SELECT timeSeriesFromGrid(toDateTime('2025-06-01 00:00:00', 'UTC'), toDateTime('2025-06-01 00:00:04', 'UTC'), 1, [1., NULL, 3., NULL, 5.]) SETTINGS function_range_max_elements_in_block = 2; -- { serverError ARGUMENT_OUT_OF_BOUND }
