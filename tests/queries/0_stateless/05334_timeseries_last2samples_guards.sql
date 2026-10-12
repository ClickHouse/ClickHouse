-- Test guard paths in the timeSeriesLastTwoSamples aggregate function factory.
--
-- AggregateFunctionLast2Samples.cpp (src/AggregateFunctions/TimeSeries/AggregateFunctionLast2Samples.cpp:85-88):
-- createAggregateFunctionLast2Samples checks the enable_time_series_aggregate_functions
-- setting at query-analysis time and throws UNKNOWN_AGGREGATE_FUNCTION when the
-- feature is disabled (setting == 0). The existing tests (05141, 05142) always
-- enable the feature with SET enable_time_series_aggregate_functions = 1, so
-- this disabled-path throw has never been reached in CI. Without it, a user
-- who calls the function without enabling the preview would see an unexpected
-- "function not found" error instead of the actionable "enable with setting X" message.
--
-- AggregateFunctionLast2Samples.cpp (lines 32-33):
-- createWithValueType throws NUMBER_OF_ARGUMENTS_DOESNT_MATCH when extra
-- parametric arguments are passed (e.g. timeSeriesLastTwoSamples(1)(...)).
-- The function takes no parameters; this guard is never tested.

-- Disabled-feature guard (lines 85-88): calling with default setting (=0) must
-- throw UNKNOWN_AGGREGATE_FUNCTION with an informative message.
SELECT timeSeriesLastTwoSamples(toDateTime('2024-01-15 12:00:00'), toFloat64(1.0)) FROM numbers(1); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

-- Same with explicit setting = 0
SELECT timeSeriesLastTwoSamples(toDateTime('2024-01-15 12:00:00'), toFloat64(1.0)) FROM numbers(1)
SETTINGS enable_time_series_aggregate_functions = 0; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

-- Parameters-not-accepted guard (lines 32-33): passing a parametric argument
-- must throw NUMBER_OF_ARGUMENTS_DOESNT_MATCH.
SELECT timeSeriesLastTwoSamples(1)(toDateTime('2024-01-15 12:00:00'), toFloat64(1.0)) FROM numbers(1)
SETTINGS enable_time_series_aggregate_functions = 1; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
