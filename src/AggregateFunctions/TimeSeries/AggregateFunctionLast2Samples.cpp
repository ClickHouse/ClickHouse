#include <AggregateFunctions/TimeSeries/AggregateFunctionLast2Samples.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/IDataType.h>
#include <Core/Settings.h>


namespace DB
{
struct Settings;
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}
namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
}
namespace
{

template <typename ValueType>
AggregateFunctionPtr createWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    const auto & timestamp_type = argument_types[0];

    if (!parameters.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Aggregate function {} does not accept parameters", name);

    AggregateFunctionPtr res;
    if (isDateTime64(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<DateTime64, ValueType>>(argument_types);
    }
    if (isUInt64(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt64, ValueType>>(argument_types);
    }
    if (isInt64(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int64, ValueType>>(argument_types);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt32, ValueType>>(argument_types);
    }
    else if (isInt32(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int32, ValueType>>(argument_types);
    }
    else if (isUInt16(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt16, ValueType>>(argument_types);
    }
    else if (isInt16(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int16, ValueType>>(argument_types);
    }
    else if (isUInt8(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt8, ValueType>>(argument_types);
    }
    else if (isInt8(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int8, ValueType>>(argument_types);
    }

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
            timestamp_type->getName(), name);

    return res;
}

AggregateFunctionPtr createAggregateFunctionLast2Samples(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
            name);

    assertBinary(name, argument_types);
    const auto & value_type = argument_types[1];

    AggregateFunctionPtr res;
    if (value_type->getTypeId() == TypeIndex::Float64)
    {
        res = createWithValueType<Float64>(name, argument_types, parameters);
    }
    else if (value_type->getTypeId() == TypeIndex::Float32)
    {
        res = createWithValueType<Float32>(name, argument_types, parameters);
    }
    else
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}",
            value_type->getName(), name);
    }

    return res;
}

}

void registerAggregateFunctionLast2Samples(AggregateFunctionFactory & factory)
{
    /// timeSeriesLastTwoSamples documentation
    FunctionDocumentation::Description description_timeSeriesLastTwoSamples = R"(
Aggregate function for re-sampling time series data for PromQL-like irate and idelta calculation.

Aggregate function that takes time series data as pairs of timestamps and values and stores only at most 2 recent samples. This aggregate function is intended to be used with a Materialized View and Aggregated table that stores re-sampled time series data for grid-aligned timestamps.

The aggregated table stores only last 2 values for each aligned timestamp. This allows to calculate PromQL-like `irate` and `idelta` by reading much less data then is stored in the raw table.

:::warning
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesLastTwoSamples = R"(
timeSeriesLastTwoSamples(timestamp, value)
    )";
    FunctionDocumentation::Arguments arguments_timeSeriesLastTwoSamples = {
        {"timestamp", "Timestamp of the sample.", {"DateTime", "DateTime64", "(U)Int*", "Int*"}},
        {"value", "Value of the time series corresponding to the timestamp.", {"Float32", "Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesLastTwoSamples = {"Returns a pair of arrays of equal length from 0 to 2. The first array contains the timestamps of sampled time series, the second array contains the corresponding values of the time series.", {"Tuple(Array(DateTime), Array(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesLastTwoSamples = {
    {
        "Example table for raw data, and a table for storing re-sampled data",
        R"(
-- Table for raw data
CREATE TABLE t_raw_timeseries
(
    metric_id UInt64,
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD),
    value Float64 CODEC(DoubleDelta)
)
ENGINE = MergeTree()
ORDER BY (metric_id, timestamp);

-- Table with data re-sampled to bigger (15 sec) time steps
CREATE TABLE t_resampled_timeseries_15_sec
(
    metric_id UInt64,
    grid_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD), -- Timestamp aligned to 15 sec
    samples AggregateFunction(timeSeriesLastTwoSamples, DateTime64(3, 'UTC'), Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (metric_id, grid_timestamp);

-- MV for populating re-sampled table
CREATE MATERIALIZED VIEW mv_resampled_timeseries TO t_resampled_timeseries_15_sec
(
    metric_id UInt64,
    grid_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD),
    samples AggregateFunction(timeSeriesLastTwoSamples, DateTime64(3, 'UTC'), Float64)
)
AS SELECT
    metric_id,
    ceil(toUnixTimestamp(timestamp + interval 999 millisecond) / 15, 0) * 15 AS grid_timestamp, -- Round timestamp up to the next grid point
    initializeAggregation('timeSeriesLastTwoSamplesState', timestamp, value) AS samples
FROM t_raw_timeseries
ORDER BY metric_id, grid_timestamp;

-- Insert some data
INSERT INTO t_raw_timeseries(metric_id, timestamp, value) SELECT number%10 AS metric_id, '2024-12-12 12:00:00'::DateTime64(3, 'UTC') + interval ((number/10)%100)*900 millisecond as timestamp, number%3+number%29 AS value FROM numbers(1000);

-- Check raw data
SELECT *
FROM t_raw_timeseries
WHERE metric_id = 3 AND timestamp BETWEEN '2024-12-12 12:00:12' AND '2024-12-12 12:00:31'
ORDER BY metric_id, timestamp;
        )",
        R"(
3    2024-12-12 12:00:12.870    29
3    2024-12-12 12:00:13.770    8
3    2024-12-12 12:00:14.670    19
3    2024-12-12 12:00:15.570    30
3    2024-12-12 12:00:16.470    9
3    2024-12-12 12:00:17.370    20
3    2024-12-12 12:00:18.270    2
3    2024-12-12 12:00:19.170    10
3    2024-12-12 12:00:20.070    21
3    2024-12-12 12:00:20.970    3
3    2024-12-12 12:00:21.870    11
3    2024-12-12 12:00:22.770    22
3    2024-12-12 12:00:23.670    4
3    2024-12-12 12:00:24.570    12
3    2024-12-12 12:00:25.470    23
3    2024-12-12 12:00:26.370    5
3    2024-12-12 12:00:27.270    13
3    2024-12-12 12:00:28.170    24
3    2024-12-12 12:00:29.069    6
3    2024-12-12 12:00:29.969    14
3    2024-12-12 12:00:30.869    25
        )"
    },
    {
        "Query the last 2 sample for timestamps '2024-12-12 12:00:15' and '2024-12-12 12:00:30'",
        R"(
-- Check re-sampled data
SELECT metric_id, grid_timestamp, (finalizeAggregation(samples).1 as timestamp, finalizeAggregation(samples).2 as value)
FROM t_resampled_timeseries_15_sec
WHERE metric_id = 3 AND grid_timestamp BETWEEN '2024-12-12 12:00:15' AND '2024-12-12 12:00:30'
ORDER BY metric_id, grid_timestamp;
        )",
        R"(
3    2024-12-12 12:00:15    (['2024-12-12 12:00:14.670','2024-12-12 12:00:13.770'],[19,8])
3    2024-12-12 12:00:30    (['2024-12-12 12:00:29.969','2024-12-12 12:00:29.069'],[14,6])
        )"
    },
    {
        "Calculate idelta and irate from the raw data",
        R"(
-- The aggregated table stores only last 2 values for each 15-second aligned timestamp.
-- This allows to calculate PromQL-like irate and idelta by reading much less data then is stored in the raw table.

WITH
    '2024-12-12 12:00:15'::DateTime64(3,'UTC') AS start_ts,       -- start of timestamp grid
    start_ts + INTERVAL 60 SECOND AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT
    metric_id,
    timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value),
    timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM t_raw_timeseries
WHERE metric_id = 3 AND timestamp BETWEEN start_ts - interval window_seconds seconds AND end_ts
GROUP BY metric_id;
        )",
        R"(
3    [11,8,-18,8,11]    [12.222222222222221,8.88888888888889,1.1111111111111112,8.88888888888889,12.222222222222221]
        )"
    },
    {
        "Calculate idelta and irate from the re-sampled data",
        R"(
WITH
    '2024-12-12 12:00:15'::DateTime64(3,'UTC') AS start_ts,       -- start of timestamp grid
    start_ts + INTERVAL 60 SECOND AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT
    metric_id,
    timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values),
    timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)
FROM (
    SELECT
        metric_id,
        finalizeAggregation(samples).1 AS timestamps,
        finalizeAggregation(samples).2 AS values
    FROM t_resampled_timeseries_15_sec
    WHERE metric_id = 3 AND grid_timestamp BETWEEN start_ts - interval window_seconds seconds AND end_ts
)
GROUP BY metric_id;
        )",
        R"(
3    [11,8,-18,8,11]    [12.222222222222221,8.88888888888889,1.1111111111111112,8.88888888888889,12.222222222222221]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesLastTwoSamples = {25, 6};
    FunctionDocumentation::Category category_timeSeriesLastTwoSamples = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesLastTwoSamples = {description_timeSeriesLastTwoSamples, syntax_timeSeriesLastTwoSamples, arguments_timeSeriesLastTwoSamples, {}, returned_value_timeSeriesLastTwoSamples, examples_timeSeriesLastTwoSamples, introduced_in_timeSeriesLastTwoSamples, category_timeSeriesLastTwoSamples};

    factory.registerFunction("timeSeriesLastTwoSamples", {createAggregateFunctionLast2Samples, documentation_timeSeriesLastTwoSamples});
}

}
