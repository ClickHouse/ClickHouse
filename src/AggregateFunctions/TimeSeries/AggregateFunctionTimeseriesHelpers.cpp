#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesInstantValue.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesToGridSparse.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesLinearRegression.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesChanges.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/readDecimalText.h>
#include <Core/Settings.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// Extracts integer or decimal parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 normalizeParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale)
{
    auto target_scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(target_scale);

    if (parameter_field.getType() == Field::Types::Decimal64)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal64>>();
        auto value_scale_multiplier = value.getScaleMultiplier();
        return (Decimal128(value.getValue()) * Decimal128(target_scale_multiplier)) / Decimal128(value_scale_multiplier);
    }
    else if (parameter_field.getType() == Field::Types::Decimal32)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal32>>();
        auto value_scale_multiplier = value.getScaleMultiplier();
        return Decimal64(value.getValue()) / value_scale_multiplier * target_scale_multiplier;
    }
    else if (Int64 int_value = 0; parameter_field.tryGet(int_value))
    {
        return Decimal64(int_value) * target_scale_multiplier;
    }
    else if (UInt64 uint_value = 0; parameter_field.tryGet(uint_value))
    {
        return Decimal64(uint_value) * target_scale_multiplier;
    }
    else if (String string_value; parameter_field.tryGet(string_value))
    {
        Decimal64 value{};
        UInt32 scale = target_scale;
        ReadBufferFromString buf(string_value);
        if (tryReadDecimalText(buf, value, 20, scale))
            return value * DecimalUtils::scaleMultiplier<Decimal64>(scale);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of {} parameter for aggregate function {}",
            parameter_field.getTypeName(), parameter_name, function_name);
    }
}

UInt64 extractIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field)
{
    if (UInt64 int_value = 0; parameter_field.tryGet(int_value))
    {
        return int_value;
    }
    else if (String string_value; parameter_field.tryGet(string_value))
    {
        UInt64 value{};
        ReadBufferFromString buf(string_value);
        if (tryReadIntText(value, buf))
            return value;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of {} parameter for aggregate function {}",
            parameter_field.getTypeName(), parameter_name, function_name);
    }
}

Float64 extractFloatParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field)
{
    if (Float64 float_value = 0; parameter_field.tryGet(float_value))
    {
        return float_value;
    }
    else if (Int64 int_value = 0; parameter_field.tryGet(int_value))
    {
        return static_cast<Float64>(int_value);
    }
    else if (UInt64 uint_value = 0; parameter_field.tryGet(uint_value))
    {
        return static_cast<Float64>(uint_value);
    }
    else if (String string_value; parameter_field.tryGet(string_value))
    {
        Float64 value{};
        ReadBufferFromString buf(string_value);
        if (tryReadFloatText(value, buf))
            return value;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of {} parameter for aggregate function {}",
            parameter_field.getTypeName(), parameter_name, function_name);
    }
}


namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}

namespace
{

template <
    bool is_rate_or_resets,
    bool is_predict,
    bool array_arguments,
    typename ValueType,
    template <bool, typename, typename, typename, bool> class FunctionTraits,
    template <typename> class Function
>
AggregateFunctionPtr createWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType() : argument_types[0];

    if (!is_predict && parameters.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Aggregate function {} requires 4 parameters: start_timestamp, end_timestamp, step, window", name);

    if (is_predict && parameters.size() != 5)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Aggregate function {} requires 5 parameters: start_timestamp, end_timestamp, step, window, predict_offset", name);

    const Field & start_timestamp_param = parameters[0];
    const Field & end_timestamp_param = parameters[1];
    const Field & step_param = parameters[2];
    const Field & window_param = parameters[3];
    const Field & predict_offset_param = is_predict ? parameters[4] : Field();

    AggregateFunctionPtr res;
    if (isDateTime64(timestamp_type))
    {
        /// Convert start, end, step and staleness parameters to the scale of the timestamp column
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        auto target_scale = timestamp_decimal->getScale();

        DateTime64 start_timestamp = normalizeParameter(name, "start", start_timestamp_param, target_scale);
        DateTime64 end_timestamp = normalizeParameter(name, "end", end_timestamp_param, target_scale);
        DateTime64 step = normalizeParameter(name, "step", step_param, target_scale);
        DateTime64 window = normalizeParameter(name, "window", window_param, target_scale);

        if constexpr (is_predict)
        {
            Float64 predict_offset = extractFloatParameter(name, "predict_offset", predict_offset_param) * static_cast<Float64>(DecimalUtils::scaleMultiplier<Int64>(target_scale));
            res = std::make_shared<Function<FunctionTraits<array_arguments, DateTime64, Int64, ValueType, is_predict>>>
                (argument_types, start_timestamp, end_timestamp, step, window, target_scale, predict_offset);
        }
        else
        {
            res = std::make_shared<Function<FunctionTraits<array_arguments, DateTime64, Int64, ValueType, is_rate_or_resets>>>
                (argument_types, start_timestamp, end_timestamp, step, window, target_scale);
        }
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        UInt64 start_timestamp = extractIntParameter(name, "start", start_timestamp_param);
        UInt64 end_timestamp = extractIntParameter(name, "end", end_timestamp_param);
        Int64 step = extractIntParameter(name, "step", step_param);
        Int64 window = extractIntParameter(name, "window", window_param);

        if constexpr (is_predict)
        {
            Float64 predict_offset = extractFloatParameter(name, "predict_offset", predict_offset_param);
            res = std::make_shared<Function<FunctionTraits<array_arguments, UInt32, Int32, ValueType, is_predict>>>
                (argument_types, start_timestamp, end_timestamp, step, window, 0, predict_offset);
        }
        else
        {
            res = std::make_shared<Function<FunctionTraits<array_arguments, UInt32, Int32, ValueType, is_rate_or_resets>>>
                (argument_types, start_timestamp, end_timestamp, step, window, 0);
        }
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                        timestamp_type->getName(), name);

    return res;
}

template <
    bool is_rate_or_resets,
    bool is_predict,
    template <bool, typename, typename, typename, bool> class FunctionTraits,
    template <typename> class Function
>
AggregateFunctionPtr createAggregateFunctionTimeseries(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
            name);

    assertBinary(name, argument_types);

    if ((argument_types[0]->getTypeId() == TypeIndex::Array) != (argument_types[1]->getTypeId() == TypeIndex::Array))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal combination of argument type {} and {} for aggregate function {}, expected both arguments to be arrays or not arrays",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

    const bool array_arguments = argument_types[1]->getTypeId() == TypeIndex::Array;
    const auto & value_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[1].get())->getNestedType() : argument_types[1];

    AggregateFunctionPtr res;
    if (value_type->getTypeId() == TypeIndex::Float64)
    {
        if (array_arguments)
            res = createWithValueType<is_rate_or_resets, is_predict, true, Float64, FunctionTraits, Function>(name, argument_types, parameters);
        else
            res = createWithValueType<is_rate_or_resets, is_predict, false, Float64, FunctionTraits, Function>(name, argument_types, parameters);
    }
    else if (value_type->getTypeId() == TypeIndex::Float32)
    {
        if (array_arguments)
            res = createWithValueType<is_rate_or_resets, is_predict, true, Float32, FunctionTraits, Function>(name, argument_types, parameters);
        else
            res = createWithValueType<is_rate_or_resets, is_predict, false, Float32, FunctionTraits, Function>(name, argument_types, parameters);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    return res;
}

}

void registerAggregateFunctionTimeseries(AggregateFunctionFactory & factory)
{
    /// timeSeriesRateToGrid documentation
    FunctionDocumentation::Description description_timeSeriesRateToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like rate](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `rate` are considered within the specified time window.

:::warning
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesRateToGrid = R"(
timeSeriesRateToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesRateToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesRateToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesRateToGrid = {"Returns rate values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the rate value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesRateToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,0.06666667,0.1,0.083333336,NULL,NULL,0.083333336]                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,0.06666667,0.1,0.083333336,NULL,NULL,0.083333336]                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesRateToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesRateToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesRateToGrid = {description_timeSeriesRateToGrid, syntax_timeSeriesRateToGrid, arguments_timeSeriesRateToGrid, parameters_timeSeriesRateToGrid, returned_value_timeSeriesRateToGrid, examples_timeSeriesRateToGrid, introduced_in_timeSeriesRateToGrid, category_timeSeriesRateToGrid};

    factory.registerFunction("timeSeriesRateToGrid",
        {createAggregateFunctionTimeseries<true, false, AggregateFunctionTimeseriesExtrapolatedValueTraits, AggregateFunctionTimeseriesExtrapolatedValue>, documentation_timeSeriesRateToGrid});

    /// timeSeriesDeltaToGrid documentation
    FunctionDocumentation::Description description_timeSeriesDeltaToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like delta](https://prometheus.io/docs/prometheus/latest/querying/functions/#delta) from this data on a regular time grid described by start timestamp, end timestamp and step.
For each point on the grid the samples for calculating `delta` are considered within the specified time window.

:::warning
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesDeltaToGrid = R"(
timeSeriesDeltaToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesDeltaToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesDeltaToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesDeltaToGrid = {"Returns delta values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the delta value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesDeltaToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,      -- start of timestamp grid
    90 + 120 AS end_ts,  -- end of timestamp grid
    15 AS step_seconds,  -- step of timestamp grid
    45 AS window_seconds -- "staleness" window
SELECT timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,3,4.5,3.75,NULL,NULL,3.75]                                               │
└───────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
-- it is possible to pass multiple samples of timestamps and values as Arrays of equal size
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,3,4.5,3.75,NULL,NULL,3.75]                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesDeltaToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesDeltaToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesDeltaToGrid = {description_timeSeriesDeltaToGrid, syntax_timeSeriesDeltaToGrid, arguments_timeSeriesDeltaToGrid, parameters_timeSeriesDeltaToGrid, returned_value_timeSeriesDeltaToGrid, examples_timeSeriesDeltaToGrid, introduced_in_timeSeriesDeltaToGrid, category_timeSeriesDeltaToGrid};

    factory.registerFunction("timeSeriesDeltaToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesExtrapolatedValueTraits, AggregateFunctionTimeseriesExtrapolatedValue>, documentation_timeSeriesDeltaToGrid});

    /// timeSeriesInstantRateToGrid documentation
    FunctionDocumentation::Description description_timeSeriesInstantRateToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like irate](https://prometheus.io/docs/prometheus/latest/querying/functions/#irate) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `irate` are considered within the specified time window.

:::warning
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesInstantRateToGrid = R"(
timeSeriesInstantRateToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesInstantRateToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesInstantRateToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesInstantRateToGrid = {"Returns irate values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the instant rate value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesInstantRateToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,0.2,0.1,0.1,NULL,NULL,0.3]                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
-- it is possible to pass multiple samples of timestamps and values as Arrays of equal size
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,0.2,0.1,0.1,NULL,NULL,0.3]                                                        │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesInstantRateToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesInstantRateToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesInstantRateToGrid = {description_timeSeriesInstantRateToGrid, syntax_timeSeriesInstantRateToGrid, arguments_timeSeriesInstantRateToGrid, parameters_timeSeriesInstantRateToGrid, returned_value_timeSeriesInstantRateToGrid, examples_timeSeriesInstantRateToGrid, introduced_in_timeSeriesInstantRateToGrid, category_timeSeriesInstantRateToGrid};

    factory.registerFunction("timeSeriesInstantRateToGrid",
        {createAggregateFunctionTimeseries<true, false, AggregateFunctionTimeseriesInstantValueTraits, AggregateFunctionTimeseriesInstantValue>, documentation_timeSeriesInstantRateToGrid});

    /// timeSeriesInstantDeltaToGrid documentation
    FunctionDocumentation::Description description_timeSeriesInstantDeltaToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like idelta](https://prometheus.io/docs/prometheus/latest/querying/functions/#idelta) from this data on a regular time grid described by start timestamp, end timestamp and step.
For each point on the grid the samples for calculating `idelta` are considered within the specified time window.

:::warning
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesInstantDeltaToGrid = R"(
timeSeriesInstantDeltaToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesInstantDeltaToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesInstantDeltaToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesInstantDeltaToGrid = {"Returns idelta values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the instant delta value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesInstantDeltaToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,2,1,1,NULL,NULL,3]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
-- it is possible to pass multiple samples of timestamps and values as Arrays of equal size
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,2,1,1,NULL,NULL,3]                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesInstantDeltaToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesInstantDeltaToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesInstantDeltaToGrid = {description_timeSeriesInstantDeltaToGrid, syntax_timeSeriesInstantDeltaToGrid, arguments_timeSeriesInstantDeltaToGrid, parameters_timeSeriesInstantDeltaToGrid, returned_value_timeSeriesInstantDeltaToGrid, examples_timeSeriesInstantDeltaToGrid, introduced_in_timeSeriesInstantDeltaToGrid, category_timeSeriesInstantDeltaToGrid};

    factory.registerFunction("timeSeriesInstantDeltaToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesInstantValueTraits, AggregateFunctionTimeseriesInstantValue>, documentation_timeSeriesInstantDeltaToGrid});

    /// timeSeriesDerivToGrid documentation
    FunctionDocumentation::Description description_timeSeriesDerivToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like derivative](https://prometheus.io/docs/prometheus/latest/querying/functions/#deriv) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `deriv` are considered within the specified time window.

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesDerivToGrid = R"(
timeSeriesDerivToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesDerivToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {}},
        {"end_timestamp", "Specifies end of the grid.", {}},
        {"grid_step", "Specifies step of the grid in seconds.", {}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesDerivToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesDerivToGrid = {"`deriv` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the derivative value for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesDerivToGrid = {
    {
        "Calculate derivative values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210]",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,0.1,0.11,0.15,NULL,NULL,0.15]                                              │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,0.1,0.11,0.15,NULL,NULL,0.15]                                                │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesDerivToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesDerivToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesDerivToGrid = {description_timeSeriesDerivToGrid, syntax_timeSeriesDerivToGrid, arguments_timeSeriesDerivToGrid, parameters_timeSeriesDerivToGrid, returned_value_timeSeriesDerivToGrid, examples_timeSeriesDerivToGrid, introduced_in_timeSeriesDerivToGrid, category_timeSeriesDerivToGrid};

    factory.registerFunction("timeSeriesDerivToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesLinearRegressionTraits, AggregateFunctionTimeseriesLinearRegression>, documentation_timeSeriesDerivToGrid});
    /// timeSeriesPredictLinearToGrid documentation
    FunctionDocumentation::Description description_timeSeriesPredictLinearToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates a [PromQL-like linear prediction](https://prometheus.io/docs/prometheus/latest/querying/functions/#predict_linear) with a specified prediction timestamp offset from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `predict_linear` are considered within the specified time window.

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesPredictLinearToGrid = R"(
timeSeriesPredictLinearToGrid(start_timestamp, end_timestamp, grid_step, staleness, predict_offset)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesPredictLinearToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {}},
        {"end_timestamp", "Specifies end of the grid.", {}},
        {"grid_step", "Specifies step of the grid in seconds.", {}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {}},
        {"predict_offset", "Specifies number of seconds of offset to add to prediction time.", {}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesPredictLinearToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesPredictLinearToGrid = {"`predict_linear` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the rate value for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesPredictLinearToGrid = {
    {
        "Calculate predict_linear values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210] with a 60 second offset",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds, -- "staleness" window
    60 AS predict_offset  -- prediction time offset
SELECT timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamp, value)─┐
│ [NULL,NULL,1,9.166667,11.6,16.916666,NULL,NULL,16.5]                                                            │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds,
    60 AS predict_offset
SELECT timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamps, values);
        )",
        R"(
┌─timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamp, value)─┐
│ [NULL,NULL,1,9.166667,11.6,16.916666,NULL,NULL,16.5]                                                            │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesPredictLinearToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesPredictLinearToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesPredictLinearToGrid = {description_timeSeriesPredictLinearToGrid, syntax_timeSeriesPredictLinearToGrid, arguments_timeSeriesPredictLinearToGrid, parameters_timeSeriesPredictLinearToGrid, returned_value_timeSeriesPredictLinearToGrid, examples_timeSeriesPredictLinearToGrid, introduced_in_timeSeriesPredictLinearToGrid, category_timeSeriesPredictLinearToGrid};

    factory.registerFunction("timeSeriesPredictLinearToGrid",
        {createAggregateFunctionTimeseries<false, true, AggregateFunctionTimeseriesLinearRegressionTraits, AggregateFunctionTimeseriesLinearRegression>, documentation_timeSeriesPredictLinearToGrid});

    /// timeSeriesChangesToGrid documentation
    FunctionDocumentation::Description description_timeSeriesChangesToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like changes](https://prometheus.io/docs/prometheus/latest/querying/functions/#changes) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `changes` are considered within the specified time window.

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesChangesToGrid = R"(
timeSeriesChangesToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesChangesToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {}},
        {"end_timestamp", "Specifies end of the grid.", {}},
        {"grid_step", "Specifies step of the grid in seconds.", {}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples.", {}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesChangesToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesChangesToGrid = {"`changes` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window to calculate the changes value for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesChangesToGrid = {
    {
        "Calculate changes values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210, 225]",
        R"(
WITH
    -- NOTE: the gap between 130 and 190 is to show how values are filled for ts = 180 according to window parameter
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 135 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesChangesToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesChangesToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,1,1,1,NULL,0,1,2]                                                            │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
WITH
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 135 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesChangesToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesChangesToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,1,1,1,NULL,0,1,2]                                                            │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesChangesToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesChangesToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesChangesToGrid = {description_timeSeriesChangesToGrid, syntax_timeSeriesChangesToGrid, arguments_timeSeriesChangesToGrid, parameters_timeSeriesChangesToGrid, returned_value_timeSeriesChangesToGrid, examples_timeSeriesChangesToGrid, introduced_in_timeSeriesChangesToGrid, category_timeSeriesChangesToGrid};

    factory.registerFunction("timeSeriesChangesToGrid",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesChangesTraits, AggregateFunctionTimeseriesChanges>, documentation_timeSeriesChangesToGrid});
    /// timeSeriesResetsToGrid documentation
    FunctionDocumentation::Description description_timeSeriesResetsToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like resets](https://prometheus.io/docs/prometheus/latest/querying/functions/#resets) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `resets` are considered within the specified time window.

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesResetsToGrid = R"(
timeSeriesResetsToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesResetsToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {}},
        {"end_timestamp", "Specifies end of the grid.", {}},
        {"grid_step", "Specifies step of the grid in seconds.", {}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples.", {}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesResetsToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesResetsToGrid = {"`resets` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window to calculate the resets value for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesResetsToGrid = {
    {
        "Calculate resets values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210, 225]",
        R"(
WITH
    -- NOTE: the gap between 130 and 190 is to show how values are filled for ts = 180 according to window parameter
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 135 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,1,1,1,NULL,0,1,2]                                                           │
└──────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
WITH
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 135 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,1,1,0,NULL,0,1,2]                                                           │
└──────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesResetsToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesResetsToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesResetsToGrid = {description_timeSeriesResetsToGrid, syntax_timeSeriesResetsToGrid, arguments_timeSeriesResetsToGrid, parameters_timeSeriesResetsToGrid, returned_value_timeSeriesResetsToGrid, examples_timeSeriesResetsToGrid, introduced_in_timeSeriesResetsToGrid, category_timeSeriesResetsToGrid};

    factory.registerFunction("timeSeriesResetsToGrid",
        {createAggregateFunctionTimeseries<true, false, AggregateFunctionTimeseriesChangesTraits, AggregateFunctionTimeseriesChanges>, documentation_timeSeriesResetsToGrid});

    /// timeSeriesResampleToGridWithStaleness documentation
    FunctionDocumentation::Description description_timeSeriesResampleToGridWithStaleness = R"(
Aggregate function that takes time series data as pairs of timestamps and values and re-samples this data to a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the most recent (within the specified time window) sample is chosen.

Alias: `timeSeriesLastToGrid`.

:::warning
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesResampleToGridWithStaleness = R"(
timeSeriesResampleToGridWithStaleness(start_timestamp, end_timestamp, grid_step, staleness_window)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesResampleToGridWithStaleness = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness_window", "Specifies the maximum staleness of the most recent sample in seconds.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesResampleToGridWithStaleness = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesResampleToGridWithStaleness = {"Returns time series values re-sampled to the specified grid. The returned array contains one value for each time grid point. The value is NULL if there is no sample for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesResampleToGridWithStaleness = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to staleness window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    30 AS window_seconds  -- "staleness" window
SELECT timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,1,3,4,4,NULL,5,8]                                                                           │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    30 AS window_seconds
SELECT timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,1,3,4,4,NULL,5,8]                                                                             │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesResampleToGridWithStaleness = {25, 6};
    FunctionDocumentation::Category category_timeSeriesResampleToGridWithStaleness = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesResampleToGridWithStaleness = {description_timeSeriesResampleToGridWithStaleness, syntax_timeSeriesResampleToGridWithStaleness, arguments_timeSeriesResampleToGridWithStaleness, parameters_timeSeriesResampleToGridWithStaleness, returned_value_timeSeriesResampleToGridWithStaleness, examples_timeSeriesResampleToGridWithStaleness, introduced_in_timeSeriesResampleToGridWithStaleness, category_timeSeriesResampleToGridWithStaleness};

    factory.registerFunction("timeSeriesResampleToGridWithStaleness",
        {createAggregateFunctionTimeseries<false, false, AggregateFunctionTimeseriesToGridSparseTraits, AggregateFunctionTimeseriesToGridSparse>, documentation_timeSeriesResampleToGridWithStaleness});
    factory.registerAlias("timeSeriesLastToGrid", "timeSeriesResampleToGridWithStaleness");
}

}
