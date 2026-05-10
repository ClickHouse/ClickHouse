#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSimpleOverTime.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

Decimal64 normalizeParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);
UInt64 extractIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}

namespace
{

template <AggregateFunctionTimeseriesSimpleOverTimeKind kind, bool array_arguments, typename ValueType>
AggregateFunctionPtr createSimpleOverTimeWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType() : argument_types[0];

    if (parameters.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 4 parameters: start_timestamp, end_timestamp, step, window", name);

    const Field & start_timestamp_param = parameters[0];
    const Field & end_timestamp_param = parameters[1];
    const Field & step_param = parameters[2];
    const Field & window_param = parameters[3];

    AggregateFunctionPtr res;
    if (isDateTime64(timestamp_type))
    {
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        auto target_scale = timestamp_decimal->getScale();

        DateTime64 start_timestamp = normalizeParameter(name, "start", start_timestamp_param, target_scale);
        DateTime64 end_timestamp = normalizeParameter(name, "end", end_timestamp_param, target_scale);
        DateTime64 step = normalizeParameter(name, "step", step_param, target_scale);
        DateTime64 window = normalizeParameter(name, "window", window_param, target_scale);

        res = std::make_shared<AggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeTraits<array_arguments, DateTime64, Int64, ValueType, kind>>>(
            argument_types, start_timestamp, end_timestamp, step, window, target_scale);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        UInt64 start_timestamp = extractIntParameter(name, "start", start_timestamp_param);
        UInt64 end_timestamp = extractIntParameter(name, "end", end_timestamp_param);
        Int64 step = extractIntParameter(name, "step", step_param);
        Int64 window = extractIntParameter(name, "window", window_param);

        res = std::make_shared<AggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeTraits<array_arguments, UInt32, Int32, ValueType, kind>>>(
            argument_types, start_timestamp, end_timestamp, step, window, 0);
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
            timestamp_type->getName(), name);

    return res;
}

template <AggregateFunctionTimeseriesSimpleOverTimeKind kind>
AggregateFunctionPtr createAggregateFunctionTimeseriesSimpleOverTime(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
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
            res = createSimpleOverTimeWithValueType<kind, true, Float64>(name, argument_types, parameters);
        else
            res = createSimpleOverTimeWithValueType<kind, false, Float64>(name, argument_types, parameters);
    }
    else if (value_type->getTypeId() == TypeIndex::Float32)
    {
        if (array_arguments)
            res = createSimpleOverTimeWithValueType<kind, true, Float32>(name, argument_types, parameters);
        else
            res = createSimpleOverTimeWithValueType<kind, false, Float32>(name, argument_types, parameters);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    return res;
}


}

void registerAggregateFunctionTimeseriesSimpleOverTime(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_timeSeriesSimpleOverTimeToGrid = R"(
Aggregate function that calculates Prometheus-style simple over-time values on a regular time grid. For each grid point it reads samples in the half-open window `(timestamp - window, timestamp]`.
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesSimpleOverTimeToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"window", "Specifies the range-vector window in seconds.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesSimpleOverTimeToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesSimpleOverTimeToGrid = {"Returns values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are no samples in the window.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesSimpleOverTimeToGrid = {};
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesSimpleOverTimeToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesSimpleOverTimeToGrid = FunctionDocumentation::Category::AggregateFunction;

    FunctionDocumentation::Syntax syntax_timeSeriesSumOverTimeToGrid = R"(
timeSeriesSumOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesAvgOverTimeToGrid = R"(
timeSeriesAvgOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesMinOverTimeToGrid = R"(
timeSeriesMinOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesMaxOverTimeToGrid = R"(
timeSeriesMaxOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesCountOverTimeToGrid = R"(
timeSeriesCountOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesStddevOverTimeToGrid = R"(
timeSeriesStddevOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesStdvarOverTimeToGrid = R"(
timeSeriesStdvarOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesPresentOverTimeToGrid = R"(
timeSeriesPresentOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )";

    FunctionDocumentation documentation_timeSeriesSumOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesSumOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesAvgOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesAvgOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesMinOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesMinOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesMaxOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesMaxOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesCountOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesCountOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesStddevOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesStddevOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesStdvarOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesStdvarOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};
    FunctionDocumentation documentation_timeSeriesPresentOverTimeToGrid = {description_timeSeriesSimpleOverTimeToGrid, syntax_timeSeriesPresentOverTimeToGrid, arguments_timeSeriesSimpleOverTimeToGrid, parameters_timeSeriesSimpleOverTimeToGrid, returned_value_timeSeriesSimpleOverTimeToGrid, examples_timeSeriesSimpleOverTimeToGrid, introduced_in_timeSeriesSimpleOverTimeToGrid, category_timeSeriesSimpleOverTimeToGrid};

    factory.registerFunction("timeSeriesSumOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Sum>, documentation_timeSeriesSumOverTimeToGrid});
    factory.registerFunction("timeSeriesAvgOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Avg>, documentation_timeSeriesAvgOverTimeToGrid});
    factory.registerFunction("timeSeriesMinOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Min>, documentation_timeSeriesMinOverTimeToGrid});
    factory.registerFunction("timeSeriesMaxOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Max>, documentation_timeSeriesMaxOverTimeToGrid});
    factory.registerFunction("timeSeriesCountOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Count>, documentation_timeSeriesCountOverTimeToGrid});
    factory.registerFunction("timeSeriesStddevOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Stddev>, documentation_timeSeriesStddevOverTimeToGrid});
    factory.registerFunction("timeSeriesStdvarOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Stdvar>, documentation_timeSeriesStdvarOverTimeToGrid});
    factory.registerFunction("timeSeriesPresentOverTimeToGrid", {createAggregateFunctionTimeseriesSimpleOverTime<AggregateFunctionTimeseriesSimpleOverTimeKind::Present>, documentation_timeSeriesPresentOverTimeToGrid});

}

}
