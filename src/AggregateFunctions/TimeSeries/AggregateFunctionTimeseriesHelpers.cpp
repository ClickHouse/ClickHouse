#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <Core/Field.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsBool enable_time_series_aggregate_functions;
    extern const SettingsBool enable_time_series_table;
}

namespace TimeSeriesToGrid
{

DateTime64 extractTimestampParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale)
{
    try
    {
        return parseTimeSeriesTimestamp(parameter_field, target_scale);
    }
    catch (Exception & e)
    {
        e.addMessage("While parsing {} parameter of aggregate function {}", parameter_name, function_name);
        throw;
    }
}

Decimal64 extractDurationParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale)
{
    try
    {
        return parseTimeSeriesDuration(parameter_field, target_scale);
    }
    catch (Exception & e)
    {
        e.addMessage("While parsing {} parameter of aggregate function {}", parameter_name, function_name);
        throw;
    }
}

UInt64 extractIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field)
{
    if (parameter_field.getType() == Field::Types::Decimal64)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal64>>();
        auto scale_multiplier = value.getScaleMultiplier();
        auto raw_value = value.getValue();
        if (scale_multiplier > 1 && raw_value % scale_multiplier != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot convert Decimal64 {} parameter to integer for aggregate function {}", parameter_name, function_name);
        return raw_value / scale_multiplier;
    }
    else if (parameter_field.getType() == Field::Types::Decimal32)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal32>>();
        auto scale_multiplier = value.getScaleMultiplier();
        auto raw_value = value.getValue();
        if (scale_multiplier > 1 && raw_value % scale_multiplier != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot convert Decimal32 {} parameter to integer for aggregate function {}", parameter_name, function_name);
        return raw_value / scale_multiplier;
    }
    else if (UInt64 int_value = 0; parameter_field.tryGet(int_value))
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
    if (parameter_field.getType() == Field::Types::Decimal64)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal64>>();
        return static_cast<Float64>(value.getValue()) / static_cast<Float64>(value.getScaleMultiplier());
    }
    else if (parameter_field.getType() == Field::Types::Decimal32)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal32>>();
        return static_cast<Float64>(value.getValue()) / static_cast<Float64>(value.getScaleMultiplier());
    }
    else if (Float64 float_value = 0; parameter_field.tryGet(float_value))
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
        if (tryReadFloatTextPrecise(value, buf))
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

void assertParametersCount(const std::string & name, const Array & parameters, size_t required, std::string_view parameter_names)
{
    if (parameters.size() != required)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires {} parameters: {}", name, required, parameter_names);
}

void assertTimeSeriesAggregateFunctionsEnabled(const std::string & name, const Settings * settings)
{
    if (settings && (*settings)[Setting::enable_time_series_aggregate_functions] == 0 && (*settings)[Setting::enable_time_series_table] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is in private preview and disabled by default. Enable it with setting enable_time_series_aggregate_functions",
            name);
}

}

void registerAggregateFunctionTimeseries(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseries(AggregateFunctionFactory & factory)
{
    registerAggregateFunctionTimeseriesExtrapolatedValue(factory);
    registerAggregateFunctionTimeseriesInstantValue(factory);
    registerAggregateFunctionTimeseriesLinearRegression(factory);
    registerAggregateFunctionTimeseriesChanges(factory);
    registerAggregateFunctionTimeseriesToGridSparse(factory);
    registerAggregateFunctionTimeseriesCompensatedSum(factory);
    registerAggregateFunctionTimeseriesCount(factory);
    registerAggregateFunctionTimeseriesMax(factory);
    registerAggregateFunctionTimeseriesMin(factory);
    registerAggregateFunctionTimeseriesPresentToGrid(factory);
    registerAggregateFunctionTimeseriesQuantileToGrid(factory);
}

}
