#pragma once

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Histogram sibling of createAggregateFunctionTimeseries: the same gate and dispatch, but the value argument is the fixed
/// native-histogram payload tuple (getTimeSeriesHistogramPayloadTupleType), so no ValueType to resolve; array arguments unsupported.
template <typename MakeFunction>
AggregateFunctionPtr createAggregateFunctionTimeseriesHistogram(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings, MakeFunction && make_function)
{
    checkTimeseriesAggregateFunctionsEnabled(name, settings);

    assertBinary(name, argument_types);

    if (argument_types[0]->getTypeId() == TypeIndex::Array || argument_types[1]->getTypeId() == TypeIndex::Array)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Array arguments are not supported yet for aggregate function {}", name);

    /// The histogram argument is decoded positionally, so accept any tuple with the exact payload element types in order,
    /// regardless of element names: `tuple(...)` yields auto-numbered names unless `enable_named_columns_in_function_tuple` is set.
    if (!isTimeSeriesHistogramPayloadTupleType(argument_types[1]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (histogram) for aggregate function {}, expected Tuple with the payload of {}",
            argument_types[1]->getName(), name, getTimeSeriesHistogramPayloadTupleType()->getName());

    const auto & timestamp_type = argument_types[0];

    /// Unlike `createAggregateFunctionTimeseries`, the grid uses the scale of the timestamp column: the grid parameters
    /// are converted to that scale. There is no ValueType to resolve - the histogram payload is fixed - so the ValueType
    /// template parameter of `make_function` goes unused (void).
    if (isDateTime64(timestamp_type))
    {
        const UInt32 scale = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type)->getScale();
        return make_function.template operator()<DateTime64, Int64, void>(
            extractTimeseriesTimestampParameter(name, "start", parameters[0], scale),
            extractTimeseriesTimestampParameter(name, "end", parameters[1], scale),
            extractTimeseriesDurationParameter(name, "step", parameters[2], scale).value,
            extractTimeseriesDurationParameter(name, "window", parameters[3], scale).value,
            scale);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        return make_function.template operator()<UInt32, Int32, void>(
            static_cast<UInt32>(extractTimeseriesTimestampParameter(name, "start", parameters[0], 0).value),
            static_cast<UInt32>(extractTimeseriesTimestampParameter(name, "end", parameters[1], 0).value),
            static_cast<Int32>(extractTimeseriesDurationParameter(name, "step", parameters[2], 0).value),
            static_cast<Int32>(extractTimeseriesDurationParameter(name, "window", parameters[3], 0).value),
            0);
    }

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                    timestamp_type->getName(), name);
}

}
