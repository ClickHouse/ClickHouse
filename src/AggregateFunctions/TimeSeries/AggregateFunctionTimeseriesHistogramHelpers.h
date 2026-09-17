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

    if (isDateTime64(timestamp_type))
    {
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        /// There is no ValueType to resolve - the histogram payload is fixed - so the ValueType template
        /// parameter of createAggregateFunctionTimeseriesWithTypes goes unused (void).
        return createAggregateFunctionTimeseriesWithTypes<DateTime64, Int64, void>(name, parameters, timestamp_decimal->getScale(), make_function);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        return createAggregateFunctionTimeseriesWithTypes<UInt32, Int32, void>(name, parameters, 0, make_function);
    }

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                    timestamp_type->getName(), name);
}

}
