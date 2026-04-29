#pragma once

#include <Common/SipHash.h>
#include <base/types.h>

#include <string_view>


namespace DB
{

/// UInt32 locality key from full metric name.
/// Must match the built-in SQL UDF `timeSeriesMetricLocalityId` (`toUInt32(sipHash64(metric_name))`), validated when
/// TimeSeries uses it (`ensureTimeSeriesMetricLocalityIdUserDefinedFunction`); startup only registers if absent; see
/// `registerBuiltinSQLUserDefinedFunctions`.
inline UInt32 timeSeriesMetricLocalityIdFromMetricName(std::string_view metric_name)
{
    return static_cast<UInt32>(sipHash64(metric_name.data(), metric_name.size()));
}

}
