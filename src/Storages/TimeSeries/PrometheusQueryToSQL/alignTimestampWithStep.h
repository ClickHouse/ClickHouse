#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Decreases a timestamp to make it divisible by `step`.
/// Returns the same timestamp if it's already divisible by `step`.
TimestampType alignTimestampWithStep(TimestampType timestamp, DurationType step);

}
