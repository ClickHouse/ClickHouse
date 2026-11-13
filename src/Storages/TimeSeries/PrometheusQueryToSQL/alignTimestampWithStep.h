#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Rounds down to the nearest multiple of `step`, including for negative timestamps.
TimestampType alignTimestampWithStep(TimestampType timestamp, DurationType step);

}
