#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns the number of points between `start_time` and `end_time` with the specified `step`,
/// including `start_time` and maybe `end_time`.
size_t stepsInTimeSeriesRange(DateTime64 start_time, DateTime64 end_time, Decimal64 step);

}
