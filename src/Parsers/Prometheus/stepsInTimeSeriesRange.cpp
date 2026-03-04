#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Common/Exception.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace DB::PrometheusQueryToSQL
{

size_t stepsInTimeSeriesRange(TimestampType start_time, TimestampType end_time, DurationType step)
{
    if (start_time == end_time)
        return 1;

    if (end_time < start_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");

    if (step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");

    return (end_time - start_time) / step + 1;
}

}
