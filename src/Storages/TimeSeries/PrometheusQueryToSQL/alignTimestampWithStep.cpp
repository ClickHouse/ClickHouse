#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/alignTimestampWithStep.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::PrometheusQueryToSQL
{

TimestampType alignTimestampWithStep(TimestampType timestamp, DurationType step)
{
    if (step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto remainder = timestamp % step;
    if (remainder < 0)
        remainder += step;
    return timestamp - remainder;
}

}
