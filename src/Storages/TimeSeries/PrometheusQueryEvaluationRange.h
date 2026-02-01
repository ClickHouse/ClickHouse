#pragma once

#include <base/Decimal.h>


namespace DB
{

/// Specifies that a prometheus query should be evaluated starting with `start_time` and ending with `end_time` with a specified `step`.
struct PrometheusQueryEvaluationRange
{
    using TimestampType = DateTime64;
    using DurationType = Decimal64;

    TimestampType start_time;
    TimestampType end_time;
    DurationType step;
};

}
