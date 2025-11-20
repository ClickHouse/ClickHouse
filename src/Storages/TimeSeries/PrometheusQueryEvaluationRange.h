#pragma once

#include <Core/Field.h>


namespace DB
{

/// Specifies that a prometheus query should be evaluated starting with `start_time` and ending with `end_time` with a specified `step`.
struct PrometheusQueryEvaluationRange
{
    DecimalField<DateTime64> start_time;
    DecimalField<DateTime64> end_time;
    DecimalField<Decimal64> step;
};

}
