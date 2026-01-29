#pragma once

#include <base/Decimal.h>


namespace DB::PrometheusQueryToSQL
{

/// Represents evaluation times found for a node in PrometheusQueryTree.
struct NodeEvaluationRange
{
    DateTime64 start_time;
    DateTime64 end_time;
    Decimal64 step;
    Decimal64 window;
};

}
