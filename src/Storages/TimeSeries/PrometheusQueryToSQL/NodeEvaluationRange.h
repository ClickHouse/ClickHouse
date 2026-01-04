#pragma once

#include <Core/Field.h>


namespace DB::PrometheusQueryToSQL
{

/// Represents evaluation times found for a node in PrometheusQueryTree.
struct NodeEvaluationRange
{
    DecimalField<DateTime64> start_time;
    DecimalField<DateTime64> end_time;
    DecimalField<Decimal64> step;
    DecimalField<Decimal64> window;
};

}
