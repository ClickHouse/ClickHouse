#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>
#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>

namespace DB
{

namespace NewOptimizer
{

struct Transformation
{
    using Function = std::vector<SubQueryPlan> (*)(QueryPlanStepPtr step, ContextPtr context);
    const Function apply = nullptr;
    const char * name = "";
};


std::vector<SubQueryPlan> trySplitAggregation(QueryPlanStepPtr step, ContextPtr context);
std::vector<SubQueryPlan> trySplitLimit(QueryPlanStepPtr step, ContextPtr context);


inline const auto & getTransformations()
{
    static const std::array<Transformation, 10> transformations = {{
        {trySplitAggregation, "splitAggregation"},
        {trySplitLimit, "splitLimit"},
    }};

    return transformations;
}

}

}
