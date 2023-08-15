#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>
#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>

namespace DB
{

namespace QueryPlanOptimizations
{

struct Optimization
{
    using Function = std::vector<SubQueryPlan> (*)(QueryPlanStepPtr step, ContextPtr context);
    const Function apply = nullptr;
    const char * name = "";
    const bool QueryPlanOptimizationSettings::* const is_enabled{};
};


std::vector<SubQueryPlan> trySplitAggregation(QueryPlanStepPtr step, ContextPtr context);


inline const auto & getOptimizations()
{
    static const std::array<Optimization, 10> optimizations = {{
        {trySplitAggregation, "splitAggregation", &QueryPlanOptimizationSettings::optimize_plan},
    }};

    return optimizations;
}

struct Frame
{
    QueryPlan::Node * node = nullptr;
    size_t next_child = 0;
};

using Stack = std::vector<Frame>;


}

}
