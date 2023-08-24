#pragma once

#include <array>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Optimizer/StepTree.h>

namespace DB
{

class GroupNode;

namespace Optimizer
{

struct Transformation
{
    using Function = std::vector<StepTree> (*)(GroupNode & group_node, ContextPtr context);
    const Function apply = nullptr;
    const char * name = "";
};


std::vector<StepTree> trySplitAggregation(GroupNode & group_node, ContextPtr context);
std::vector<StepTree> trySplitLimit(GroupNode & group_node, ContextPtr context);


inline const auto & getTransformations()
{
    static const std::vector<Transformation> transformations = {{
        {trySplitAggregation, "splitAggregation"},
        {trySplitLimit, "splitLimit"},
    }};

    return transformations;
}

}

}
