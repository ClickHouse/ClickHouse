#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

namespace DB::QueryPlanOptimizations
{

size_t tryRemoveDuplicateDistincts(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto * distinct_step = typeid_cast<DistinctStep *>(parent_node->step.get());
    auto * predistinct_step = typeid_cast<DistinctStep *>(child_node->step.get());

    if (!distinct_step || !predistinct_step)
        return 0;

    // We will keep only the "final" single-threaded distinct.
    std::swap(parent_node->children, child_node->children);
    const auto it = std::find_if(nodes.begin(), nodes.end(), [&](const auto & node) { return &node == child_node; });
    nodes.erase(it);

    return 2;
}
}
