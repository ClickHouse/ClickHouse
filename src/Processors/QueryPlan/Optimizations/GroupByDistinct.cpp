#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

const DB::DataStream & getChildOutputStream(DB::QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node \"{}\" is expected to have only one child.", node.step->getName());
    return node.children.front()->step->getOutputStream();
}

}

namespace DB::QueryPlanOptimizations
{

size_t tryReplaceGroupByWithDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent_step = parent_node->step;
    auto & child_step = child_node->step;

    auto * limit_step = typeid_cast<LimitStep *>(parent_step.get());
    auto * aggregating_step = typeid_cast<AggregatingStep *>(child_step.get());

    if (!limit_step || !aggregating_step)
        return 0;

    if (aggregating_step->getParams().aggregates.empty())
        return 0;

    auto & distinct_node = nodes.emplace_back();
    distinct_node.step = std::make_unique<DistinctStep>(getChildOutputStream(*child_node), 
        SizeLimits{}, 
        limit_step->getLimitForSorting(),
        aggregating_step->getParams().keys, 
        false, 
        false);

    distinct_node.step->setStepDescription(child_step->getStepDescription());

    parent_node->children = {&distinct_node};
    std::swap(child_node->children, distinct_node.children);

    return 1;
}
}
