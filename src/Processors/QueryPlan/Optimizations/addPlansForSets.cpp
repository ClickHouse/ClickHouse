#include <memory>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

bool addPlansForSets(QueryPlan & root_plan, QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(node.step.get());
    if (!delayed)
        return false;

    auto plans = DelayedCreatingSetsStep::makePlansForSets(std::move(*delayed));
    node.children.reserve(1 + plans.size());

    Headers input_headers;
    input_headers.reserve(1 + plans.size());
    input_headers.push_back(node.children.front()->step->getOutputHeader());

    for (const auto & plan : plans)
    {
        input_headers.push_back(plan->getCurrentHeader());
        node.children.push_back(plan->getRootNode());
        auto [add_nodes, add_resources] = QueryPlan::detachNodesAndResources(std::move(*plan));
        nodes.splice(nodes.end(), std::move(add_nodes));
        root_plan.addResources(std::move(add_resources));
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_headers));
    creating_sets->setStepDescription("Create sets before main query execution");
    node.step = std::move(creating_sets);
    return true;
}

}
