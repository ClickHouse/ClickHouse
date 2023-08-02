#include <memory>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

bool addPlansForSets(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(node.step.get());
    if (!delayed)
        return false;

    auto plans = DelayedCreatingSetsStep::makePlansForSets(std::move(*delayed));
    node.children.reserve(1 + plans.size());

    DataStreams input_streams;
    input_streams.reserve(1 + plans.size());
    input_streams.push_back(node.children.front()->step->getOutputStream());

    for (const auto & plan : plans)
    {
        input_streams.push_back(plan->getCurrentDataStream());
        node.children.push_back(plan->getRootNode());
        nodes.splice(nodes.end(), QueryPlan::detachNodes(std::move(*plan)));
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_streams));
    creating_sets->setStepDescription("Create sets before main query execution");
    node.step = std::move(creating_sets);
    return true;
}

}
