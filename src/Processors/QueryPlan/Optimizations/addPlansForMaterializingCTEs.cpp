#include <Common/typeid_cast.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

void addPlansForMaterializingCTEs(
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan & root_plan,
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes)
{
    auto * delayed = typeid_cast<DelayedMaterializingCTEsStep *>(node.step.get());
    if (!delayed)
        return;

    auto plans = DelayedMaterializingCTEsStep::makePlansForCTEs(std::move(*delayed), optimization_settings);

    SharedHeaders input_headers;
    input_headers.reserve(1 + plans.size());
    input_headers.push_back(node.children.front()->step->getOutputHeader());

    for (auto & plan : plans)
    {
        input_headers.push_back(plan->getCurrentHeader());
        node.children.push_back(plan->getRootNode());
        auto [add_nodes, add_resources] = QueryPlan::detachNodesAndResources(std::move(*plan));
        nodes.splice(nodes.end(), std::move(add_nodes));
        root_plan.addResources(std::move(add_resources));
    }

    auto materializing_ctes = std::make_unique<MaterializingCTEsStep>(std::move(input_headers));
    materializing_ctes->setStepDescription("Materialize CTEs before main query execution");
    node.step = std::move(materializing_ctes);
}

}

void resolveMaterializingCTEs(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & root_plan, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        addPlansForMaterializingCTEs(optimization_settings, root_plan, *frame.node, nodes);

        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }
}

}
