#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/StepTree.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <Common/typeid_cast.h>


namespace DB::Optimizer
{

std::vector<StepTree> trySplitAggregation(GroupNode & group_node, ContextPtr context)
{
    auto * aggregate_step = typeid_cast<AggregatingStep *>(group_node.getStep().get());

    if (!aggregate_step)
        return {};

    if (aggregate_step->isPreliminaryAgg())
        return {};

    auto partial_agg_step = aggregate_step->makePreliminaryAgg();

    const Settings & settings = context->getSettingsRef();
    std::shared_ptr<MergingAggregatedStep> merge_agg_step = aggregate_step->makeMergingAggregatedStep(partial_agg_step->getOutputStream(), settings);

    Group * child_group = group_node.getChildren()[0];
    auto child_step = std::make_shared<GroupStep>(aggregate_step->getInputStreams()[0], *child_group);

    std::vector<StepTree> res;
    StepTree step_tree;
    step_tree.addStep(child_step);

    if (!aggregate_step->getParams().keys_size || !aggregate_step->isFinal())
    {
        /// TODO require gather
        step_tree.addStep(partial_agg_step);
        step_tree.addStep(merge_agg_step);
    }
    else
    {
        /// TODO require Hash
        step_tree.addStep(partial_agg_step);
        step_tree.addStep(merge_agg_step);
    }
    res.emplace_back(std::move(step_tree));

    return res;
}

}
