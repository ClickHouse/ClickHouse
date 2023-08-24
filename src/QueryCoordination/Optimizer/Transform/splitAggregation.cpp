#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/StepTree.h>
#include <Common/typeid_cast.h>


namespace DB::Optimizer
{

std::vector<StepTree> trySplitAggregation(GroupNode & group_node, ContextPtr context)
{
    auto * aggregate_step = typeid_cast<AggregatingStep *>(group_node.getStep().get());

    if (!aggregate_step)
        return {};

    if (!aggregate_step->isFinal())
        return {};

    auto partial_agg_step = aggregate_step->clone(false);

    const Settings & settings = context->getSettingsRef();
    std::shared_ptr<MergingAggregatedStep> merge_agg_step = partial_agg_step->makeMergingAggregatedStep(partial_agg_step->getOutputStream(), settings);

    std::vector<StepTree> res;
    if (!aggregate_step->getParams().keys_size || aggregate_step->withTotalsOrCubeOrRollup())
    {
        /// require gather
        StepTree step_tree;
        step_tree.addStep(partial_agg_step);
        step_tree.addStep(merge_agg_step);

        res.emplace_back(std::move(step_tree));
    }
    else
    {
        /// require Hash
        StepTree step_tree;
        step_tree.addStep(partial_agg_step);
        step_tree.addStep(merge_agg_step);

        res.emplace_back(std::move(step_tree));
    }

    return res;
}

}
