#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/SplitAggregation.h>
#include <Common/typeid_cast.h>

namespace DB
{

SplitAggregation::SplitAggregation()
{
    pattern.setStepType(Agg);
    pattern.addChildren({Pattern(PatternAny)});
}

std::vector<StepTree> SplitAggregation::transform(StepTree & step_tree, ContextPtr context)
{
    auto * aggregate_step = typeid_cast<AggregatingStep *>(step_tree.getRootNode()->step.get());

    if (!aggregate_step)
        return {};

    if (aggregate_step->isPreliminaryAgg())
        return {};

    auto child_step = step_tree.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    auto partial_agg_step = aggregate_step->makePreliminaryAgg();

    const Settings & settings = context->getSettingsRef();
    std::shared_ptr<MergingAggregatedStep> merge_agg_step
        = aggregate_step->makeMergingAggregatedStep(partial_agg_step->getOutputStream(), settings);

    StepTree res_step_tree;
    res_step_tree.addStep(child_step);
    res_step_tree.addStep(partial_agg_step);
    res_step_tree.addStep(merge_agg_step);

    std::vector<StepTree> res;
    res.emplace_back(std::move(res_step_tree));
    return res;
}

}
