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

std::vector<SubQueryPlan> SplitAggregation::transform(SubQueryPlan & sub_plan, ContextPtr context)
{
    auto * aggregate_step = typeid_cast<AggregatingStep *>(sub_plan.getRootNode()->step.get());

    if (!aggregate_step)
        return {};

    if (aggregate_step->isPreliminaryAgg())
        return {};

    auto child_step = sub_plan.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    auto partial_agg_step = aggregate_step->makePreliminaryAgg();

    const Settings & settings = context->getSettingsRef();
    std::shared_ptr<MergingAggregatedStep> merge_agg_step
        = aggregate_step->makeMergingAggregatedStep(partial_agg_step->getOutputStream(), settings);

    SubQueryPlan res_sub_plan;
    res_sub_plan.addStep(child_step);
    res_sub_plan.addStep(partial_agg_step);
    res_sub_plan.addStep(merge_agg_step);

    std::vector<SubQueryPlan> res;
    res.emplace_back(std::move(res_sub_plan));
    return res;
}

}
