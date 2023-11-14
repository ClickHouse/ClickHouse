#include <Processors/QueryPlan/TopNStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/SplitTopN.h>

namespace DB
{

SplitTopN::SplitTopN(size_t id_) : Rule(id_)
{
    pattern.setStepType(TopN);
    pattern.addChildren({Pattern(PatternAny)});
}

std::vector<SubQueryPlan> SplitTopN::transform(SubQueryPlan & sub_plan, ContextPtr context)
{
    auto * topn_step = typeid_cast<TopNStep *>(sub_plan.getRootNode()->step.get());

    if (!topn_step)
        return {};

    if (topn_step->getPhase() != TopNStep::Phase::Unknown)
        return {};

    auto child_step = sub_plan.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;
    auto pre_topn = topn_step->makePreliminary(exact_rows_before_limit);

    const auto max_block_size = context->getSettingsRef().max_block_size;
    auto final_topn = topn_step->makeFinal(pre_topn->getOutputStream(), max_block_size, exact_rows_before_limit);

    SubQueryPlan res_sub_plan;
    res_sub_plan.addStep(child_step);
    res_sub_plan.addStep(pre_topn);
    res_sub_plan.addStep(final_topn);

    std::vector<SubQueryPlan> res;
    res.emplace_back(std::move(res_sub_plan));
    return res;
}
}
