#include <Processors/QueryPlan/TopNStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/SplitTopN.h>

namespace DB
{

SplitTopN::SplitTopN()
{
    pattern.setStepType(TopN);
    pattern.addChildren({Pattern(PatternAny)});
}

std::vector<StepTree> SplitTopN::transform(StepTree & step_tree, ContextPtr context)
{
    auto * topn_step = typeid_cast<TopNStep *>(step_tree.getRootNode()->step.get());

    if (!topn_step)
        return {};

    if (topn_step->getPhase() != TopNStep::Phase::Unknown)
        return {};

    auto child_step = step_tree.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;
    auto pre_topn = topn_step->makePreliminary(exact_rows_before_limit);

    const auto max_block_size = context->getSettingsRef().max_block_size;
    auto final_topn = topn_step->makeFinal(pre_topn->getOutputStream(), max_block_size, exact_rows_before_limit);

    StepTree res_step_tree;
    res_step_tree.addStep(child_step);
    res_step_tree.addStep(pre_topn);
    res_step_tree.addStep(final_topn);

    std::vector<StepTree> res;
    res.emplace_back(std::move(res_step_tree));
    return res;
}
}
