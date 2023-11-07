#include <Processors/QueryPlan/LimitStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/SplitLimit.h>


namespace DB
{

SplitLimit::SplitLimit()
{
    pattern.setStepType(Limit);
    pattern.addChildren({Pattern(PatternAny)});
}

std::vector<StepTree> SplitLimit::transform(StepTree & step_tree, ContextPtr context)
{
    auto * limit_step = typeid_cast<LimitStep *>(step_tree.getRootNode()->step.get());

    if (!limit_step)
        return {};

    if (limit_step->getPhase() != LimitStep::Phase::Unknown)
        return {};

    auto child_step = step_tree.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    auto pre_limit = std::make_shared<LimitStep>(
        limit_step->getInputStreams().front(), limit_step->getLimitForSorting(), 0, context->getSettings().exact_rows_before_limit);
    pre_limit->setPhase(LimitStep::Phase::Preliminary);

    auto final_limit = std::make_shared<LimitStep>(
        limit_step->getInputStreams().front(),
        limit_step->getLimit(),
        limit_step->getOffset(),
        context->getSettings().exact_rows_before_limit);
    final_limit->setPhase(LimitStep::Phase::Final);

    StepTree res_step_tree;
    res_step_tree.addStep(child_step);
    res_step_tree.addStep(pre_limit);
    res_step_tree.addStep(final_limit);

    std::vector<StepTree> res;
    res.emplace_back(std::move(res_step_tree));
    return res;
}

}
