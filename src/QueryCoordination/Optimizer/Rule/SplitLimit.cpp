#include <Processors/QueryPlan/LimitStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/SplitLimit.h>


namespace DB
{

SplitLimit::SplitLimit(size_t id_) : Rule(id_)
{
    pattern.setStepType(Limit);
    pattern.addChildren({Pattern(PatternAny)});
}

std::vector<SubQueryPlan> SplitLimit::transform(SubQueryPlan & sub_plan, ContextPtr context)
{
    auto * limit_step = typeid_cast<LimitStep *>(sub_plan.getRootNode()->step.get());

    if (!limit_step)
        return {};

    if (limit_step->getPhase() != LimitStep::Phase::Unknown)
        return {};

    auto child_step = sub_plan.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    auto pre_limit = std::make_shared<LimitStep>(
        limit_step->getInputStreams().front(), limit_step->getLimitForSorting(), 0, context->getSettings().exact_rows_before_limit);
    pre_limit->setPhase(LimitStep::Phase::Preliminary);
    pre_limit->setStepDescription("Preliminary");

    auto final_limit = std::make_shared<LimitStep>(
        limit_step->getInputStreams().front(),
        limit_step->getLimit(),
        limit_step->getOffset(),
        context->getSettings().exact_rows_before_limit);
    final_limit->setPhase(LimitStep::Phase::Final);
    final_limit->setStepDescription("Final");

    SubQueryPlan res_sub_plan;
    res_sub_plan.addStep(child_step);
    res_sub_plan.addStep(pre_limit);
    res_sub_plan.addStep(final_limit);

    std::vector<SubQueryPlan> res;
    res.emplace_back(std::move(res_sub_plan));
    return res;
}

}
