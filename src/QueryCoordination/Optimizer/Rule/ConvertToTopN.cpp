#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/TopNStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/ConvertToTopN.h>

namespace DB
{

ConvertToTopN::ConvertToTopN(size_t id_) : Rule(id_)
{
    pattern.setStepType(Limit);
    Pattern child_pattern(Sort);
    child_pattern.addChildren({Pattern(PatternAny)});
    pattern.addChildren({child_pattern});
}

std::vector<SubQueryPlan> ConvertToTopN::transform(SubQueryPlan & sub_plan, ContextPtr /*context*/)
{
    auto * limit_step = typeid_cast<LimitStep *>(sub_plan.getRootNode()->step.get());

    if (!limit_step)
        return {};

    if (limit_step->getPhase() != LimitStep::Phase::Unknown)
        return {};

    auto group_step = sub_plan.getRootNode()->children[0]->children[0]->step;
    if (!typeid_cast<GroupStep *>(group_step.get()))
        return {};

    auto sorting_step = sub_plan.getRootNode()->children[0]->step;
    auto topn = std::make_shared<TopNStep>(sorting_step, sub_plan.getRootNode()->step);

    SubQueryPlan res_sub_plan;
    res_sub_plan.addStep(group_step);
    res_sub_plan.addStep(topn);

    std::vector<SubQueryPlan> res;
    res.emplace_back(std::move(res_sub_plan));
    return res;
}

}
