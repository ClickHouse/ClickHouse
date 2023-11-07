#include <QueryCoordination/Optimizer/Rule/ConvertToTopN.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/TopNStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>

namespace DB
{

ConvertToTopN::ConvertToTopN()
{
    pattern.setStepType(Limit);
    Pattern child_pattern(Sort);
    child_pattern.addChildren({Pattern(PatternAny)});
    pattern.addChildren({child_pattern});
}

std::vector<StepTree> ConvertToTopN::transform(StepTree & step_tree, ContextPtr /*context*/)
{
    auto * limit_step = typeid_cast<LimitStep *>(step_tree.getRootNode()->step.get());

    if (!limit_step)
        return {};

    if (limit_step->getPhase() != LimitStep::Phase::Unknown)
        return {};

    auto group_step = step_tree.getRootNode()->children[0]->children[0]->step;
    if (!typeid_cast<GroupStep *>(group_step.get()))
        return {};

    auto sorting_step = step_tree.getRootNode()->children[0]->step;
    auto topn = std::make_shared<TopNStep>(sorting_step, step_tree.getRootNode()->step);

    StepTree res_step_tree;
    res_step_tree.addStep(group_step);
    res_step_tree.addStep(topn);

    std::vector<StepTree> res;
    res.emplace_back(std::move(res_step_tree));
    return res;
}

}
