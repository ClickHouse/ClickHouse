#include <Processors/QueryPlan/SortingStep.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <QueryCoordination/Optimizer/Rule/SplitSort.h>


namespace DB
{

SplitSort::SplitSort(size_t id_) : Rule(id_)
{
    pattern.setStepType(Sort);
    pattern.addChildren({Pattern(PatternAny)});
}

std::vector<SubQueryPlan> SplitSort::transform(SubQueryPlan & sub_plan, ContextPtr context)
{
    auto * sorting_step = typeid_cast<SortingStep *>(sub_plan.getRootNode()->step.get());

    if (!sorting_step)
        return {};

    if (sorting_step->getPhase() != SortingStep::Phase::Unknown)
        return {};

    auto child_step = sub_plan.getRootNode()->children[0]->step;
    auto * group_step = typeid_cast<GroupStep *>(child_step.get());
    if (!group_step)
        return {};

    auto pre_sort = sorting_step->clone();
    pre_sort->setPhase(SortingStep::Phase::Preliminary);

    const auto max_block_size = context->getSettingsRef().max_block_size;
    const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;
    auto merging_sorted = std::make_unique<SortingStep>(
        pre_sort->getOutputStream(), sorting_step->getSortDescription(), max_block_size, sorting_step->getLimit(), exact_rows_before_limit);
    merging_sorted->setPhase(SortingStep::Phase::Final);

    SubQueryPlan res_sub_plan;
    res_sub_plan.addStep(child_step);
    res_sub_plan.addStep(pre_sort);
    res_sub_plan.addStep(std::move(merging_sorted));

    std::vector<SubQueryPlan> res;
    res.emplace_back(std::move(res_sub_plan));
    return res;
}

}
