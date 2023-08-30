#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/StepTree.h>
#include <QueryCoordination/Optimizer/GroupStep.h>
#include <Common/typeid_cast.h>


namespace DB::Optimizer
{

std::vector<StepTree> trySplitLimit(GroupNode & group_node, ContextPtr context)
{
    auto * limit_step = typeid_cast<LimitStep *>(group_node.getStep().get());

    if (!limit_step)
        return {};

    if (limit_step->getType() != LimitStep::Type::Unknown)
        return {};

    Group * child_group = group_node.getChildren()[0];
    auto child_step = std::make_shared<GroupStep>(limit_step->getInputStreams()[0], *child_group);

    auto pre_limit = std::make_shared<LimitStep>(limit_step->getInputStreams().front(), limit_step->getLimitForSorting(), 0, context->getSettings().exact_rows_before_limit);
    pre_limit->setType(LimitStep::Type::Local);

    auto final_limit = std::make_shared<LimitStep>(limit_step->getInputStreams().front(), limit_step->getLimit(), limit_step->getOffset(), context->getSettings().exact_rows_before_limit);
    final_limit->setType(LimitStep::Type::Global);

    StepTree step_tree;
    step_tree.addStep(child_step);
    step_tree.addStep(pre_limit);
    step_tree.addStep(final_limit);

    std::vector<StepTree> res;
    res.emplace_back(std::move(step_tree));
    return res;
}

}
