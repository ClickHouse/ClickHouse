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

    std::vector<StepTree> res;

    auto pre_limit = std::make_shared<LimitStep>(limit_step->getInputStreams().front(), limit_step->getLimitForSorting(), 0, context->getSettings().exact_rows_before_limit);
    pre_limit->setType(LimitStep::Type::Local);

    auto final_limit = std::make_shared<LimitStep>(limit_step->getInputStreams().front(), limit_step->getLimit(), limit_step->getOffset(), context->getSettings().exact_rows_before_limit);
    final_limit->setType(LimitStep::Type::Global);

    StepTree sub_query_plan;
    sub_query_plan.addStep(pre_limit);
    sub_query_plan.addStep(final_limit);

    res.emplace_back(std::move(sub_query_plan));
    return res;
}

}
