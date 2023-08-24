#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Common/typeid_cast.h>
#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>


namespace DB::NewOptimizer
{

std::vector<SubQueryPlan> trySplitLimit(QueryPlanStepPtr step, ContextPtr context)
{
    auto * limit_step = typeid_cast<LimitStep *>(step.get());

    if (!limit_step)
        return {};

    step->setStepDescription("origin LIMIT");

    std::vector<SubQueryPlan> res;

    auto pre_limit = std::make_shared<LimitStep>(step->getInputStreams().front(), limit_step->getLimitForSorting(), 0, context->getSettings().exact_rows_before_limit);
    pre_limit->setStepDescription("preliminary LIMIT (without OFFSET)");

    auto final_limit = std::make_shared<LimitStep>(step->getInputStreams().front(), limit_step->getLimit(), limit_step->getOffset(), context->getSettings().exact_rows_before_limit);
    final_limit->setStepDescription("final LIMIT");

    SubQueryPlan sub_query_plan;
    sub_query_plan.addStep(pre_limit);
    sub_query_plan.addStep(final_limit);

    res.emplace_back(std::move(sub_query_plan));
    return res;
}

}
