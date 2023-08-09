#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Common/typeid_cast.h>
#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>


namespace DB::QueryPlanOptimizations
{

std::vector<SubQueryPlan> trySplitAggregation(QueryPlanStepPtr step)
{
    auto * aggregate_step = typeid_cast<AggregatingStep *>(step.get());

    if (!aggregate_step)
        return {};

    auto partial_agg_step = aggregate_step->clone(false);

    const Settings & settings = context->getSettingsRef();
    std::shared_ptr<MergingAggregatedStep> merge_agg_step = partial_agg_step->makeMergingAggregatedStep(partial_agg_step->getOutputStream(), settings);

    std::vector<SubQueryPlan> res;
    if (!aggregate_step->getParams().keys_size || aggregate_step->withTotalsOrCubeOrRollup())
    {
        /// require gather
        SubQueryPlan sub_query_plan;
        sub_query_plan.addStep(partial_agg_step);
        sub_query_plan.addStep(merge_agg_step);

        res.emplace_back(sub_query_plan);
    }
    else
    {
        /// require Hash
        SubQueryPlan sub_query_plan;
        sub_query_plan.addStep(partial_agg_step);
        sub_query_plan.addStep(merge_agg_step);

        res.emplace_back(sub_query_plan);
    }

    return res;
}

}
