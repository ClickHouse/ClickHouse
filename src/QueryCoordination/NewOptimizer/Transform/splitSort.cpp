//#include <Processors/QueryPlan/Optimizations/Optimizations.h>
//#include <Processors/QueryPlan/ITransformingStep.h>
//#include <Processors/QueryPlan/AggregatingStep.h>
//#include <Processors/QueryPlan/TotalsHavingStep.h>
//#include <Processors/QueryPlan/SortingStep.h>
//#include <Processors/QueryPlan/WindowStep.h>
//#include <Common/typeid_cast.h>
//#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>
//#include <Processors/QueryPlan/MergingAggregatedStep.h>
//
//
//namespace DB::QueryPlanOptimizations
//{
//
//std::vector<SubQueryPlan> trySplitSort(QueryPlanStepPtr step, ContextPtr context)
//{
//    auto * sorting_step = typeid_cast<SortingStep *>(step.get());
//
//    if (!sorting_step)
//        return {};
//
//    const SortDescription & sort_description = sorting_step->getSortDescription();
//    const UInt64 limit = sorting_step->getLimit();
//    const auto max_block_size = context->getSettingsRef().max_block_size;
//    const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;
//
//    ExchangeDataStep::SortInfo sort_info{
//        .max_block_size = max_block_size,
//        .always_read_till_end = exact_rows_before_limit,
//        .limit = limit,
//        .result_description = sort_description};
//
//    auto * exchange_step = dynamic_cast<ExchangeDataStep *>(exchange_node->step.get());
//    exchange_step->setSortInfo(sort_info);
//
//
//    auto partial_agg_step = aggregate_step->clone(false);
//
//    const Settings & settings = context->getSettingsRef();
//    std::shared_ptr<MergingAggregatedStep> merge_agg_step = partial_agg_step->makeMergingAggregatedStep(partial_agg_step->getOutputStream(), settings);
//
//    std::vector<SubQueryPlan> res;
//    if (!aggregate_step->getParams().keys_size || aggregate_step->withTotalsOrCubeOrRollup())
//    {
//        /// require gather
//        SubQueryPlan sub_query_plan;
//        sub_query_plan.addStep(partial_agg_step);
//        sub_query_plan.addStep(merge_agg_step);
//
//        res.emplace_back(std::move(sub_query_plan));
//    }
//    else
//    {
//        /// require Hash
//        SubQueryPlan sub_query_plan;
//        sub_query_plan.addStep(partial_agg_step);
//        sub_query_plan.addStep(merge_agg_step);
//
//        res.emplace_back(std::move(sub_query_plan));
//    }
//
//    return res;
//}
//
//}
