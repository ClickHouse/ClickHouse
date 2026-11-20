#pragma once

#include <Planner/CollectMaterializedCTE.h>
#include <Planner/Planner.h>
#include <Planner/PlannerSteps.h>

namespace DB
{

class PlannerExpressionsAnalysisResult;
class PlannerQueryProcessingInfo;

void buildStreamingPlan(
    QueryPlan & query_plan,
    PlannerContextPtr & planner_context,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    UsefulSets & useful_sets,
    const QueryTreeNodePtr & query_tree,
    const SelectQueryOptions & select_query_options,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryAnalysisResult & query_analysis_result,
    const OrderedMaterializedCTEs & materialized_ctes);

}
