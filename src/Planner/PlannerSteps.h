#pragma once

#include <Planner/PlannerContext.h>
#include <Planner/PlannerExpressionAnalysis.h>

namespace DB
{

class PlannerQueryProcessingInfo;
class QueryNode;
class QueryPlan;
struct QueryAnalysisResult;
struct Settings;

using UsefulSets = std::unordered_set<FutureSetPtr>;

FiltersForTableExpressionMap collectFiltersForAnalysis(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options, const ActionsDAG * post_filter);

/// A step that allows it may evaluate an `IN` with a join added below the step instead of a set. A join does not keep the
/// order of the rows it reads, so no step after the sorting for `ORDER BY` or the shuffle for `SHUFFLE` allows it.
void addExpressionStep(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    ActionsAndProjectInputsFlagPtr & expression_actions,
    const CorrelatedSubtrees & correlated_subtrees,
    const SelectQueryOptions & select_query_options,
    std::string_view step_description,
    UsefulSets & useful_sets,
    bool allow_in_to_join);

void addFilterStep(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    FilterAnalysisResult & filter_analysis_result,
    const SelectQueryOptions & select_query_options,
    std::string_view step_description,
    UsefulSets & useful_sets,
    bool allow_in_to_join);

void addAggregationStep(QueryPlan & query_plan,
    const QueryNode & query_node,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const Settings & settings,
    std::optional<UInt64> trivial_group_by_limit);

void addMergingAggregatedStep(QueryPlan & query_plan,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const std::unordered_map<String, String> & shard_collapse_duplicate_keys);

void addTotalsHavingStep(QueryPlan & query_plan,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const SelectQueryOptions & select_query_options,
    const QueryNode & query_node,
    UsefulSets & useful_sets);

void addCubeOrRollupStepIfNeeded(QueryPlan & query_plan,
    const AggregationAnalysisResult & aggregation_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node);

void addDistinctStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const Names & column_names,
    const QueryNode & query_node,
    bool before_order,
    bool pre_distinct);

void addSortingStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context);

void addShuffleStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & query_processing_info);

void preserveShuffleKeyColumn(ActionsDAG & actions, const QueryPlan & query_plan);

void addMergeSortingStep(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    std::string_view description);

void addWithFillStepIfNeeded(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    SortAnalysisResult & sort_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    const SelectQueryOptions & select_query_options,
    UsefulSets & useful_sets);

void addLimitByStep(
    QueryPlan & query_plan,
    const LimitByAnalysisResult & limit_by_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    bool do_not_skip_offset);

void addLimitRangeStep(
    QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node,
    const NameToNameMap & interpolate_target_column_names,
    UsefulSets & useful_sets);

bool addPreliminaryLimitOptimizationStepIfNeeded(QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryTreeNodePtr & query_tree,
    const SelectQueryOptions & select_query_options);

void addPreliminarySortOrDistinctOrLimitStepsIfNeeded(
    QueryPlan & query_plan,
    PlannerExpressionsAnalysisResult & expressions_analysis_result,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryTreeNodePtr & query_tree,
    const SelectQueryOptions & select_query_options,
    UsefulSets & useful_sets);

void addWindowSteps(QueryPlan & query_plan,
    const PlannerContextPtr & planner_context,
    WindowAnalysisResult & window_analysis_result,
    size_t max_step_description_length);

void addLimitStep(
    QueryPlan & query_plan,
    const QueryAnalysisResult & query_analysis_result,
    const PlannerContextPtr & planner_context,
    const QueryNode & query_node);

void addExtremesStepIfNeeded(QueryPlan & query_plan, const PlannerContextPtr & planner_context);

void addOffsetStep(QueryPlan & query_plan, const QueryAnalysisResult & query_analysis_result);

void addBuildSubqueriesForSetsStepIfNeeded(
    QueryPlan & query_plan,
    const SelectQueryOptions & select_query_options,
    const PlannerContextPtr & planner_context,
    const UsefulSets & useful_sets);

void addAdditionalFilterStepIfNeeded(QueryPlan & query_plan,
    const QueryNode & query_node,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context,
    UsefulSets & useful_sets
);

void addRowCountMarkerStepIfNeeded(QueryPlan & query_plan, const PlannerContextPtr & planner_context);

}
