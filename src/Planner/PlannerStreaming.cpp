#include <Planner/PlannerStreaming.h>

#include <Planner/PlannerContext.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Planner/PlannerQueryProcessingInfo.h>
#include <Planner/QueryAnalysisResult.h>
#include <Planner/Utils.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/traverseQueryTree.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>

#include <DataTypes/IDataType.h>

#include <Core/Settings.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool exact_rows_before_limit;
    extern const SettingsBool extremes;
    extern const SettingsBool use_query_cache;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_STREAM;
}

namespace
{

void validateStreamingQueryNode(const QueryTreeNodePtr & query_tree, const PlannerQueryProcessingInfo & query_processing_info)
{
    const auto & query_node = query_tree->as<QueryNode &>();
    const auto & join_tree = query_node.getJoinTreeNode();
    const auto & settings = query_node.getContext()->getSettingsRef();

    auto reject_if = [](bool condition, std::string_view construct)
    {
        if (condition)
            throw Exception(ErrorCodes::ILLEGAL_STREAM, "{} is not supported over an unbounded STREAM", construct);
    };

    auto join_tree_type = join_tree->getNodeType();
    reject_if(join_tree_type == QueryTreeNodeType::JOIN || join_tree_type == QueryTreeNodeType::CROSS_JOIN, "JOIN");
    reject_if(join_tree_type == QueryTreeNodeType::ARRAY_JOIN, "ARRAY JOIN");
    reject_if(queryHasWithTotalsInAnySubqueryInJoinTree(query_tree), "WITH TOTALS in a subquery");

    bool has_correlated_subquery = false;
    traverseQueryTree(query_tree,
        [&](const QueryTreeNodePtr & parent, const QueryTreeNodePtr &) { return parent == query_tree || !isQueryOrUnionNode(parent); },
        [&](const QueryTreeNodePtr & node) { has_correlated_subquery |= isCorrelatedQueryOrUnionNode(node); });
    reject_if(has_correlated_subquery, "A correlated subquery");

    /// An aggregation is supported only if it emits every group as soon as it is complete, which is the case when
    /// a key of `GROUP BY` is the current time computed for every block. This is decided by the plan optimization
    /// (see `applyCurrentTimeOrderToAggregation`) and checked by `AggregatingStep::requireFlushOnOrderKeyChange`.
    /// Here only the constructs that cannot be aggregated in this way are rejected.
    reject_if(query_node.isGroupByWithTotals(), "WITH TOTALS");
    reject_if(query_node.isGroupByWithRollup() || query_node.isGroupByWithCube() || query_node.isGroupByWithGroupingSets(), "ROLLUP, CUBE or GROUPING SETS");
    reject_if(!query_node.hasGroupBy() && (query_node.hasHaving() || hasAggregateFunctionNodes(query_tree)), "HAVING or an aggregate function without GROUP BY");
    reject_if(query_node.hasWindow() || query_node.hasQualify() || hasWindowFunctionNodes(query_tree), "A window function");
    reject_if(query_node.hasOrderBy(), "ORDER BY");
    reject_if(query_node.isShuffle(), "SHUFFLE");
    reject_if(query_node.isLimitWithTies(), "LIMIT WITH TIES");
    reject_if(query_node.hasLimitAfter() || query_node.hasLimitUntil(), "LIMIT AFTER or LIMIT UNTIL");

    auto is_negative_or_fractional = [](const QueryTreeNodePtr & node) { return node && !isUInt64(node->getResultType()); };
    bool has_negative_or_fractional_limit = is_negative_or_fractional(query_node.getLimit()) || is_negative_or_fractional(query_node.getOffset())
                                         || is_negative_or_fractional(query_node.getLimitByLimit()) || is_negative_or_fractional(query_node.getLimitByOffset());
    reject_if(has_negative_or_fractional_limit, "A negative or fractional LIMIT or OFFSET");

    reject_if(query_node.hasLimit() && settings[Setting::exact_rows_before_limit], "LIMIT with the setting exact_rows_before_limit");
    reject_if(settings[Setting::extremes], "The setting extremes");
    reject_if(settings[Setting::use_query_cache], "The setting use_query_cache");
    reject_if(query_processing_info.getFromStage() != QueryProcessingStage::FetchColumns, "A read up to a stage other than FetchColumns");
    reject_if(query_processing_info.getToStage() != QueryProcessingStage::Complete, "A processing stage other than Complete");
}

}

void buildStreamingPlan(
    QueryPlan & query_plan,
    PlannerContextPtr & planner_context,
    PlannerExpressionsAnalysisResult & expression_analysis_result,
    UsefulSets & useful_sets,
    const QueryTreeNodePtr & query_tree,
    const SelectQueryOptions & select_query_options,
    const PlannerQueryProcessingInfo & query_processing_info,
    const QueryAnalysisResult & query_analysis_result,
    const OrderedMaterializedCTEs & materialized_ctes)
{
    validateStreamingQueryNode(query_tree, query_processing_info);

    const auto & query_node = query_tree->as<QueryNode &>();
    auto & projection = expression_analysis_result.getProjection();

    if (expression_analysis_result.hasWhere())
        addFilterStep(planner_context, query_plan, expression_analysis_result.getWhere(), select_query_options, "WHERE", useful_sets);

    if (expression_analysis_result.hasAggregation())
    {
        auto & aggregation = expression_analysis_result.getAggregation();
        if (aggregation.before_aggregation_actions)
            addExpressionStep(planner_context, query_plan, aggregation.before_aggregation_actions, {}, select_query_options, "Before GROUP BY", useful_sets);

        addAggregationStep(query_plan, query_node, expression_analysis_result, query_analysis_result, planner_context, planner_context->getQueryContext()->getSettingsRef(), {});
        typeid_cast<AggregatingStep &>(*query_plan.getRootNode()->step).requireFlushOnOrderKeyChange();

        if (expression_analysis_result.hasHaving())
            addFilterStep(planner_context, query_plan, expression_analysis_result.getHaving(), select_query_options, "HAVING", useful_sets);
    }

    addExpressionStep(planner_context, query_plan, projection.projection_actions, projection.correlated_subtrees, select_query_options, "Projection", useful_sets);

    if (query_node.isDistinct())
    {
        addDistinctStep(query_plan, query_analysis_result, planner_context, projection.projection_column_names, query_node, false, false);
        typeid_cast<DistinctStep &>(*query_plan.getRootNode()->step).disableExternalDistinct();
    }

    if (expression_analysis_result.hasLimitBy())
    {
        auto & limit_by = expression_analysis_result.getLimitBy();
        addExpressionStep(planner_context, query_plan, limit_by.before_limit_by_actions, {}, select_query_options, "Before LIMIT BY", useful_sets);
        addLimitByStep(query_plan, limit_by, query_analysis_result, planner_context, query_node, false);
    }

    if (query_node.hasLimit())
        addLimitStep(query_plan, query_analysis_result, planner_context, query_node);
    else if (query_node.hasOffset())
        addOffsetStep(query_plan, query_analysis_result);

    addExpressionStep(planner_context, query_plan, projection.project_names_actions, {}, select_query_options, "Project names", useful_sets);
    addAdditionalFilterStepIfNeeded(query_plan, query_node, select_query_options, planner_context, useful_sets);

    if (!select_query_options.only_analyze)
    {
        addBuildSubqueriesForSetsStepIfNeeded(query_plan, select_query_options, planner_context, useful_sets);
        addBuildSubqueriesForMaterializedCTEsIfNeeded(query_plan, select_query_options, materialized_ctes);
    }
}

}
