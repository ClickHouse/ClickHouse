#pragma once

#include <optional>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SelectQueryInfo.h>
#include "Planner/PlannerExpressionAnalysis.h"

namespace DB
{

class QueryNode;

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Planner
{
public:
    /// Initialize planner with query tree after analysis phase
    Planner(
        const QueryTreeNodePtr & query_tree_,
        SelectQueryOptions & select_query_options_,
        bool qualify_column_names = true
    );

    /// Initialize planner with query tree after query analysis phase and global planner context
    Planner(const QueryTreeNodePtr & query_tree_,
        SelectQueryOptions & select_query_options_,
        GlobalPlannerContextPtr global_planner_context_);

    /// Initialize planner with query tree after query analysis phase and planner context
    Planner(const QueryTreeNodePtr & query_tree_,
        SelectQueryOptions & select_query_options_,
        PlannerContextPtr planner_context_);

    const QueryPlan & getQueryPlan() const
    {
        return query_plan;
    }

    QueryPlan & getQueryPlan()
    {
        return query_plan;
    }

    const std::set<std::string> & getUsedRowPolicies() const
    {
        return used_row_policies;
    }

    void buildQueryPlanIfNeeded();

    QueryPlan && extractQueryPlan() &&
    {
        return std::move(query_plan);
    }

    SelectQueryInfo buildSelectQueryInfo() const;

    void addStorageLimits(const StorageLimitsList & limits);

    PlannerContextPtr getPlannerContext() const
    {
        return planner_context;
    }

    /// We support mapping QueryNode -> QueryPlanStep (the last step added to plan from this query)
    /// It is useful for parallel replicas analysis.
    using QueryNodeToPlanStepMapping = std::unordered_map<const QueryNode *, const QueryPlan::Node *>;
    const QueryNodeToPlanStepMapping & getQueryNodeToPlanStepMapping() const { return query_node_to_plan_step_mapping; }
    const PlannerExpressionsAnalysisResult & getExpressionAnalysisResult() const { return expression_analysis_result.value(); }

private:
    void buildPlanForUnionNode();

    void buildPlanForQueryNode();

    QueryTreeNodePtr query_tree;
    SelectQueryOptions & select_query_options;
    PlannerContextPtr planner_context;
    std::optional<PlannerExpressionsAnalysisResult> expression_analysis_result;
    QueryPlan query_plan;
    StorageLimitsList storage_limits;
    std::set<std::string> used_row_policies;
    QueryNodeToPlanStepMapping query_node_to_plan_step_mapping;
};

}
