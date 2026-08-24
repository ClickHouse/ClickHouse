#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ActionsDAG;
class QueryNode;

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Planner
{
public:
    /// Initialize planner with query tree after analysis phase
    Planner(const QueryTreeNodePtr & query_tree_,
        SelectQueryOptions & select_query_options_,
        const ActionsDAG * post_filter_ = nullptr);

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

    void setUsedRowPolicies(std::set<std::string> policies)
    {
        used_row_policies = std::move(policies);
    }

    /// Inject a pre-built query plan (e.g. from the query plan cache).
    /// buildQueryPlanIfNeeded() will skip planning when the plan is already initialized.
    ///
    /// Preconditions (enforced by caller, typically executeQuery.cpp):
    ///   - Must be called before any method that triggers buildQueryPlanIfNeeded().
    ///   - The query must not use parallel replicas (use_parallel_replicas=true); those
    ///     queries are excluded from cache eligibility in Phase 4 because the
    ///     query_plan_with_parallel_replicas_builder lambda operates on ReadFromMergeTree
    ///     nodes which are not present in universalized (cached) plans.
    void setQueryPlan(QueryPlan && plan)
    {
        if (query_plan.isInitialized())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "setQueryPlan called on an already-initialized plan");
        query_plan = std::move(plan);
    }

    void buildQueryPlanIfNeeded();

    QueryPlan && extractQueryPlan() &&
    {
        return std::move(query_plan);
    }

    void addStorageLimits(const StorageLimitsList & limits);

    PlannerContextPtr getPlannerContext() const
    {
        return planner_context;
    }

    /// We support mapping QueryNode -> QueryPlanStep (the last step added to plan from this query)
    /// It is useful for parallel replicas analysis.
    using QueryNodeToPlanStepMapping = std::unordered_map<const QueryNode *, const QueryPlan::Node *>;
    const QueryNodeToPlanStepMapping & getQueryNodeToPlanStepMapping() const { return query_node_to_plan_step_mapping; }

private:
    SelectQueryInfo buildSelectQueryInfo() const;

    void buildPlanForUnionNode();

    void buildPlanForQueryNode();

    LoggerPtr log = getLogger("Planner");
    QueryTreeNodePtr query_tree;
    SelectQueryOptions & select_query_options;
    PlannerContextPtr planner_context;
    QueryPlan query_plan;
    StorageLimitsList storage_limits;
    std::set<std::string> used_row_policies;
    QueryNodeToPlanStepMapping query_node_to_plan_step_mapping;
};

}
