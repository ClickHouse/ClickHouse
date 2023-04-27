#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Analyzer/QueryTreePassManager.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Planner
{
public:
    /// Initialize planner with query tree after analysis phase
    Planner(const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_);

    /// Initialize planner with query tree after query analysis phase and global planner context
    Planner(const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_,
        GlobalPlannerContextPtr global_planner_context_);

    /// Initialize planner with query tree after query analysis phase and planner context
    Planner(const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_,
        PlannerContextPtr planner_context_);

    const QueryPlan & getQueryPlan() const
    {
        return query_plan;
    }

    QueryPlan & getQueryPlan()
    {
        return query_plan;
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

private:
    void buildPlanForUnionNode();

    void buildPlanForQueryNode();

    QueryTreeNodePtr query_tree;
    SelectQueryOptions select_query_options;
    PlannerContextPtr planner_context;
    QueryPlan query_plan;
    StorageLimitsList storage_limits;
};

}
