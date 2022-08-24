#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Analyzer/QueryTreePassManager.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class Planner : public WithContext
{
public:
    /// Initialize planner with query tree after analysis phase
    Planner(const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context_);

    /// Initialize interpreter with query tree after query analysis phase and global planner context
    Planner(const QueryTreeNodePtr & query_tree_,
        const SelectQueryOptions & select_query_options_,
        ContextPtr context_,
        GlobalPlannerContextPtr global_planner_context_);

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

private:
    QueryTreeNodePtr query_tree;
    QueryPlan query_plan;
    SelectQueryOptions select_query_options;
    PlannerContextPtr planner_context;
};

}
