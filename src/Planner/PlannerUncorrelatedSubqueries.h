#pragma once

#include <memory>
#include <optional>
#include <unordered_set>

#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct SelectQueryOptions;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

class QueryPlan;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

class GlobalPlannerContext;
using GlobalPlannerContextPtr = std::shared_ptr<GlobalPlannerContext>;

class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;
using UsefulSets = std::unordered_set<FutureSetPtr>;

/// `x IN (subquery)` where the subquery reads nothing from the outer query.
struct UncorrelatedInSubquery
{
    const ActionsDAG::Node * in_node = nullptr;
    QueryTreeNodePtr subquery;
    /// Computes `in_node` and what it reads.
    ActionsDAG actions;
};

bool canRewriteInToJoin(
    const QueryTreeNodePtr & in_node,
    const QueryTreeNodePtr & query_node,
    const PlannerContext & planner_context);

/// Returns an `IN` of `actions` that becomes a join. When one such `IN` is inside the left argument of another, the inner one is returned.
std::optional<UncorrelatedInSubquery> findInnermostUncorrelatedInSubquery(const ActionsDAG & actions, const PlannerContext & planner_context);

/// Adds the steps that compute `in_subquery.actions`, where `in_subquery.in_node` is evaluated with a join with the subquery.
void buildQueryPlanForUncorrelatedInSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    UncorrelatedInSubquery in_subquery,
    const SelectQueryOptions & select_query_options,
    GlobalPlannerContextPtr subquery_global_planner_context,
    UsefulSets & useful_sets);

}
