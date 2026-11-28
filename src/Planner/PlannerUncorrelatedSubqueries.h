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
    /// Computes the `IN` and what it reads.
    ActionsDAG actions;
};

[[nodiscard]] bool canRewriteInToJoin(const ActionsDAG::Node & in_node, const QueryTreeNodePtr & subquery);

/// Finds an `IN` with a subquery that is not skipped. If one such `IN` is in the left argument of another, it finds the
/// inner one first, because the key of the outer one reads its result.
[[nodiscard]] std::optional<UncorrelatedInSubquery> findInnermostUncorrelatedInSubquery(const ActionsDAG & actions, const NameSet & skipped);

/// Adds the steps that compute the actions of the `IN`, evaluating the `IN` with a join with the plan of its subquery.
void buildQueryPlanForUncorrelatedInSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    UncorrelatedInSubquery in_subquery,
    QueryPlan subquery_plan,
    UsefulSets & useful_sets);

}
