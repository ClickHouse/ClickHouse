#pragma once
#include <list>
#include <memory>

namespace DB
{

class QueryNode;
class TableNode;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct SelectQueryOptions;

/// Find a query which can be executed with parallel replicas up to WithMergableStage.
/// Returned query will always contain some (>1) subqueries, possibly with joins.
const QueryNode * findQueryForParallelReplicas(const QueryTreeNodePtr & query_tree_node, SelectQueryOptions & select_query_options);

/// Find a table from which we should read on follower replica. It's the left-most table within all JOINs and UNIONs.
const TableNode * findTableForParallelReplicas(const QueryTreeNodePtr & query_tree_node, SelectQueryOptions & select_query_options);

struct JoinTreeQueryPlan;

class PlannerContext;
using PlannerContextPtr = std::shared_ptr<PlannerContext>;

struct StorageLimits;
using StorageLimitsList = std::list<StorageLimits>;

/// Execute QueryNode with parallel replicas up to WithMergableStage and return a plan.
/// This method does not check that QueryNode is valid. Ideally it should be a result of findParallelReplicasQuery.
JoinTreeQueryPlan buildQueryPlanForParallelReplicas(
    const QueryNode & query_node,
    const PlannerContextPtr & planner_context,
    std::shared_ptr<const StorageLimitsList> storage_limits);

}
