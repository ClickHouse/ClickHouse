#pragma once
#include <list>
#include <memory>

namespace DB
{

class ITableExpressionNode;
class QueryNode;
struct StorageID;
class TableNode;
class UnionNode;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct SelectQueryOptions;

/// Find a query which can be executed with parallel replicas up to WithMergableStage.
/// Returned query will always contain some (>1) subqueries, possibly with joins.
const QueryNode * findQueryForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options);

/// Find a table from which we should read on follower replica. It's the left-most table within all JOINs and UNIONs.
/// It's either a table, or a table function that is only a reference to a table (`ITableFunction::getReferencedTableID`).
const ITableExpressionNode * findTableForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options);

/// The table each replica is asked about before it joins the read (`RemoteQueryExecutor::main_table`).
///
/// This is the storage the expression resolved to, including for a table function that only references
/// another table. It is tempting to use the referenced table instead, since a hidden inner table is named
/// after a UUID the initiator assigned and a replica that assigned a different one cannot answer about it -
/// but the probe is also what applies `max_replica_delay_for_distributed_queries`, and only a
/// `ReplicatedMergeTree` reports a delay. Naming the outer `TimeSeries` table would make every replica
/// answer "not replicated", i.e. always up to date, and quietly drop the staleness check. Answering about a
/// table a replica may not have costs parallelism; skipping the check costs correctness.
StorageID getStorageIDForParallelReplicas(const ITableExpressionNode & table_expression_node);

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Check whether a resolved storage is eligible for parallel replicas (MergeTree, replication, no FINAL).
bool isTableNodeEligibleForParallelReplicas(const TableNode & table_node, const StoragePtr & storage, const ContextPtr & context);

/// Find a UNION node whose every child query reads from a table eligible for parallel replicas.
/// Used for views with UNION ALL where each branch reads from a separate MergeTree table.
const UnionNode * findTableUnionForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options);

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
