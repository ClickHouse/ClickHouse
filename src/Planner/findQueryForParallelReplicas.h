#pragma once
#include <base/types.h>

#include <Core/QualifiedTableName.h>

#include <list>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

namespace DB
{

class QueryNode;
class TableNode;
class UnionNode;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct SelectQueryOptions;

/// Find a query which can be executed with parallel replicas up to WithMergableStage.
/// Returned query will always contain some (>1) subqueries, possibly with joins.
const QueryNode * findQueryForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options);

/// Find a table expression from which we should read on follower replica. It's the left-most table within all JOINs and UNIONs.
/// The result is either a TableNode or a TableFunctionNode (for the `merge` table function).
const IQueryTreeNode * findTableForParallelReplicas(const QueryTreeNodePtr & query_tree_node, const SelectQueryOptions & select_query_options);

/// The same, but without the precondition that this server is a replica reading for an initiator:
/// the initiator uses it on the query it sends to find out which table expression the replicas will
/// designate, and passes it to them in `parallel_replicas_designated_table`.
const IQueryTreeNode * findTableDesignatedForParallelReplicas(const QueryTreeNodePtr & query_tree_node);

/// A name identifying a table expression designated for coordinated reading with parallel replicas,
/// used to compare the designation of an initiator with the designation of a replica. A table function
/// is identified only by its name: the text of its arguments is not guaranteed to survive the
/// rewriting of the query that is sent to the replicas, and a designated table function can only be
/// `merge(...)` anyway.
String parallelReplicasDesignatedTableName(const IQueryTreeNode * table_expression);

/// Serialization of the qualified names of the replicated tables whose replication delay the
/// initiator checked when it selected the replicas (`max_replica_delay_for_distributed_queries`
/// with falling back to stale replicas switched off), for the internal setting
/// `parallel_replicas_freshness_checked_tables`. The set of tables matched by a `Merge` table is
/// enumerated again at reading time and may have grown since that check: a replica reading a
/// replicated table that is absent from this set fails closed instead of serving data nobody
/// verified to be fresh.
String freshnessCheckedTableName(const QualifiedTableName & name);
String serializeFreshnessCheckedTables(const std::vector<QualifiedTableName> & tables);
std::unordered_set<String> parseFreshnessCheckedTables(const String & serialized);

/// A key identifying the `Merge` table expression a child table set belongs to, so that a replica
/// compares the set its own `Merge` table expression resolves to against the set the initiator read
/// for the same table expression, and not against the set of a sibling `Merge` table expression of
/// the same query. It consists of the escaped name of the table (or of the table function) and the
/// escaped alias, separated by `:`; `mergeChildTableSetKeyBaseName` returns the first component,
/// which identifies the table expression when the alias is not comparable (see `resolveStorages`).
String mergeChildTableSetKey(const IQueryTreeNode * table_expression);
String mergeChildTableSetKeyBaseName(const String & key);

/// Serialization of the child table sets of the `Merge` tables (and `merge` table functions) the
/// query reads, one set per `Merge` table expression, for the internal setting
/// `parallel_replicas_merge_child_tables`. When the initiator builds no local plan, the reading
/// coordinator has no pinned snapshot replica and a child table matched by no participating
/// replica would never be announced, so its rows would silently vanish from the result. A replica
/// whose `Merge` table resolves to a child set different from the initiator's set for the same
/// table expression fails closed instead. Every set is serialized as its `mergeChildTableSetKey`,
/// `=`, and the `freshnessCheckedTableName` names joined by `,`, and is terminated by `;`, so an
/// empty string means "no sets" while `key=;` is a single empty set.
struct MergeChildTableSet
{
    String key;
    std::vector<QualifiedTableName> tables;
};

struct ParsedMergeChildTableSet
{
    String key;
    std::set<String> tables;
};

String serializeMergeChildTableSets(const std::vector<MergeChildTableSet> & table_sets);
std::vector<ParsedMergeChildTableSet> parseMergeChildTableSets(const String & serialized);

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
