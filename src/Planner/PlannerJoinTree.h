#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/SelectQueryOptions.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Planner/PlannerContext.h>

namespace DB
{

using UsefulSets = std::unordered_set<FutureSetPtr>;

struct JoinTreeQueryPlan
{
    QueryPlan query_plan;
    QueryProcessingStage::Enum stage{}; // stage till query plan has been built
    std::set<std::string> used_row_policies{};
    UsefulSets useful_sets{};
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping{};
    /// Constant columns the storage returned (ALIAS columns excluded). The expression chain keeps
    /// them flowing rather than fold-and-drop them, so a distributed shard delivers the constants the
    /// initiator expects at the stage boundary.
    NameSet source_constants{};
    /// Duplicate-ALIAS GROUP BY keys the shard collapsed into a single key before computing its two-level bucket
    /// numbers (see `buildShardCollapseFanOut`). Maps each duplicate key column name to the representative (first)
    /// key column it duplicates. A distributed aggregation merge must bucket by only the representative keys so its
    /// two-level bucketing matches the shard's; otherwise equal groups from different shards can land in different
    /// buckets and never merge (wrong results). Empty when no such collapse fed an aggregation.
    std::unordered_map<String, String> shard_collapse_duplicate_keys{};
};

/// Turn parallel replicas off for a query joining multiple tables when
/// `parallel_replicas_for_queries_with_multiple_tables` is disabled.
/// It has to run before the parallel-replicas compatibility checks of `Planner::buildPlanForQueryNode`:
/// those checks throw with `enable_parallel_replicas = 2`, and a query for which parallel replicas are
/// already meant to be off must not surface a parallel-replicas-only exception.
void disableParallelReplicasForMultipleTablesQueryIfNeeded(const QueryTreeNodePtr & query_node, const PlannerContextPtr & planner_context);

/// Build JOIN TREE query plan for query node
JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context);

}
