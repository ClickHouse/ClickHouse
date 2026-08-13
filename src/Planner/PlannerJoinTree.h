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
    /// Whether this plan reads through custom-key parallel replicas (the row split does not align with
    /// LIMIT BY / DISTINCT / aggregation keys, so those must be finalized on the initiator).
    bool is_parallel_replicas_custom_key = false;
    std::set<std::string> used_row_policies{};
    UsefulSets useful_sets{};
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping{};
    /// Constant columns the storage returned (ALIAS columns excluded). The expression chain keeps
    /// them flowing rather than fold-and-drop them, so a distributed shard delivers the constants the
    /// initiator expects at the stage boundary.
    NameSet source_constants{};
};

/// Build JOIN TREE query plan for query node
JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context);

}
