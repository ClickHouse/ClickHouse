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
    /// Names of constant columns produced by the join tree (read + computed alias columns). These are
    /// constants observed from the source that the planner cannot re-create at another plan position,
    /// so the expression chain must keep them flowing rather than fold-and-drop them (otherwise a
    /// distributed shard fails to deliver a column the initiator expects at the stage boundary).
    NameSet source_constants{};
};

/// Build JOIN TREE query plan for query node
JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context);

}
