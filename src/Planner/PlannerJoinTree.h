#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/SelectQueryOptions.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Planner/PlannerContext.h>

namespace DB
{

struct JoinTreeQueryPlan
{
    QueryPlan query_plan;
    QueryProcessingStage::Enum from_stage;
    std::set<std::string> used_row_policies{};
    std::vector<ActionsDAGPtr> actions_dags{};
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping{};
};

/// Build JOIN TREE query plan for query node
JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context);

}
