#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/SelectQueryOptions.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Planner/PlannerContext.h>

namespace DB
{

/// Build query plan for query JOIN TREE node
QueryPlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context);

}
