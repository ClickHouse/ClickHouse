#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/FunctionNode.h>

#include <Planner/PlannerContext.h>
#include <Planner/PlannerActionsVisitor.h>

#include <Processors/QueryPlan/AggregatingStep.h>

namespace DB
{

/// Extract aggregate descriptions from aggregate function nodes
AggregateDescriptions extractAggregateDescriptions(const QueryTreeNodes & aggregate_function_nodes, const PlannerContext & planner_context);

/// The same, naming the nodes found in `node_to_name` by the map (see `calculateActionNodeName`).
AggregateDescriptions extractAggregateDescriptions(
    const QueryTreeNodes & aggregate_function_nodes,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name,
    bool use_column_identifier_as_action_node_name);

}
