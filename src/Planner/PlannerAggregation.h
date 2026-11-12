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

/// the same, but a node in node_to_name takes its name from the map
AggregateDescriptions extractAggregateDescriptions(
    const QueryTreeNodes & aggregate_function_nodes,
    const PlannerContext & planner_context,
    QueryTreeNodeToName & node_to_name);

}
