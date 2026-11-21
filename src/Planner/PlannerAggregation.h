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

/// the aggregator parameters that the settings of the query give
/// `group_by_each_block_no_merge` is passed explicitly instead of being read from the settings: it applies only
/// to the `GROUP BY` of the query itself, not to the other aggregations built from these parameters.
Aggregator::Params getAggregatorParams(
    const ContextPtr & query_context,
    const Names & keys,
    const AggregateDescriptions & aggregates,
    bool overflow_row = false,
    bool group_by_each_block_no_merge = false);

}
