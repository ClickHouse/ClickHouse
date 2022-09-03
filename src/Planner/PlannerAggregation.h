#pragma once

#include <Analyzer/IQueryTreeNode.h>

#include <Planner/PlannerContext.h>

#include <Processors/QueryPlan/AggregatingStep.h>

namespace DB
{

/** Resolve GROUPING functions in query node.
  * GROUPING function is replaced with specialized GROUPING function based on GROUP BY modifiers.
  * For ROLLUP, CUBE, GROUPING SETS specialized GROUPING function take special __grouping_set column as argument.
  */
void resolveGroupingFunctions(QueryTreeNodePtr & query_node,
    const Names & aggregation_keys,
    const GroupingSetsParamsList & grouping_sets_parameters_list,
    const PlannerContext & planner_context);

}
