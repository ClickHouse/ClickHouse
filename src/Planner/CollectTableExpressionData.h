#pragma once

#include <Planner/PlannerContext.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Collect table expression data for query node.
  * Collected table expression data is registered in planner context.
  *
  * ALIAS table column nodes are registered in table expression data and replaced in query tree with inner alias expression.
  */
void collectTableExpressionData(QueryTreeNodePtr & query_node, PlannerContext & planner_context);

}
