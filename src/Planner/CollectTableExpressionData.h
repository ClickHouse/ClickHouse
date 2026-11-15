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
void collectTableExpressionData(QueryTreeNodePtr & query_node, PlannerContextPtr & planner_context);

/** Collect source columns for expression node.
  * Collected source columns are registered in planner context.
  *
  * ALIAS table column nodes are registered in table expression data and replaced in query tree with inner alias expression.
  */
void collectSourceColumns(QueryTreeNodePtr & expression_node, PlannerContextPtr & planner_context, bool keep_alias_columns = true);

/** Register sets, then collect source columns for expression node, in that order.
  *
  * The order is required: collectSourceColumns expands ALIAS column expressions through
  * PlannerActionsVisitor, which resolves IN operators via PlannerContext::getPreparedSets;
  * the sets must therefore be registered by collectSets beforehand, otherwise an IN inside
  * an ALIAS expression throws "No set is registered for key".
  */
void collectSetsAndSourceColumns(QueryTreeNodePtr & expression_node, PlannerContextPtr & planner_context, bool keep_alias_columns = true);

}
