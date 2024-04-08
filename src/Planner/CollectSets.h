#pragma once

#include <Planner/PlannerContext.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

struct SelectQueryOptions;

/** Collect prepared sets and sets for subqueries that are necessary to execute IN function and its variations.
  * Collected sets are registered in planner context.
  */
void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context);

/// Build subquery which we execute for IN function.
/// It is needed to support `IN table` case.
QueryTreeNodePtr makeExecutableSubqueryForIn(const QueryTreeNodePtr & in_second_argument, const ContextPtr & context);

}
