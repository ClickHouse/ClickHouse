#pragma once

#include <Planner/PlannerContext.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

struct SelectQueryOptions;

/** Collect prepared sets and sets for subqueries that are necessary to execute IN function and its variations.
  * Collected sets are registered in planner context.
  */
void collectSets(QueryTreeNodePtr node, PlannerContext & planner_context);

}
