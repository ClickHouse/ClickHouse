#pragma once

#include <memory>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
class PlannerContext;
struct SelectQueryOptions;

/** Collect prepared sets and sets for subqueries that are necessary to execute IN function and its variations.
  * Collected sets are registered in planner context.
  */
void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context);

}
