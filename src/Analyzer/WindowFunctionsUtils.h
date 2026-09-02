#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Collect window function nodes in node children.
  * Do not visit subqueries.
  */
QueryTreeNodes collectWindowFunctionNodes(const QueryTreeNodePtr & node);

/** Collect window function nodes in node children and add them into result.
  * Do not visit subqueries.
  */
void collectWindowFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result);

/** Returns true if there are window function nodes in node children, false otherwise.
  * Do not visit subqueries.
  */
bool hasWindowFunctionNodes(const QueryTreeNodePtr & node);

/** Assert that there are no window function nodes in node children.
  * Do not visit subqueries.
  */
void assertNoWindowFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_window_functions_place_message);

}
