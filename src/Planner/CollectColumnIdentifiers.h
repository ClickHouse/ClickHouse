#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Planner/Planner.h>
#include <Planner/TableExpressionData.h>

namespace DB
{

/** Collect all top level column identifiers from query tree node.
  * Top level column identifiers are in the SELECT list or GROUP BY/ORDER BY/WHERE/HAVING clause, but not in child nodes of join tree.
  * For example, in the following query:
  * SELECT sum(b) FROM (SELECT x AS a, y AS b FROM t) AS t1 JOIN t2 ON t1.a = t2.key GROUP BY t2.y
  * The top level column identifiers are: `t1.b`, `t2.y`
  *
  * There is precondition that table expression data is collected in planner context.
  */
ColumnIdentifierSet collectTopLevelColumnIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context);

void collectTopLevelColumnIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context, ColumnIdentifierSet & out);

}

