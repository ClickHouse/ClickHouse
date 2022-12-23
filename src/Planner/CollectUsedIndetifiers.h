#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Planner/Planner.h>
#include <Planner/TableExpressionData.h>

namespace DB
{

using ColumnIdentifierSet = std::unordered_set<ColumnIdentifier>;

ColumnIdentifierSet collectUsedIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context);
void collectUsedIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context, ColumnIdentifierSet & out);


}

