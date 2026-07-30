#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class TableNode;

/// Collect list of selected columns for a specific table from query tree.
/// Works similarly to collectTableExpressionData, but the difference is that here
/// we also go through all subqueries.
std::vector<String> collectSelectedColumnsFromTable(QueryTreeNodePtr & query_tree, const StorageID & storage_id, const ContextPtr & context);

/// Same as above, but matches columns by a specific `TableNode` instance rather than by `StorageID`.
/// Use this when the same physical table is referenced from several scopes (e.g. directly and inside
/// an inlined view): each `TableNode` instance then owns only the columns selected from it in its own scope.
std::vector<String> collectSelectedColumnsForTableNode(QueryTreeNodePtr & query_tree, const TableNode & table_node, const ContextPtr & context);

/// Same as above for a table expression that is not necessarily a `TableNode`: a parameterized view is
/// resolved into a `TableFunctionNode` that owns the view's storage, and the columns selected from it
/// have that node as their column source.
std::vector<String> collectSelectedColumnsForTableExpression(
    QueryTreeNodePtr & query_tree, const IQueryTreeNode & table_expression, const StorageID & storage_id, const ContextPtr & context);

}
