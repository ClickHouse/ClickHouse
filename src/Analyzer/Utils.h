#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/// Returns true if node part of root tree, false otherwise
bool isNodePartOfTree(const IQueryTreeNode * node, const IQueryTreeNode * root);

/// Returns true if function name is name of IN function or its variations, false otherwise
bool isNameOfInFunction(const std::string & function_name);

/** Add table expression in tables in select query children.
  * If table expression node is not of identifier node, table node, query node, table function node, join node or array join node type throws logical error exception.
  */
void addTableExpressionOrJoinIntoTablesInSelectQuery(ASTPtr & tables_in_select_query_ast, const QueryTreeNodePtr & table_expression);

/// Extract table, table function query, union from join tree
QueryTreeNodes extractTableExpressions(const QueryTreeNodePtr & join_tree_node);

}
