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

/// Extract table, table function, query, union from join tree
QueryTreeNodes extractTableExpressions(const QueryTreeNodePtr & join_tree_node);

/// Extract left table expression from join tree
QueryTreeNodePtr extractLeftTableExpression(const QueryTreeNodePtr & join_tree_node);

/** Build table expressions stack that consists from table, table function, query, union, join, array join from join tree.
  *
  * Example: SELECT * FROM t1 INNER JOIN t2 INNER JOIN t3.
  * Result table expressions stack:
  * 1. t1 INNER JOIN t2 INNER JOIN t3
  * 2. t3
  * 3. t1 INNER JOIN t2
  * 4. t2
  * 5. t1
  */
QueryTreeNodes buildTableExpressionsStack(const QueryTreeNodePtr & join_tree_node);

/** Returns true if nested identifier can be resolved from compound type.
  * Compound type can be tuple or array of tuples.
  *
  * Example: Compound type: Tuple(nested_path Tuple(nested_path_2 UInt64)). Nested identifier: nested_path_1.nested_path_2.
  * Result: true.
  */
bool nestedIdentifierCanBeResolved(const DataTypePtr & compound_type, IdentifierView nested_identifier);

/** Assert that there are no function nodes with specified function name in node children.
  * Do not visit subqueries.
  */
void assertNoFunctionNodes(const QueryTreeNodePtr & node,
    std::string_view function_name,
    int exception_code,
    std::string_view exception_function_name,
    std::string_view exception_place_message);

/** Returns true if there is function node with specified function name in node children, false otherwise.
  * Do not visit subqueries.
  */
bool hasFunctionNode(const QueryTreeNodePtr & node, std::string_view function_name);

/** Replace columns in node children.
  * If there is column node and its source is specified table expression node and there is
  * node for column name in map replace column node with node from map.
  * Do not visit subqueries.
  */
void replaceColumns(QueryTreeNodePtr & node,
    const QueryTreeNodePtr & table_expression_node,
    const std::unordered_map<std::string, QueryTreeNodePtr> & column_name_to_node);

}
