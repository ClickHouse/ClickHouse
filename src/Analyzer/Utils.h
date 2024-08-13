#pragma once

#include <Analyzer/IQueryTreeNode.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Returns true if node part of root tree, false otherwise
bool isNodePartOfTree(const IQueryTreeNode * node, const IQueryTreeNode * root);

/// Returns true if function name is name of IN function or its variations, false otherwise
bool isNameOfInFunction(const std::string & function_name);

/// Returns true if function name is name of local IN function or its variations, false otherwise
bool isNameOfLocalInFunction(const std::string & function_name);

/// Returns true if function name is name of global IN function or its variations, false otherwise
bool isNameOfGlobalInFunction(const std::string & function_name);

/// Returns global IN function name for local IN function name
std::string getGlobalInFunctionNameForLocalInFunctionName(const std::string & function_name);

/// Add unique suffix to names of duplicate columns in block
void makeUniqueColumnNamesInBlock(Block & block);

/** Build cast function that cast expression into type.
  * If resolve = true, then result cast function is resolved during build, otherwise
  * result cast function is not resolved during build.
  */
QueryTreeNodePtr buildCastFunction(const QueryTreeNodePtr & expression,
    const DataTypePtr & type,
    const ContextPtr & context,
    bool resolve = true);

/// Try extract boolean constant from condition node
std::optional<bool> tryExtractConstantFromConditionNode(const QueryTreeNodePtr & condition_node);

/** Add table expression in tables in select query children.
  * If table expression node is not of identifier node, table node, query node, table function node, join node or array join node type throws logical error exception.
  */
void addTableExpressionOrJoinIntoTablesInSelectQuery(ASTPtr & tables_in_select_query_ast, const QueryTreeNodePtr & table_expression, const IQueryTreeNode::ConvertToASTOptions & convert_to_ast_options);

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
