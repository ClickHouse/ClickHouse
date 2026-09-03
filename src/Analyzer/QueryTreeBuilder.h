#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ColumnTransformers.h>

namespace DB
{

/** Build query tree from AST.
  * AST that represent query ASTSelectWithUnionQuery, ASTSelectIntersectExceptQuery, ASTSelectQuery.
  * AST that represent a list of expressions ASTExpressionList.
  * AST that represent expression ASTIdentifier, ASTAsterisk, ASTLiteral, ASTFunction.
  *
  * For QUERY and UNION nodes contexts are created with respect to specified SETTINGS.
  */
QueryTreeNodePtr buildQueryTree(ASTPtr query, ContextPtr context);

/// For table function only
class TableFunctionNode;
std::shared_ptr<TableFunctionNode> buildTableFunctionQueryTree(ASTPtr query, ContextPtr context);
}
