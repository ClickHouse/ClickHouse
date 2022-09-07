#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ColumnTransformers.h>

namespace DB
{

/** Build query tree from AST.
  * AST that represent query ASTSelectWithUnionQuery, ASTSelectQuery.
  * AST that represent a list of expressions ASTExpressionList.
  * AST that represent expression ASTIdentifier, ASTAsterisk, ASTLiteral, ASTFunction.
  */
QueryTreeNodePtr buildQueryTree(ASTPtr query);

}
