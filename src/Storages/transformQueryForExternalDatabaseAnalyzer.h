#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

ASTPtr getASTForExternalDatabaseFromQueryTree(ContextPtr context, const QueryTreeNodePtr & query_tree, const TableExpressionNodePtr & table_expression);

}
