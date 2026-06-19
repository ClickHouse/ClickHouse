#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

ASTPtr getASTForExternalDatabaseFromQueryTree(ContextPtr context, const QueryTreeNodePtr & query_tree, const ColumnSourceNodePtr & table_expression);

}
