#pragma once

#include <Analyzer/IQueryTreeNode.h>


namespace DB
{

ASTPtr getASTForExternalDatabaseFromQueryTree(const QueryTreeNodePtr & query_tree);

}
