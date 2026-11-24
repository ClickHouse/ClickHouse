#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

/// analysis of a projection query; keeps the column names and key order that existing parts rely on

/// Builds the query tree of the `SELECT` of a projection over `source` and runs the analysis passes on it.
QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context);

/// the name that a projection part stores the expression under: the name of the expression as the query writes it
String getStoredName(const QueryTreeNodePtr & node);

}
