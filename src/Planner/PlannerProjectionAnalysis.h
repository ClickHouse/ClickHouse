#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

/// The analysis of the query of a projection. It keeps the rules of the old analysis that the existing parts depend on:
/// the names of the stored columns, the expressions over the columns of the table, and the order of the keys.

/// the context for the analysis of the query of a projection: the settings of the session do not change the meaning of the query
ContextMutablePtr createProjectionAnalysisContext(const ASTPtr & query_ast, const ContextPtr & context);

/// Builds the query tree of the `SELECT` of a projection over `source` and runs the analysis passes on it.
/// With `only_analyze` a scalar subquery is not calculated, it gets a default value.
QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context, bool only_analyze = true);

/// the name that a projection part stores the expression under: the name that the old analysis gave to the expression
String getStoredName(const QueryTreeNodePtr & node, const ContextPtr & context);

/// A scalar subquery is calculated one time, and the stored query keeps its value.
void storeValuesOfScalarSubqueries(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context);

}
