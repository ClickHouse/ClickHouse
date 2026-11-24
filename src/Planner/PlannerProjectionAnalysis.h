#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

/// analysis of a projection query; keeps the column names and key order that existing parts rely on

/// keeps in the stored query the settings that change what it means
void storeSettingsOfProjectionQuery(const ASTPtr & query_ast, const ContextPtr & context);

/// session settings do not change the analysis; with `only_analyze` scalar subqueries are not run and get a default value
QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context, bool only_analyze = true);

/// the name a projection part stores this expression under
String getStoredName(const QueryTreeNodePtr & node, const ContextPtr & context);

/// runs scalar subqueries once and keeps their values in the stored query
void storeValuesOfScalarSubqueries(ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context);

/// puts the expression of an alias in its place in the arguments of `if`, as the old analysis did before it changed `if`
void storeExpressionsOfAliasesInIf(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context);

}
