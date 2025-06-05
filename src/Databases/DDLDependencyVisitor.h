#pragma once

#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <unordered_set>


namespace DB
{
using TableNamesSet = std::unordered_set<QualifiedTableName>;

/// Returns a list of all tables explicitly referenced in the create query of a specified table.
/// For example, a column default expression can use dictGet() and thus reference a dictionary.
/// Does not validate AST, works a best-effort way.
TableNamesSet getDependenciesFromCreateQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & current_database);

/// Returns a list of all tables explicitly referenced in the select query specified as a dictionary source.
TableNamesSet getDependenciesFromDictionaryNestedSelectQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & select_query, const String & current_database);

}
