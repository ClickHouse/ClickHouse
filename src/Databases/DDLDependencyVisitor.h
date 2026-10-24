#pragma once

#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <unordered_set>


namespace DB
{
using TableNamesSet = std::unordered_set<QualifiedTableName>;

struct CreateQueryDependencies
{
    TableNamesSet dependencies;
    std::optional<StorageID> mv_to_dependency;
    std::optional<StorageID> mv_from_dependency;
    TableNamesSet plain_view_dependencies;
};


/// Returns a list of all tables explicitly referenced in the create query of a specified table.
/// For example, a column default expression can use dictGet() and thus reference a dictionary.
/// Does not validate AST, works a best-effort way.
/// @param validate_current_database - if false, skips validation that database exists when setting current database (for bootstrap/restore scenarios)
/// @param max_expanded_ast_elements - bounds the expansion of `WITH` elements in the query of a materialized view, zero means no limit
/// @param engine_arguments_database - the database the arguments of the table engine are resolved against, `current_database` if not set
CreateQueryDependencies getDependenciesFromCreateQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & current_database, bool can_throw = false, bool validate_current_database = true, size_t max_expanded_ast_elements = 0, const std::optional<String> & engine_arguments_database = {});

/// Returns a list of all tables explicitly referenced in the select query specified as a dictionary source.
TableNamesSet getDependenciesFromDictionaryNestedSelectQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & select_query, const String & current_database, bool can_throw = false);

}
