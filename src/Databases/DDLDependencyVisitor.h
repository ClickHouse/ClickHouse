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
/// @param current_database - the database against which the unqualified names of the query of a view are resolved
/// @param execution_database - the database against which the unqualified names in the column expressions, in the table engine
///     with its arguments, and the names of `joinGet`, `dictGet` and the calls of parameterized views in the query of a view
///     are resolved: they are evaluated in the context of a query. `current_database` if not set
/// @param enable_global_with_statement - the value of `enable_global_with_statement` the query runs with where its `SETTINGS`
///     clauses do not set it. If not set, the value is the one of the session which reads the query, so a name is a table
///     if it is a table with either value. Set it to check what the query reads in a given session.
CreateQueryDependencies getDependenciesFromCreateQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & current_database, bool can_throw = false, bool validate_current_database = true, size_t max_expanded_ast_elements = 0, const std::optional<String> & execution_database = {}, std::optional<bool> enable_global_with_statement = {});

/// Returns a list of all tables explicitly referenced in the select query specified as a dictionary source.
TableNamesSet getDependenciesFromDictionaryNestedSelectQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & select_query, const String & current_database, bool can_throw = false);

}
