#pragma once

#include <Parsers/IAST_fwd.h>

#include <span>
#include <string_view>

namespace DB
{

/// Remove the named settings (both the `name = value` and the `name = DEFAULT` forms) from the
/// query-level SETTINGS carriers that InterpreterSetQuery::applySettingsFromQuery reads back onto the
/// query context, and detach any of those SETTINGS nodes that becomes empty from its owner (so the
/// formatter does not emit a bare `SETTINGS` keyword that fails to re-parse).
///
/// The server-side AST fuzzer pins resource-limit settings on a throwaway context before running a
/// fuzzed query, but executeQueryImpl re-applies the query's own SETTINGS on top of the context
/// (InterpreterSetQuery::applySettingsFromQuery). A seed/fuzzed `SETTINGS max_rows_to_read = 0` (etc.)
/// would otherwise lift the guard, so the fuzzer strips those settings from the AST first.
///
/// The carriers handled are exactly those applySettingsFromQuery re-applies: the SELECT clause, the
/// INSERT clause, the trailing query clause of any ASTQueryWithOutput (SELECT-UNION, EXPLAIN, SHOW,
/// CREATE ... AS SELECT, ...), the CREATE storage clause, and the BACKUP/RESTORE clause. Two of these
/// are not reachable by a plain `children` walk or were left half-handled before: BACKUP/RESTORE keep
/// their settings outside `children`, and the storage clause needs the same empty-node pruning as the
/// others. SETTINGS in positions that do not feed the query context (refresh strategy, dictionary
/// layout, column declarations, standalone SET) are deliberately left untouched.
///
/// Pruning matters because ASTSelectQuery, ASTInsertQuery, ASTQueryWithOutput, ASTStorage and
/// ASTBackupQuery formatters all print `SETTINGS ` whenever their slot is non-null, so a clause that
/// held only stripped settings would serialize to a trailing bare `SETTINGS` that throws on re-parse.
void removeSettingsFromQuery(const ASTPtr & ast, std::span<const std::string_view> setting_names);

/// Like removeSettingsFromQuery, but touches only the SETTINGS carriers of the top-level query itself:
/// the INSERT clause, the trailing clause of the (top-level) SELECT-UNION, and the SETTINGS clause of
/// each first-order SELECT of that union tree. It does not descend into subqueries, table expressions
/// or table functions, so a SETTINGS clause the user wrote inside a nested subquery (for example the
/// documented leaf-node pattern `view(SELECT ... SETTINGS max_execution_time = 10)`) is preserved.
///
/// Use this variant when the goal is to stop the query text from re-applying the named settings over
/// the context shipped alongside it (InterpreterSetQuery::applySettingsFromQuery and the interpreter of
/// the top-level SELECT only read these top-level carriers), rather than to erase the settings from the
/// query wholesale. Empty SETTINGS nodes are pruned the same way as in removeSettingsFromQuery.
void removeSettingsFromQueryTopLevel(const ASTPtr & ast, std::span<const std::string_view> setting_names);

}
