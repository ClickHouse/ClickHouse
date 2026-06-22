#pragma once

#include <Parsers/IAST_fwd.h>

#include <span>
#include <string_view>

namespace DB
{

/// Remove the named settings from every SETTINGS clause embedded in the query AST, and detach any
/// SETTINGS node that becomes empty from its owner (so the formatter does not emit a bare `SETTINGS`
/// keyword that fails to re-parse).
///
/// The server-side AST fuzzer pins resource-limit settings on a throwaway context before running a
/// fuzzed query, but executeQueryImpl re-applies the query's own SETTINGS on top of the context
/// (InterpreterSetQuery::applySettingsFromQuery). A seed/fuzzed `SETTINGS max_rows_to_read = 0` (etc.)
/// would otherwise lift the guard, so the fuzzer strips those settings from the AST first. When a
/// clause held only stripped settings the SETTINGS node is left empty; ASTSelectQuery, ASTInsertQuery
/// and ASTQueryWithOutput formatters all print `SETTINGS ` whenever the node pointer exists, so an
/// empty node serializes to a trailing `SETTINGS` that throws on re-parse. Pruning the empty node from
/// its owner keeps the re-serialized query valid and executable.
void removeSettingsFromQuery(const ASTPtr & ast, std::span<const std::string_view> setting_names);

}
