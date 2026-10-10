#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

#include <functional>
#include <optional>
#include <utility>

namespace DB
{

class ASTIdentifier;

/// Whether the qualifier `x` of a qualified matcher `x.*` or `x.COLUMNS(...)` names a column (e.g. of type `Tuple`).
using IsColumnQualifier = std::function<bool(const ASTIdentifier & qualifier)>;

/// The first column matcher (`*`, `t.*`, `COLUMNS(...)`, `t.COLUMNS(...)`) in the expression AST, or nullptr.
///
/// A matcher expands into the columns of the query's table sources, so it has no meaning in an expression that
/// is evaluated over a single table on its own (a row policy, `additional_table_filters`). It can hide behind a
/// SQL UDF that is inlined into the expression later, which is caught by descending into the UDF body with the
/// arguments of the call substituted for its parameters. A matcher inside a nested subquery resolves against that
/// subquery's own tables, so subqueries are skipped.
///
/// A qualified matcher whose qualifier is a column, e.g. `tup.*` for a `Tuple` column `tup`, expands into the
/// elements of that column rather than into table columns, so it is not reported.
ASTPtr findColumnMatcherInExpression(const ASTPtr & ast, const IsColumnQualifier & is_column_qualifier);

/// The rule of `QueryAnalyzer::resolveQualifiedMatcher` for a qualified matcher in an expression over a single table:
/// its qualifier expands into the elements of a column when it is a column of a `Tuple` type (possibly inside
/// `Nullable`, `Array` or `Map`), unless `analyzer_compatibility_prefer_alias_over_subcolumn` is enabled and the
/// qualifier also names the table, in which case it expands into the table columns.
///
/// `try_get_column` looks up a column (or subcolumn) of the table by name, `table_names` are the single-part names
/// under which the table can be referenced (its name, its alias), `database_and_table_name` is the two-part one.
/// A qualifier that starts with one of them, e.g. `t.tup.*` or `db.t.tup.*`, is also looked up as a column without
/// that prefix.
IsColumnQualifier makeTupleColumnQualifierCheck(
    std::function<std::optional<NameAndTypePair>(const String &)> try_get_column,
    NameSet table_names,
    std::optional<std::pair<String, String>> database_and_table_name,
    bool prefer_table_over_column);

}
