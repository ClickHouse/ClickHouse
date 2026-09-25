#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <optional>


namespace DB
{

class ASTFunction;
class ASTSelectQuery;

/// The result names an `ARRAY JOIN` clause of the query introduces: the alias of every `ARRAY JOIN`
/// expression, or its column name when it has none. An identifier with such a name denotes the
/// array-joined element, not the source column of the same name, and `ARRAY JOIN arr AS value` is
/// allowed even when `value` already is a source column.
NameSet getArrayJoinResultNames(const ASTSelectQuery & select_query);

/// The arguments of a function call in the shape `IFunctionBase` and `IFunctionOverloadResolver`
/// methods expect: the result type of every argument, and its constant column where it has one.
/// Whether a function is injective can depend on them - `toString` of a date-time in a time zone
/// with a UTC offset transition is not injective, and neither is `toString(x, NULL)` - so a caller
/// that can resolve them has no reason to pass nothing.
///
/// Only what the AST alone decides is resolved: a plain identifier that names a source column, and
/// a literal. For anything else - including a compound identifier, which may name a joined column
/// or a subcolumn rather than the source column of the same short name, and an identifier that
/// may name an `ARRAY JOIN` result (`array_join_result_names`, see `getArrayJoinResultNames`) -
/// the answer is undecidable at this point, and `std::nullopt` is returned so that the caller
/// claims nothing.
std::optional<ColumnsWithTypeAndName> tryGetASTFunctionArgumentColumns(
    const ASTFunction & function, const NamesAndTypesList & source_columns, const NameSet & array_join_result_names);

}
