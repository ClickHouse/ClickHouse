#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <optional>


namespace DB
{

class ASTFunction;

/// The arguments of a function call in the shape `IFunctionBase` and `IFunctionOverloadResolver`
/// methods expect: the result type of every argument, and its constant column where it has one.
/// Whether a function is injective can depend on them - `toString` of a date-time in a time zone
/// with a UTC offset transition is not injective, and neither is `toString(x, NULL)` - so a caller
/// that can resolve them has no reason to pass nothing.
///
/// Only what the AST alone decides is resolved: an identifier that names a source column, and a
/// literal. For anything else the answer is undecidable at this point, and `std::nullopt` is
/// returned so that the caller claims nothing.
std::optional<ColumnsWithTypeAndName> tryGetASTFunctionArgumentColumns(
    const ASTFunction & function, const NamesAndTypesList & source_columns);

}
