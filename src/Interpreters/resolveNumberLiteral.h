#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Resolve a number literal used as a function argument, given the type of a sibling argument. The
/// spelling of the literal decides its value, the sibling decides its type: compared with a `Decimal`
/// the literal is parsed from its text into a wide `Decimal`, in arithmetic with a `Decimal` or next to
/// a float it is a float, and an integer literal that fits an integer sibling takes that type. Otherwise
/// the literal keeps its default type (`Float64`, or a wide integer for a value that does not fit
/// `UInt64`/`Int64`). Null type when even that fails.
std::pair<Field, DataTypePtr> resolveNumberLiteralForFunction(
    const String & text, const DataTypePtr & reference_type, bool is_comparison);

/// Same, for the literals inside a `Tuple`, `Array` or `Map`, against the matching element of
/// `reference_type`. Null type when no element gained one, so the caller keeps the default.
std::pair<Field, DataTypePtr> resolveNestedNumberLiteralsForComparison(
    const Field & field, const DataTypePtr & reference_type);

/// Resolve one element on the right of `IN` against the left-hand side type. The element can be a
/// bare literal (`x IN (1.1)`) or a tuple/array holding them (`(x, y) IN ((1.1, 2))`).
std::pair<Field, DataTypePtr> resolveNumberLiteralSetElement(
    const Field & element, const DataTypePtr & left_type);

/// Whether the field is a number literal, or a container holding one.
bool fieldHasNumberLiteral(const Field & field);

/// Resolve every number literal in the field to `Float64`, as before `enable_context_typed_number_literals`.
Field resolveNumberLiteralsAsFloat64(const Field & field);

/// Same, for every literal in the AST.
void resolveNumberLiteralsAsFloat64(IAST & ast);

}
