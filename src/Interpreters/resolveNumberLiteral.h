#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Resolve a number literal used as a function argument, given the type of a sibling argument.
/// In a comparison with a `Decimal` the literal is parsed from its text straight into a wide
/// `Decimal`, so no precision is lost through `Float64`; an integer literal that fits an integer
/// reference type takes that type. Otherwise the literal keeps its default type (`Float64`, or a wide
/// integer for a value that does not fit `UInt64`/`Int64`). Null type when even that fails.
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

}
