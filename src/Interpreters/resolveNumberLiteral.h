#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Resolve a NumberLiteral to a concrete typed Field for use as a function argument.
///
/// Given the literal text and a reference type from a sibling argument (e.g. a Decimal column),
/// determines the best target type and parses the literal accordingly:
///
/// - For comparison functions (is_comparison=true) with a Decimal reference type and plain
///   decimal notation (no scientific exponent), parses directly from text to Decimal,
///   preserving full precision without Float64 intermediate rounding.
/// - For integer reference types where the literal's default type fits, uses the reference type.
/// - Otherwise falls back to the NumberLiteral's default type (Float64 for decimal-point
///   literals, UInt128/Int128/UInt256/Int256 for big integers).
///
/// Returns {resolved Field, target DataType}. Returns {Null, nullptr} if resolution fails entirely.
std::pair<Field, DataTypePtr> resolveNumberLiteralForFunction(
    const String & text, const DataTypePtr & reference_type, bool is_comparison);

/// Same, for the literals inside a `Tuple` or `Array`, against the matching element of
/// `reference_type`. Null type when no element gained one, so the caller keeps the default.
std::pair<Field, DataTypePtr> resolveNestedNumberLiteralsForComparison(
    const Field & field, const DataTypePtr & reference_type);

/// Whether the field is a number literal, or a container holding one.
bool fieldHasNumberLiteral(const Field & field);

/// Resolve one element on the right of `IN` against the left-hand side type. The element can be a
/// bare literal (`x IN (1.1)`) or a tuple/array holding them (`(x, y) IN ((1.1, 2))`).
std::pair<Field, DataTypePtr> resolveNumberLiteralSetElement(
    const Field & element, const DataTypePtr & left_type);

}
