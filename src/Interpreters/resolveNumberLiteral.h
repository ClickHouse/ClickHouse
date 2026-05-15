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

}
