#pragma once
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeInterval.h>
#include <Common/IntervalKind.h>

namespace DB
{

enum class LeastSupertypeOnError : uint8_t
{
    Throw,
    String,
    Null,
    Variant,
};

/** Get data type that covers all possible values of passed data types.
  * If there is no such data type, throws an exception.
  *
  * Examples: least common supertype for UInt8, Int8 - Int16.
  * Examples: there is no least common supertype for Array(UInt8), Int8.
  */
template <LeastSupertypeOnError on_error = LeastSupertypeOnError::Throw>
DataTypePtr getLeastSupertype(const DataTypes & types);

/// Same as above but return String type instead of throwing exception.
/// All types can be cast to String, because they can be serialized to String.
DataTypePtr getLeastSupertypeOrString(const DataTypes & types);

/// Same as getLeastSupertype but in case when there is no supertype for provided types
/// it uses Variant of these types as a supertype. Any type can be cast to a Variant
/// that contains this type.
/// As nested Variants are not allowed, if one of the types is Variant, it's variants
/// are used in the resulting Variant.
/// Examples:
/// (UInt64, String) -> Variant(UInt64, String)
/// (Array(UInt64), Array(String)) -> Variant(Array(UInt64), Array(String))
/// (Variant(UInt64, String), Array(UInt32)) -> Variant(UInt64, String, Array(UInt32))
DataTypePtr getLeastSupertypeOrVariant(const DataTypes & types);

/// Same as above, but when allow_lossy_numeric is set and there is no lossless common
/// type for an all-numeric set of types (e.g. Decimal + Float64, Int64 + Float64),
/// returns the numeric supertype Float64 (matching arithmetic promotion) instead of a
/// Variant. Used by if/multiIf/coalesce/ifNull/array/map under the
/// allow_lossy_numeric_supertype setting. With allow_lossy_numeric = false this is
/// identical to getLeastSupertypeOrVariant(types).
DataTypePtr getLeastSupertypeOrVariant(const DataTypes & types, bool allow_lossy_numeric);

/// Same as above but return nullptr instead of throwing exception.
DataTypePtr tryGetLeastSupertype(const DataTypes & types);

/// If `type` is a Variant whose alternatives are all numeric (after removing any
/// Nullable/LowCardinality wrappers), returns a hint pointing at the
/// `allow_lossy_numeric_supertype` setting, suitable for appending to an aggregate
/// function "illegal type" error. Such Variants are typically produced by
/// if/multiIf/coalesce/array over mixed numeric types. Returns an empty string for
/// any other type, so callers can append it unconditionally.
String getNumericVariantSupertypeHint(const DataTypePtr & type);

using TypeIndexSet = std::unordered_set<TypeIndex>;

template <LeastSupertypeOnError on_error = LeastSupertypeOnError::Throw>
DataTypePtr getLeastSupertype(const TypeIndexSet & types);

DataTypePtr getLeastSupertypeOrString(const TypeIndexSet & types);

DataTypePtr tryGetLeastSupertype(const TypeIndexSet & types);

/// A vector that shows the conversion rates to the next Interval type starting from NanoSecond
static std::vector<int> interval_conversions = {1, 1000, 1000, 1000, 60, 60, 24, 7, 4, 3, 4};

}
