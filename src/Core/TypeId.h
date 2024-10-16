#pragma once

#include <Core/Types_fwd.h>

namespace DB
{

/// @note Except explicitly described you should not assume on TypeIndex numbers and/or their orders in this enum.
enum class TypeIndex : uint8_t
{
    Nothing = 0,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Date,
    Date32,
    DateTime,
    DateTime64,
    String,
    FixedString,
    Enum8,
    Enum16,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    UUID,
    Array,
    Tuple,
    Set,
    Interval,
    Nullable,
    Function,
    AggregateFunction,
    LowCardinality,
    Map,
    ObjectDeprecated,
    Object,
    IPv4,
    IPv6,
    JSONPaths,
    Variant,
    Dynamic
};

/**
 * Obtain TypeIndex value from real type if possible.
 *
 * Returns TypeIndex::Nothing if type was not present in TypeIndex;
 * Returns TypeIndex element otherwise.
 *
 * @example TypeToTypeIndex<UInt8> == TypeIndex::UInt8
 * @example TypeToTypeIndex<MySuperType> == TypeIndex::Nothing
 */
template <class T> inline constexpr TypeIndex TypeToTypeIndex = TypeIndex::Nothing;

template <TypeIndex index> struct TypeIndexToTypeHelper : std::false_type {};

/**
 * Obtain real type from TypeIndex if possible.
 *
 * Returns a type alias if is corresponds to TypeIndex value.
 * Yields a compiler error otherwise.
 *
 * @example TypeIndexToType<TypeIndex::UInt8> == UInt8
 */
template <TypeIndex index> using TypeIndexToType = typename TypeIndexToTypeHelper<index>::T;
template <TypeIndex index> constexpr bool TypeIndexHasType = TypeIndexToTypeHelper<index>::value;

#define TYPEID_MAP(_A) \
    template <> inline constexpr TypeIndex TypeToTypeIndex<_A> = TypeIndex::_A; \
    template <> struct TypeIndexToTypeHelper<TypeIndex::_A> : std::true_type { using T = _A; };

TYPEID_MAP(UInt8)
TYPEID_MAP(UInt16)
TYPEID_MAP(UInt32)
TYPEID_MAP(UInt64)
TYPEID_MAP(UInt128)
TYPEID_MAP(UInt256)
TYPEID_MAP(Int8)
TYPEID_MAP(Int16)
TYPEID_MAP(Int32)
TYPEID_MAP(Int64)
TYPEID_MAP(Int128)
TYPEID_MAP(Int256)
TYPEID_MAP(Float32)
TYPEID_MAP(Float64)
TYPEID_MAP(UUID)
TYPEID_MAP(IPv4)
TYPEID_MAP(IPv6)

TYPEID_MAP(Decimal32)
TYPEID_MAP(Decimal64)
TYPEID_MAP(Decimal128)
TYPEID_MAP(Decimal256)
TYPEID_MAP(DateTime64)

TYPEID_MAP(String)

struct Array;
TYPEID_MAP(Array)

#undef TYPEID_MAP

}
