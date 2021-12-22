#pragma once

#include <Core/Types.h>

namespace DB
{
/**
 * Obtain TypeIndex value from real type if possible.
 *
 * Returns TypeIndex::Nothing if type was not present in TypeIndex;
 * Returns TypeIndex element otherwise.
 *
 * @example TypeId<UInt8> == TypeIndex::UInt8
 * @example TypeId<MySuperType> == TypeIndex::Nothing
 */
template <class T> inline constexpr TypeIndex TypeId = TypeIndex::Nothing;

template <TypeIndex index> struct ReverseTypeIdT : std::false_type {};

/**
 * Obtain real type from TypeIndex if possible.
 *
 * Returns a type alias if is corresponds to TypeIndex value.
 * Yields a compiler error otherwise.
 *
 * @example ReverseTypeId<TypeIndex::UInt8> == UInt8
 */
template <TypeIndex index> using ReverseTypeId = typename ReverseTypeIdT<index>::T;
template <TypeIndex index> constexpr bool HasReverseTypeId = ReverseTypeIdT<index>::value;

#define TYPEID_MAP(_A) \
    template <> inline constexpr TypeIndex TypeId<_A> = TypeIndex::_A; \
    template <> struct ReverseTypeIdT<TypeIndex::_A> : std::true_type { using T = _A; };

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

TYPEID_MAP(Decimal32)
TYPEID_MAP(Decimal64)
TYPEID_MAP(Decimal128)
TYPEID_MAP(Decimal256)
TYPEID_MAP(DateTime64)

TYPEID_MAP(String)

struct Array;
TYPEID_MAP(Array)
}
