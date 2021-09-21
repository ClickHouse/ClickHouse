#pragma once

#include <Core/Types.h>

namespace DB
{
namespace detail
{
template <TypeIndex index> struct ReverseTypeIdT : std::false_type {};
template <TypeIndex index> struct ReverseDataTypeIdT : std::false_type {};
}

class DataTypeArray;
class DataTypeDate;
class DataTypeDate32;
class DataTypeString;
class DataTypeFixedString;
class DataTypeUUID;
class DataTypeDateTime;
class DataTypeDateTime64;
template <typename T> class DataTypeEnum;
template <typename T> class DataTypeNumber;
template <is_decimal T> class DataTypeDecimal;

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

/**
 * Obtain real type from TypeIndex if possible.
 *
 * Returns a type alias if is corresponds to TypeIndex value.
 * Yields a compiler error otherwise.
 *
 * @example ReverseTypeId<TypeIndex::UInt8> == UInt8
 */
template <TypeIndex index> using ReverseTypeId = typename detail::ReverseTypeIdT<index>::T;

/**
 * Obtain data type from TypeIndex if possible.
 *
 * Returns a type alias if is corresponds to TypeIndex value.
 * Yields a compiler error otherwise.
 *
 * @example ReverseDataTypeId<TypeIndex::UInt8> == DataTypeNumber<UInt8>
 * @example ReverseDataTypeId<TypeIndex::UUID> == DataTypeUUID
 */
template <TypeIndex index> using ReverseDataTypeId = typename detail::ReverseDataTypeIdT<index>::T;

template <TypeIndex index> constexpr bool HasReverseTypeId = detail::ReverseTypeIdT<index>::value;

#define TYPEID_MAP(_A, _B) \
    template <> inline constexpr TypeIndex TypeId<_A> = TypeIndex::_A; \
    template <> struct detail::ReverseTypeIdT<TypeIndex::_A> : std::true_type { using T = _A; }; \
    template <> struct detail::ReverseDataTypeIdT<TypeIndex::_A> : std::true_type { using T = _B; };

TYPEID_MAP(UInt8,   DataTypeNumber<UInt8>)
TYPEID_MAP(UInt16,  DataTypeNumber<UInt16>)
TYPEID_MAP(UInt32,  DataTypeNumber<UInt32>)
TYPEID_MAP(UInt64,  DataTypeNumber<UInt64>)
TYPEID_MAP(UInt128, DataTypeNumber<UInt128>)
TYPEID_MAP(UInt256, DataTypeNumber<UInt256>)

TYPEID_MAP(Int8,   DataTypeNumber<Int8>)
TYPEID_MAP(Int16,  DataTypeNumber<Int16>)
TYPEID_MAP(Int32,  DataTypeNumber<Int32>)
TYPEID_MAP(Int64,  DataTypeNumber<Int64>)
TYPEID_MAP(Int128, DataTypeNumber<Int128>)
TYPEID_MAP(Int256, DataTypeNumber<Int256>)

TYPEID_MAP(Float32, DataTypeNumber<Float32>)
TYPEID_MAP(Float64, DataTypeNumber<Float64>)

TYPEID_MAP(Decimal32,  DataTypeDecimal<Decimal32>)
TYPEID_MAP(Decimal64,  DataTypeDecimal<Decimal64>)
TYPEID_MAP(Decimal128, DataTypeDecimal<Decimal128>)
TYPEID_MAP(Decimal256, DataTypeDecimal<Decimal256>)

TYPEID_MAP(DateTime64, DataTypeDateTime64)

TYPEID_MAP(String, DataTypeString)

TYPEID_MAP(UUID, DataTypeUUID)

struct Array;
TYPEID_MAP(Array, DataTypeArray)

/// Special cases:
template <> struct detail::ReverseTypeIdT<TypeIndex::Enum8> : std::true_type { using T = Int8; };
template <> struct detail::ReverseTypeIdT<TypeIndex::Enum16> : std::true_type { using T = Int16; };
template <> struct detail::ReverseDataTypeIdT<TypeIndex::Enum8> : std::true_type { using T = DataTypeEnum<Int8>; };
template <> struct detail::ReverseDataTypeIdT<TypeIndex::Enum16> : std::true_type { using T = DataTypeEnum<Int16>; };

template <> struct detail::ReverseDataTypeIdT<TypeIndex::FixedString> : std::true_type { using T = DataTypeFixedString; };

template <> struct detail::ReverseDataTypeIdT<TypeIndex::Date> : std::true_type { using T = DataTypeDate; };
template <> struct detail::ReverseDataTypeIdT<TypeIndex::Date32> : std::true_type { using T = DataTypeDate32; };
template <> struct detail::ReverseDataTypeIdT<TypeIndex::DateTime> : std::true_type { using T = DataTypeDateTime; };
}
