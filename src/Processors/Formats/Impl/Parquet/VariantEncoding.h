#pragma once

#include <Common/transformEndianness.h>
#include <base/types.h>

#include <algorithm>
#include <bit>

/// Shared constants and types for the Parquet Variant (V3) binary encoding format.
/// Used by both the read path (`VariantBinaryDecoder.cpp`) and write path (`VariantWrite.cpp`).

namespace DB::Parquet
{

constexpr UInt8 VARIANT_BASIC_TYPE_MASK = 0x03;
constexpr UInt8 VARIANT_VALUE_HEADER_SHIFT = 2;
constexpr UInt8 VARIANT_FIELD_OFFSET_SIZE_MINUS_ONE_MASK = 0x03;
constexpr UInt8 VARIANT_FIELD_ID_SIZE_MINUS_ONE_MASK = 0x03;
constexpr UInt8 VARIANT_FIELD_ID_SIZE_MINUS_ONE_SHIFT = 2;
constexpr UInt8 VARIANT_OBJECT_IS_LARGE_SHIFT = 4;
constexpr UInt8 VARIANT_ARRAY_IS_LARGE_SHIFT = 2;
constexpr UInt8 VARIANT_METADATA_VERSION_MASK = 0x0F;
constexpr UInt8 VARIANT_METADATA_SORTED_STRINGS_SHIFT = 4;
constexpr UInt8 VARIANT_METADATA_OFFSET_SIZE_MINUS_ONE_SHIFT = 6;

enum class VariantBasicType : UInt8
{
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
};

/// Full set of primitive type tags defined by the Parquet Variant spec.
enum class VariantPrimitiveType : UInt8
{
    Null = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Decimal4 = 8,
    Decimal8 = 9,
    Decimal16 = 10,
    Date = 11,
    TimestampMicros = 12,
    TimestampNtzMicros = 13,
    Float = 14,
    Binary = 15,
    String = 16,
    TimeNtzMicros = 17,
    TimestampNanos = 18,
    TimestampNtzNanos = 19,
    UUID = 20,
};

/// Returns the minimum number of bytes needed to represent `value` in little-endian.
inline UInt8 variantByteLength(UInt64 value)
{
    const UInt8 length = static_cast<UInt8>(std::max<size_t>(1, (std::bit_width(value) + 7) / 8));
    chassert(length <= sizeof(value));
    return length;
}

inline void appendVariantLittleEndian(UInt64 value, size_t size, String & out)
{
    chassert(size <= sizeof(value));
    transformEndianness<std::endian::little>(value);
    out.append(reinterpret_cast<const char *>(&value), size);
}

template <typename T>
void appendVariantPOD(T value, String & out)
{
    transformEndianness<std::endian::little>(value);
    out.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

}
