#pragma once

#include <cstring>
#include <string>
#include <string_view>

#include <base/types.h>


/// Minimal writers for the protobuf wire format used by the Mapbox Vector Tile (MVT) specification:
/// https://github.com/mapbox/vector-tile-spec/tree/master/2.1
///
/// MVT tiles are small and consist only of point features here, so instead of pulling in the generic protobuf
/// library (which builds message objects and serializes them via descriptor reflection) we append the wire bytes
/// directly into a byte buffer. This avoids per-feature allocations and reflection, which is the entire reason the
/// encoding is fast. All multi-byte fixed fields are written little-endian as required by the protobuf format,
/// independent of the host byte order.
namespace DB::MVT
{

/// Protobuf wire types.
enum class WireType : UInt32
{
    Varint = 0,
    Fixed64 = 1,
    LengthDelimited = 2,
    Fixed32 = 5,
};

/// Append an unsigned integer as a protobuf base-128 varint.
inline void writeVarint(std::string & out, UInt64 value)
{
    while (value >= 0x80)
    {
        out.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

/// Zigzag-encode a signed integer so that small-magnitude negative values stay small after varint encoding.
inline UInt64 zigzag(Int64 value)
{
    return (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63);
}

/// Field key: (field_number << 3) | wire_type.
inline void writeTag(std::string & out, UInt32 field_number, WireType wire_type)
{
    writeVarint(out, (field_number << 3) | static_cast<UInt32>(wire_type));
}

/// Varint field (wire type 0).
inline void writeVarintField(std::string & out, UInt32 field_number, UInt64 value)
{
    writeTag(out, field_number, WireType::Varint);
    writeVarint(out, value);
}

/// Length-delimited field (wire type 2): key, length, then the raw bytes.
inline void writeLengthDelimitedField(std::string & out, UInt32 field_number, std::string_view data)
{
    writeTag(out, field_number, WireType::LengthDelimited);
    writeVarint(out, data.size());
    out.append(data.data(), data.size());
}

/// 32-bit fixed field (wire type 5) carrying a little-endian float.
inline void writeFloatField(std::string & out, UInt32 field_number, Float32 value)
{
    writeTag(out, field_number, WireType::Fixed32);
    UInt32 bits = 0;
    memcpy(&bits, &value, sizeof(bits));
    for (size_t i = 0; i < sizeof(bits); ++i)
        out.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
}

/// 64-bit fixed field (wire type 1) carrying a little-endian double.
inline void writeDoubleField(std::string & out, UInt32 field_number, Float64 value)
{
    writeTag(out, field_number, WireType::Fixed64);
    UInt64 bits = 0;
    memcpy(&bits, &value, sizeof(bits));
    for (size_t i = 0; i < sizeof(bits); ++i)
        out.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
}

}
