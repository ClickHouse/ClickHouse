#pragma once

#include <string>
#include <string_view>

#include <base/types.h>
#include <base/unaligned.h>
#include <IO/VarInt.h>


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

/// Append an unsigned integer as a protobuf base-128 varint, which is the same encoding as ClickHouse's `VarUInt`
/// (signed zigzag values use `encodeZigZag` from `IO/VarInt.h`).
inline void writeVarint(std::string & out, UInt64 value)
{
    char buf[10]; /// a 64-bit varint is at most 10 bytes
    char * end = writeVarUInt(value, buf);
    out.append(buf, end - buf);
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
    char buf[sizeof(value)];
    unalignedStoreLittleEndian<Float32>(buf, value);
    out.append(buf, sizeof(buf));
}

/// 64-bit fixed field (wire type 1) carrying a little-endian double.
inline void writeDoubleField(std::string & out, UInt32 field_number, Float64 value)
{
    writeTag(out, field_number, WireType::Fixed64);
    char buf[sizeof(value)];
    unalignedStoreLittleEndian<Float64>(buf, value);
    out.append(buf, sizeof(buf));
}

}
