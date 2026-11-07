#include <Storages/Kafka/Clients/Protocol/Wire.h>

namespace Kafka::Protocol
{

namespace
{
constexpr int varint_max_bytes = 5; /// 32-bit varint
constexpr int varlong_max_bytes = 10; /// 64-bit varint
}

void throwProtocol(const String & message)
{
    /// The message may embed arbitrary runtime data (topic names, raw broker
    /// bytes), so it must not pass through a fmt format string.
    throw Exception(ErrorKind::Protocol, PreformattedMessage{.text = message, .format_string = {}, .format_string_args = {}});
}

void checkVersionSupported(Int16 version, Int16 min_version, Int16 max_version, const char * api)
{
    if (version < min_version || version > max_version)
        throw Exception(
            ErrorKind::NotSupported,
            "{} v{} is outside the supported range [v{}, v{}]",
            api, version, min_version, max_version);
}

// -- Writer -------------------------------------------------------------------

void Writer::writeUnsignedVarint(UInt32 value)
{
    while (value >= 0x80)
    {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

void Writer::writeUnsignedVarlong(UInt64 value)
{
    while (value >= 0x80)
    {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

void Writer::writeVarint(Int32 value)
{
    const auto zz = (static_cast<UInt32>(value) << 1) ^ static_cast<UInt32>(value >> 31);
    writeUnsignedVarint(zz);
}

void Writer::writeVarlong(Int64 value)
{
    const auto zz = (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63);
    writeUnsignedVarlong(zz);
}

void Writer::writeUuid(const Uuid & id)
{
    buf.append(reinterpret_cast<const char *>(id.data.data()), id.data.size());
}

void Writer::writeString(std::string_view s)
{
    if (s.size() > static_cast<size_t>(INT16_MAX))
        throw Exception(ErrorKind::Protocol, "STRING too long for i16 length: {}", s.size());
    writeInt16(static_cast<Int16>(s.size()));
    buf.append(s);
}

void Writer::writeNullableString(const std::optional<String> & s)
{
    if (!s)
    {
        writeInt16(-1);
        return;
    }
    writeString(*s);
}

void Writer::writeCompactString(std::string_view s)
{
    if (s.size() + 1 > static_cast<size_t>(UINT32_MAX))
        throw Exception(ErrorKind::Protocol, "COMPACT_STRING too long: {}", s.size());
    writeUnsignedVarint(static_cast<UInt32>(s.size()) + 1);
    buf.append(s);
}

void Writer::writeCompactNullableString(const std::optional<String> & s)
{
    if (!s)
    {
        writeUnsignedVarint(0);
        return;
    }
    writeCompactString(*s);
}

void Writer::writeBytes(std::string_view data)
{
    if (data.size() > static_cast<size_t>(INT32_MAX))
        throw Exception(ErrorKind::Protocol, "BYTES too long for i32 length: {}", data.size());
    writeInt32(static_cast<Int32>(data.size()));
    buf.append(data);
}

void Writer::writeNullableBytes(const std::optional<Bytes> & data)
{
    if (!data)
    {
        writeInt32(-1);
        return;
    }
    writeBytes(*data);
}

void Writer::writeCompactBytes(std::string_view data)
{
    if (data.size() + 1 > static_cast<size_t>(UINT32_MAX))
        throw Exception(ErrorKind::Protocol, "COMPACT_BYTES too long: {}", data.size());
    writeUnsignedVarint(static_cast<UInt32>(data.size()) + 1);
    buf.append(data);
}

void Writer::writeCompactNullableBytes(const std::optional<Bytes> & data)
{
    if (!data)
    {
        writeUnsignedVarint(0);
        return;
    }
    writeCompactBytes(*data);
}

void Writer::overwriteInt32At(size_t pos, Int32 v)
{
    overwriteUInt32At(pos, static_cast<UInt32>(v));
}

void Writer::overwriteUInt32At(size_t pos, UInt32 v)
{
    if (pos + 4 > buf.size())
        throwProtocol("Overwrite position out of range");
    for (int i = 0; i < 4; ++i)
        buf[pos + i] = static_cast<char>((v >> ((3 - i) * 8)) & 0xFF);
}

// -- Reader -------------------------------------------------------------------

UInt32 Reader::readUnsignedVarint()
{
    UInt32 result = 0;
    for (int i = 0; i < varint_max_bytes; ++i)
    {
        const UInt8 b = readByte();
        /// The 5th byte may only carry 4 payload bits.
        if (i == varint_max_bytes - 1 && (b & 0xF0) != 0)
            throwProtocol("Unsigned varint too long");
        result |= static_cast<UInt32>(b & 0x7F) << (7 * i);
        if ((b & 0x80) == 0)
            return result;
    }
    throwProtocol("Unsigned varint too long");
}

Int32 Reader::readVarint()
{
    const UInt32 zz = readUnsignedVarint();
    return static_cast<Int32>(zz >> 1) ^ -static_cast<Int32>(zz & 1);
}

Int64 Reader::readVarlong()
{
    UInt64 result = 0;
    for (int i = 0; i < varlong_max_bytes; ++i)
    {
        const UInt8 b = readByte();
        if (i == varlong_max_bytes - 1 && (b & 0xFE) != 0)
            throwProtocol("Varlong too long");
        result |= static_cast<UInt64>(b & 0x7F) << (7 * i);
        if ((b & 0x80) == 0)
            return static_cast<Int64>(result >> 1) ^ -static_cast<Int64>(result & 1);
    }
    throwProtocol("Varlong too long");
}

Uuid Reader::readUuid()
{
    need(16);
    Uuid id;
    for (size_t i = 0; i < 16; ++i)
        id.data[i] = static_cast<UInt8>(data[pos + i]);
    pos += 16;
    return id;
}

String Reader::readString()
{
    const Int16 len = readInt16();
    if (len < 0)
        throwProtocol("Unexpected null STRING");
    return readUtf8(static_cast<size_t>(len));
}

std::optional<String> Reader::readNullableString()
{
    const Int16 len = readInt16();
    if (len < 0)
        return std::nullopt;
    return readUtf8(static_cast<size_t>(len));
}

String Reader::readCompactString()
{
    const UInt32 raw = readUnsignedVarint();
    if (raw == 0)
        throwProtocol("Unexpected null COMPACT_STRING");
    return readUtf8(raw - 1);
}

std::optional<String> Reader::readCompactNullableString()
{
    const UInt32 raw = readUnsignedVarint();
    if (raw == 0)
        return std::nullopt;
    return readUtf8(raw - 1);
}

String Reader::readBytes()
{
    const Int32 len = readInt32();
    if (len < 0)
        throwProtocol("Unexpected null BYTES");
    return readUtf8(static_cast<size_t>(len));
}

std::optional<String> Reader::readNullableBytes()
{
    const Int32 len = readInt32();
    if (len < 0)
        return std::nullopt;
    return readUtf8(static_cast<size_t>(len));
}

String Reader::readCompactBytes()
{
    const UInt32 raw = readUnsignedVarint();
    if (raw == 0)
        throwProtocol("Unexpected null COMPACT_BYTES");
    return readUtf8(raw - 1);
}

std::optional<String> Reader::readCompactNullableBytes()
{
    const UInt32 raw = readUnsignedVarint();
    if (raw == 0)
        return std::nullopt;
    return readUtf8(raw - 1);
}

std::string_view Reader::readRaw(size_t n)
{
    need(n);
    std::string_view out = data.substr(pos, n);
    pos += n;
    return out;
}

void Reader::skip(size_t n)
{
    need(n);
    pos += n;
}

void Reader::skipTaggedFields()
{
    const UInt32 count = readUnsignedVarint();
    for (UInt32 i = 0; i < count; ++i)
    {
        readUnsignedVarint(); /// tag
        const UInt32 size = readUnsignedVarint();
        skip(size);
    }
}

String Reader::readUtf8(size_t n)
{
    need(n);
    String out(data.substr(pos, n));
    pos += n;
    return out;
}

}
