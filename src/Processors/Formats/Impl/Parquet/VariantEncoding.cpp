#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>

#include <Core/UUID.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/Dynamic/Var.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <set>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Parquet
{

namespace
{

/// Basic type tags occupying the lowest 2 bits of a value's first byte.
enum class BasicType : UInt8
{
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
};

/// Primitive type ids stored in the upper 6 bits of a primitive value's first byte.
enum class PrimitiveType : UInt8
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
    TimestampTz = 12,
    TimestampNtz = 13,
    Float = 14,
    Binary = 15,
    String = 16,
    TimeNtz = 17,
    TimestampNanosTz = 18,
    TimestampNanosNtz = 19,
    Uuid = 20,
};

[[noreturn]] void throwCorrupt(const char * what)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupt variant value: {}", what);
}

void checkRange(std::string_view buf, size_t offset, size_t length, const char * what)
{
    if (offset > buf.size() || length > buf.size() - offset)
        throwCorrupt(what);
}

/// Read an unsigned little-endian integer of `n` bytes (n in 1..8).
UInt64 readLE(const char * p, size_t n)
{
    UInt64 v = 0;
    for (size_t i = 0; i < n; ++i)
        v |= static_cast<UInt64>(static_cast<UInt8>(p[i])) << (8 * i);
    return v;
}

/// Read a signed little-endian integer of `n` bytes (n in 1..8), sign-extended.
Int64 readLESigned(const char * p, size_t n)
{
    UInt64 v = readLE(p, n);
    if (n < 8 && (v & (1ULL << (8 * n - 1))))
        v |= ~((1ULL << (8 * n)) - 1);
    return static_cast<Int64>(v);
}

/// Read a signed little-endian 128-bit integer of `n` bytes (n in 1..16), sign-extended.
Int128 readLESigned128(const char * p, size_t n)
{
    Int128 v = 0;
    for (size_t i = 0; i < n; ++i)
        v |= static_cast<Int128>(static_cast<UInt8>(p[i])) << (8 * i);
    if (n < 16 && (static_cast<UInt8>(p[n - 1]) & 0x80))
    {
        Int128 mask = static_cast<Int128>(-1);
        mask <<= static_cast<int>(8 * n);
        v |= mask;
    }
    return v;
}

void writeJSONStringView(std::string_view s, WriteBuffer & out)
{
    static constexpr char hex[] = "0123456789abcdef";
    writeChar('"', out);
    for (char c : s)
    {
        switch (c)
        {
            case '"': writeCString("\\\"", out); break;
            case '\\': writeCString("\\\\", out); break;
            case '\b': writeCString("\\b", out); break;
            case '\f': writeCString("\\f", out); break;
            case '\n': writeCString("\\n", out); break;
            case '\r': writeCString("\\r", out); break;
            case '\t': writeCString("\\t", out); break;
            default:
                if (static_cast<UInt8>(c) < 0x20)
                {
                    writeCString("\\u00", out);
                    writeChar(hex[(c >> 4) & 0xF], out);
                    writeChar(hex[c & 0xF], out);
                }
                else
                    writeChar(c, out);
        }
    }
    writeChar('"', out);
}

/// Convert a day count since the Unix epoch into a civil (year, month, day).
/// Howard Hinnant's algorithm, valid for the whole int range.
void civilFromDays(Int64 z, Int64 & y, unsigned & m, unsigned & d)
{
    z += 719468;
    Int64 era = (z >= 0 ? z : z - 146096) / 146097;
    auto doe = static_cast<UInt64>(z - era * 146097);
    UInt64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    Int64 yy = static_cast<Int64>(yoe) + era * 400;
    UInt64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    UInt64 mp = (5 * doy + 2) / 153;
    d = static_cast<unsigned>(doy - (153 * mp + 2) / 5 + 1);
    m = static_cast<unsigned>(mp < 10 ? mp + 3 : mp - 9);
    y = yy + (m <= 2);
}

void writePadded(WriteBuffer & out, Int64 value, size_t width)
{
    String s = std::to_string(value);
    for (size_t i = s.size(); i < width; ++i)
        writeChar('0', out);
    writeString(s, out);
}

void writeDateJSON(Int32 days, WriteBuffer & out, bool quote = true)
{
    Int64 y = 0;
    unsigned m = 0;
    unsigned d = 0;
    civilFromDays(days, y, m, d);
    if (quote)
        writeChar('"', out);
    writePadded(out, y, 4);
    writeChar('-', out);
    writePadded(out, m, 2);
    writeChar('-', out);
    writePadded(out, d, 2);
    if (quote)
        writeChar('"', out);
}

/// Render a timestamp given as a count of `sub` fractional units per second (10^6 for micros,
/// 10^9 for nanos) since the Unix epoch. `utc` appends a trailing 'Z'.
void writeTimestampJSON(Int64 value, Int64 sub, bool utc, WriteBuffer & out, bool quote = true)
{
    Int64 seconds = value / sub;
    Int64 frac = value % sub;
    if (frac < 0)
    {
        frac += sub;
        seconds -= 1;
    }
    Int64 days = seconds / 86400;
    Int64 tod = seconds % 86400;
    if (tod < 0)
    {
        tod += 86400;
        days -= 1;
    }
    Int64 y = 0;
    unsigned m = 0;
    unsigned d = 0;
    civilFromDays(days, y, m, d);

    if (quote)
        writeChar('"', out);
    writePadded(out, y, 4);
    writeChar('-', out);
    writePadded(out, m, 2);
    writeChar('-', out);
    writePadded(out, d, 2);
    writeChar('T', out);
    writePadded(out, tod / 3600, 2);
    writeChar(':', out);
    writePadded(out, (tod % 3600) / 60, 2);
    writeChar(':', out);
    writePadded(out, tod % 60, 2);
    if (frac != 0)
    {
        writeChar('.', out);
        writePadded(out, frac, sub == 1000000000 ? 9 : 6);
    }
    if (utc)
        writeChar('Z', out);
    if (quote)
        writeChar('"', out);
}

void writeTimeJSON(Int64 micros, WriteBuffer & out, bool quote = true)
{
    Int64 seconds = micros / 1000000;
    Int64 frac = micros % 1000000;
    if (quote)
        writeChar('"', out);
    writePadded(out, seconds / 3600, 2);
    writeChar(':', out);
    writePadded(out, (seconds % 3600) / 60, 2);
    writeChar(':', out);
    writePadded(out, seconds % 60, 2);
    if (frac != 0)
    {
        writeChar('.', out);
        writePadded(out, frac, 6);
    }
    if (quote)
        writeChar('"', out);
}

void writeDecimalJSON(Int128 unscaled, UInt32 scale, WriteBuffer & out)
{
    bool negative = unscaled < 0;
    Int128 v = negative ? -unscaled : unscaled;

    String digits;
    if (v == 0)
        digits = "0";
    while (v > 0)
    {
        digits += static_cast<char>('0' + static_cast<int>(v % 10));
        v /= 10;
    }
    std::reverse(digits.begin(), digits.end());

    if (negative)
        writeChar('-', out);

    if (scale == 0)
    {
        writeString(digits, out);
        return;
    }

    if (digits.size() <= scale)
        digits = String(scale - digits.size() + 1, '0') + digits;
    digits.insert(digits.size() - scale, ".");
    writeString(digits, out);
}

void writeUuidJSON(const char * p, WriteBuffer & out, bool quote = true)
{
    static constexpr char hex[] = "0123456789abcdef";
    if (quote)
        writeChar('"', out);
    for (size_t i = 0; i < 16; ++i)
    {
        if (i == 4 || i == 6 || i == 8 || i == 10)
            writeChar('-', out);
        auto b = static_cast<UInt8>(p[i]);
        writeChar(hex[b >> 4], out);
        writeChar(hex[b & 0xF], out);
    }
    if (quote)
        writeChar('"', out);
}

struct Metadata
{
    std::vector<std::string_view> keys;
};

Metadata parseMetadata(std::string_view metadata)
{
    if (metadata.empty())
        throwCorrupt("empty metadata");

    UInt8 header = metadata[0];
    UInt8 version = header & 0x0F;
    if (version != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported variant metadata version {}", static_cast<UInt32>(version));
    size_t offset_size = ((header >> 6) & 0x03) + 1;

    size_t pos = 1;
    checkRange(metadata, pos, offset_size, "metadata dictionary size");
    size_t dict_size = readLE(metadata.data() + pos, offset_size);
    pos += offset_size;

    checkRange(metadata, pos, (dict_size + 1) * offset_size, "metadata offsets");
    const char * offsets = metadata.data() + pos;
    pos += (dict_size + 1) * offset_size;
    size_t strings_start = pos;

    Metadata result;
    result.keys.reserve(dict_size);
    for (size_t i = 0; i < dict_size; ++i)
    {
        size_t begin = readLE(offsets + i * offset_size, offset_size);
        size_t end = readLE(offsets + (i + 1) * offset_size, offset_size);
        if (end < begin)
            throwCorrupt("metadata offsets not monotonic");
        checkRange(metadata, strings_start + begin, end - begin, "metadata string");
        result.keys.emplace_back(metadata.data() + strings_start + begin, end - begin);
    }
    return result;
}

void decodeValueAt(std::string_view value, size_t pos, const Metadata & meta, WriteBuffer & out);

void decodePrimitive(std::string_view value, size_t pos, UInt8 type_id, const Metadata &, WriteBuffer & out)
{
    const char * p = value.data() + pos + 1;
    auto require = [&](size_t n, const char * what) { checkRange(value, pos + 1, n, what); };

    switch (static_cast<PrimitiveType>(type_id))
    {
        case PrimitiveType::Null: writeCString("null", out); return;
        case PrimitiveType::BooleanTrue: writeCString("true", out); return;
        case PrimitiveType::BooleanFalse: writeCString("false", out); return;
        case PrimitiveType::Int8: require(1, "int8"); writeIntText(readLESigned(p, 1), out); return;
        case PrimitiveType::Int16: require(2, "int16"); writeIntText(readLESigned(p, 2), out); return;
        case PrimitiveType::Int32: require(4, "int32"); writeIntText(readLESigned(p, 4), out); return;
        case PrimitiveType::Int64: require(8, "int64"); writeIntText(readLESigned(p, 8), out); return;
        case PrimitiveType::Double:
        {
            require(8, "double");
            Float64 d = 0;
            memcpy(&d, p, 8);
            writeFloatText(d, out);
            return;
        }
        case PrimitiveType::Float:
        {
            require(4, "float");
            Float32 f = 0;
            memcpy(&f, p, 4);
            writeFloatText(f, out);
            return;
        }
        case PrimitiveType::Decimal4: require(5, "decimal4"); writeDecimalJSON(readLESigned128(p + 1, 4), static_cast<UInt8>(p[0]), out); return;
        case PrimitiveType::Decimal8: require(9, "decimal8"); writeDecimalJSON(readLESigned128(p + 1, 8), static_cast<UInt8>(p[0]), out); return;
        case PrimitiveType::Decimal16: require(17, "decimal16"); writeDecimalJSON(readLESigned128(p + 1, 16), static_cast<UInt8>(p[0]), out); return;
        case PrimitiveType::Date: require(4, "date"); writeDateJSON(static_cast<Int32>(readLESigned(p, 4)), out); return;
        case PrimitiveType::TimestampTz: require(8, "timestamp"); writeTimestampJSON(readLESigned(p, 8), 1000000, true, out); return;
        case PrimitiveType::TimestampNtz: require(8, "timestamp_ntz"); writeTimestampJSON(readLESigned(p, 8), 1000000, false, out); return;
        case PrimitiveType::TimestampNanosTz: require(8, "timestamp_ns"); writeTimestampJSON(readLESigned(p, 8), 1000000000, true, out); return;
        case PrimitiveType::TimestampNanosNtz: require(8, "timestamp_ns_ntz"); writeTimestampJSON(readLESigned(p, 8), 1000000000, false, out); return;
        case PrimitiveType::TimeNtz: require(8, "time"); writeTimeJSON(readLESigned(p, 8), out); return;
        case PrimitiveType::Uuid: require(16, "uuid"); writeUuidJSON(p, out); return;
        case PrimitiveType::String:
        {
            require(4, "string length");
            UInt32 len = static_cast<UInt32>(readLE(p, 4));
            require(4 + static_cast<size_t>(len), "string body");
            writeJSONStringView(std::string_view(p + 4, len), out);
            return;
        }
        case PrimitiveType::Binary:
        {
            require(4, "binary length");
            UInt32 len = static_cast<UInt32>(readLE(p, 4));
            require(4 + static_cast<size_t>(len), "binary body");
            writeJSONStringView(base64Encode(std::string(p + 4, len)), out);
            return;
        }
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown variant primitive type id");
}

void decodeObject(std::string_view value, size_t pos, UInt8 header, const Metadata & meta, WriteBuffer & out)
{
    size_t field_offset_size = ((header >> 2) & 0x03) + 1;
    size_t field_id_size = ((header >> 4) & 0x03) + 1;
    bool is_large = (header >> 6) & 0x01;
    size_t num_size = is_large ? 4 : 1;

    size_t p = pos + 1;
    checkRange(value, p, num_size, "object size");
    size_t num = readLE(value.data() + p, num_size);
    p += num_size;

    checkRange(value, p, num * field_id_size, "object field ids");
    const char * ids = value.data() + p;
    p += num * field_id_size;

    checkRange(value, p, (num + 1) * field_offset_size, "object field offsets");
    const char * offsets = value.data() + p;
    p += (num + 1) * field_offset_size;
    size_t data_start = p;

    writeChar('{', out);
    for (size_t i = 0; i < num; ++i)
    {
        if (i)
            writeChar(',', out);
        size_t id = readLE(ids + i * field_id_size, field_id_size);
        if (id >= meta.keys.size())
            throwCorrupt("object field id out of range");
        writeJSONStringView(meta.keys[id], out);
        writeChar(':', out);
        size_t off = readLE(offsets + i * field_offset_size, field_offset_size);
        decodeValueAt(value, data_start + off, meta, out);
    }
    writeChar('}', out);
}

void decodeArray(std::string_view value, size_t pos, UInt8 header, const Metadata & meta, WriteBuffer & out)
{
    size_t field_offset_size = ((header >> 2) & 0x03) + 1;
    bool is_large = (header >> 4) & 0x01;
    size_t num_size = is_large ? 4 : 1;

    size_t p = pos + 1;
    checkRange(value, p, num_size, "array size");
    size_t num = readLE(value.data() + p, num_size);
    p += num_size;

    checkRange(value, p, (num + 1) * field_offset_size, "array offsets");
    const char * offsets = value.data() + p;
    p += (num + 1) * field_offset_size;
    size_t data_start = p;

    writeChar('[', out);
    for (size_t i = 0; i < num; ++i)
    {
        if (i)
            writeChar(',', out);
        size_t off = readLE(offsets + i * field_offset_size, field_offset_size);
        decodeValueAt(value, data_start + off, meta, out);
    }
    writeChar(']', out);
}

void decodeValueAt(std::string_view value, size_t pos, const Metadata & meta, WriteBuffer & out)
{
    checkRange(value, pos, 1, "value header");
    UInt8 header = value[pos];
    auto basic_type = static_cast<BasicType>(header & 0x03);
    UInt8 rest = (header >> 2) & 0x3F;

    switch (basic_type)
    {
        case BasicType::ShortString:
        {
            checkRange(value, pos + 1, rest, "short string");
            writeJSONStringView(std::string_view(value.data() + pos + 1, rest), out);
            return;
        }
        case BasicType::Primitive: decodePrimitive(value, pos, rest, meta, out); return;
        case BasicType::Object: decodeObject(value, pos, header, meta, out); return;
        case BasicType::Array: decodeArray(value, pos, header, meta, out); return;
    }
}

/// ---------- Decoding into a ClickHouse Field ----------

/// Render one scalar value as plain (unquoted) text, for use as an Array/Map element. Nested
/// containers inside an array/object are not supported (the "primitives inside" assumption).
String renderScalarPlain(std::string_view value, size_t pos, const Metadata & meta)
{
    checkRange(value, pos, 1, "value header");
    UInt8 header = value[pos];
    auto basic = static_cast<BasicType>(header & 0x03);
    UInt8 rest = (header >> 2) & 0x3F;
    if (basic == BasicType::ShortString)
    {
        checkRange(value, pos + 1, rest, "short string");
        return String(value.substr(pos + 1, rest));
    }
    if (basic != BasicType::Primitive)
    {
        /// A nested object/array as an element: render it to JSON text (our own writer, no Poco).
        WriteBufferFromOwnString nested;
        decodeValueAt(value, pos, meta, nested);
        nested.finalize();
        return nested.str();
    }

    const char * p = value.data() + pos + 1;
    auto require = [&](size_t n, const char * what) { checkRange(value, pos + 1, n, what); };
    WriteBufferFromOwnString buf;
    switch (static_cast<PrimitiveType>(rest))
    {
        case PrimitiveType::Null: return "null";
        case PrimitiveType::BooleanTrue: return "true";
        case PrimitiveType::BooleanFalse: return "false";
        case PrimitiveType::Int8: require(1, "int8"); writeIntText(readLESigned(p, 1), buf); break;
        case PrimitiveType::Int16: require(2, "int16"); writeIntText(readLESigned(p, 2), buf); break;
        case PrimitiveType::Int32: require(4, "int32"); writeIntText(readLESigned(p, 4), buf); break;
        case PrimitiveType::Int64: require(8, "int64"); writeIntText(readLESigned(p, 8), buf); break;
        case PrimitiveType::Double: { require(8, "double"); Float64 d = 0; memcpy(&d, p, 8); writeFloatText(d, buf); break; }
        case PrimitiveType::Float: { require(4, "float"); Float32 f = 0; memcpy(&f, p, 4); writeFloatText(f, buf); break; }
        case PrimitiveType::Decimal4: require(5, "decimal4"); writeDecimalJSON(readLESigned128(p + 1, 4), static_cast<UInt8>(p[0]), buf); break;
        case PrimitiveType::Decimal8: require(9, "decimal8"); writeDecimalJSON(readLESigned128(p + 1, 8), static_cast<UInt8>(p[0]), buf); break;
        case PrimitiveType::Decimal16: require(17, "decimal16"); writeDecimalJSON(readLESigned128(p + 1, 16), static_cast<UInt8>(p[0]), buf); break;
        case PrimitiveType::Date: require(4, "date"); writeDateJSON(static_cast<Int32>(readLESigned(p, 4)), buf, false); break;
        case PrimitiveType::TimestampTz: require(8, "timestamp"); writeTimestampJSON(readLESigned(p, 8), 1000000, true, buf, false); break;
        case PrimitiveType::TimestampNtz: require(8, "timestamp_ntz"); writeTimestampJSON(readLESigned(p, 8), 1000000, false, buf, false); break;
        case PrimitiveType::TimestampNanosTz: require(8, "timestamp_ns"); writeTimestampJSON(readLESigned(p, 8), 1000000000, true, buf, false); break;
        case PrimitiveType::TimestampNanosNtz: require(8, "timestamp_ns_ntz"); writeTimestampJSON(readLESigned(p, 8), 1000000000, false, buf, false); break;
        case PrimitiveType::TimeNtz: require(8, "time"); writeTimeJSON(readLESigned(p, 8), buf, false); break;
        case PrimitiveType::Uuid: require(16, "uuid"); writeUuidJSON(p, buf, false); break;
        case PrimitiveType::String:
        {
            require(4, "string length");
            UInt32 len = static_cast<UInt32>(readLE(p, 4));
            require(4 + static_cast<size_t>(len), "string body");
            return String(value.substr(pos + 1 + 4, len));
        }
        case PrimitiveType::Binary:
        {
            require(4, "binary length");
            UInt32 len = static_cast<UInt32>(readLE(p, 4));
            require(4 + static_cast<size_t>(len), "binary body");
            return base64Encode(std::string(value.substr(pos + 1 + 4, len)));
        }
    }
    buf.finalize();
    return buf.str();
}

VariantField decodeValueToField(std::string_view value, size_t pos, const Metadata & meta)
{
    checkRange(value, pos, 1, "value header");
    UInt8 header = value[pos];
    auto basic_type = static_cast<BasicType>(header & 0x03);
    UInt8 rest = (header >> 2) & 0x3F;

    if (basic_type == BasicType::ShortString)
        return {VariantFieldKind::String, Field(String(value.substr(pos + 1, rest)))};

    if (basic_type == BasicType::Object)
    {
        size_t field_offset_size = ((header >> 2) & 0x03) + 1;
        size_t field_id_size = ((header >> 4) & 0x03) + 1;
        bool is_large = (header >> 6) & 0x01;
        size_t num_size = is_large ? 4 : 1;
        size_t p = pos + 1;
        checkRange(value, p, num_size, "object size");
        size_t num = readLE(value.data() + p, num_size);
        p += num_size;
        checkRange(value, p, num * field_id_size, "object ids");
        const char * ids = value.data() + p;
        p += num * field_id_size;
        checkRange(value, p, (num + 1) * field_offset_size, "object offsets");
        const char * offsets = value.data() + p;
        p += (num + 1) * field_offset_size;
        size_t data_start = p;

        Map map;
        map.reserve(num);
        for (size_t i = 0; i < num; ++i)
        {
            size_t id = readLE(ids + i * field_id_size, field_id_size);
            if (id >= meta.keys.size())
                throwCorrupt("object field id out of range");
            size_t off = readLE(offsets + i * field_offset_size, field_offset_size);
            Tuple entry;
            entry.push_back(Field(String(meta.keys[id])));
            entry.push_back(Field(renderScalarPlain(value, data_start + off, meta)));
            map.push_back(Field(std::move(entry)));
        }
        return {VariantFieldKind::Map, Field(std::move(map))};
    }

    if (basic_type == BasicType::Array)
    {
        size_t field_offset_size = ((header >> 2) & 0x03) + 1;
        bool is_large = (header >> 4) & 0x01;
        size_t num_size = is_large ? 4 : 1;
        size_t p = pos + 1;
        checkRange(value, p, num_size, "array size");
        size_t num = readLE(value.data() + p, num_size);
        p += num_size;
        checkRange(value, p, (num + 1) * field_offset_size, "array offsets");
        const char * offsets = value.data() + p;
        p += (num + 1) * field_offset_size;
        size_t data_start = p;

        Array arr;
        arr.reserve(num);
        for (size_t i = 0; i < num; ++i)
        {
            size_t off = readLE(offsets + i * field_offset_size, field_offset_size);
            arr.push_back(Field(renderScalarPlain(value, data_start + off, meta)));
        }
        return {VariantFieldKind::Array, Field(std::move(arr))};
    }

    /// Primitive.
    const char * p = value.data() + pos + 1;
    switch (static_cast<PrimitiveType>(rest))
    {
        case PrimitiveType::Null: return {VariantFieldKind::Null, Field()};
        case PrimitiveType::BooleanTrue: return {VariantFieldKind::Bool, Field(UInt64(1))};
        case PrimitiveType::BooleanFalse: return {VariantFieldKind::Bool, Field(UInt64(0))};
        case PrimitiveType::Int8: checkRange(value, pos + 1, 1, "int8"); return {VariantFieldKind::Int64, Field(readLESigned(p, 1))};
        case PrimitiveType::Int16: checkRange(value, pos + 1, 2, "int16"); return {VariantFieldKind::Int64, Field(readLESigned(p, 2))};
        case PrimitiveType::Int32: checkRange(value, pos + 1, 4, "int32"); return {VariantFieldKind::Int64, Field(readLESigned(p, 4))};
        case PrimitiveType::Int64: checkRange(value, pos + 1, 8, "int64"); return {VariantFieldKind::Int64, Field(readLESigned(p, 8))};
        case PrimitiveType::Double:
        {
            checkRange(value, pos + 1, 8, "double");
            Float64 d = 0;
            memcpy(&d, p, 8);
            return {VariantFieldKind::Float64, Field(d)};
        }
        case PrimitiveType::Float:
        {
            checkRange(value, pos + 1, 4, "float");
            Float32 f = 0;
            memcpy(&f, p, 4);
            return {VariantFieldKind::Float64, Field(Float64(f))};
        }
        case PrimitiveType::Date:
            checkRange(value, pos + 1, 4, "date");
            return {VariantFieldKind::Date, Field(readLESigned(p, 4))};
        case PrimitiveType::TimestampTz:
        case PrimitiveType::TimestampNtz:
            checkRange(value, pos + 1, 8, "timestamp");
            return {VariantFieldKind::DateTimeMicros, Field(DecimalField<DateTime64>(DateTime64(readLESigned(p, 8)), 6))};
        case PrimitiveType::TimestampNanosTz:
        case PrimitiveType::TimestampNanosNtz:
            checkRange(value, pos + 1, 8, "timestamp_ns");
            return {VariantFieldKind::DateTimeNanos, Field(DecimalField<DateTime64>(DateTime64(readLESigned(p, 8)), 9))};
        case PrimitiveType::Uuid:
        {
            checkRange(value, pos + 1, 16, "uuid");
            UInt128 x = 0;
            for (int i = 0; i < 16; ++i) // big-endian
                x = (x << 8) | static_cast<UInt8>(p[i]);
            return {VariantFieldKind::Uuid, Field(UUID(x))};
        }
        default:
            /// decimal / time / binary / long string -> textual String.
            return {VariantFieldKind::String, Field(renderScalarPlain(value, pos, meta))};
    }
}

/// ---------- Encoding ----------

void writeLE(String & buf, UInt64 v, size_t n)
{
    for (size_t i = 0; i < n; ++i)
        buf.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

size_t bytesForValue(UInt64 v)
{
    if (v <= 0xFF)
        return 1;
    if (v <= 0xFFFF)
        return 2;
    if (v <= 0xFFFFFF)
        return 3;
    return 4;
}

void collectKeys(const Poco::Dynamic::Var & var, std::set<String> & keys)
{
    if (var.type() == typeid(Poco::JSON::Object::Ptr))
    {
        auto obj = var.extract<Poco::JSON::Object::Ptr>();
        for (const auto & pair : *obj)
        {
            keys.insert(pair.first);
            collectKeys(pair.second, keys);
        }
    }
    else if (var.type() == typeid(Poco::JSON::Array::Ptr))
    {
        const auto & arr = var.extract<Poco::JSON::Array::Ptr>();
        for (size_t i = 0; i < arr->size(); ++i)
            collectKeys(arr->get(static_cast<unsigned>(i)), keys);
    }
}

String encodeValue(const Poco::Dynamic::Var & var, const std::map<String, size_t> & key_ids, size_t field_id_size);

String encodeScalar(const Poco::Dynamic::Var & var)
{
    String out;
    if (var.isEmpty())
    {
        out.push_back(static_cast<char>((static_cast<UInt8>(PrimitiveType::Null) << 2)));
        return out;
    }
    if (var.type() == typeid(bool))
    {
        bool b = var.extract<bool>();
        out.push_back(static_cast<char>(
            (static_cast<UInt8>(b ? PrimitiveType::BooleanTrue : PrimitiveType::BooleanFalse) << 2)));
        return out;
    }
    if (var.isInteger())
    {
        Int64 v = var.convert<Int64>();
        if (v >= std::numeric_limits<Int8>::min() && v <= std::numeric_limits<Int8>::max())
        {
            out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int8) << 2));
            writeLE(out, static_cast<UInt64>(v), 1);
        }
        else if (v >= std::numeric_limits<Int16>::min() && v <= std::numeric_limits<Int16>::max())
        {
            out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int16) << 2));
            writeLE(out, static_cast<UInt64>(v), 2);
        }
        else if (v >= std::numeric_limits<Int32>::min() && v <= std::numeric_limits<Int32>::max())
        {
            out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int32) << 2));
            writeLE(out, static_cast<UInt64>(v), 4);
        }
        else
        {
            out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int64) << 2));
            writeLE(out, static_cast<UInt64>(v), 8);
        }
        return out;
    }
    if (var.isNumeric())
    {
        Float64 d = var.convert<Float64>();
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Double) << 2));
        UInt64 bits = 0;
        memcpy(&bits, &d, 8);
        writeLE(out, bits, 8);
        return out;
    }

    /// String.
    String s = var.convert<String>();
    if (s.size() <= 63)
    {
        out.push_back(static_cast<char>((static_cast<UInt8>(s.size()) << 2) | static_cast<UInt8>(BasicType::ShortString)));
        out.append(s);
    }
    else
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::String) << 2));
        writeLE(out, s.size(), 4);
        out.append(s);
    }
    return out;
}

String encodeArray(const Poco::JSON::Array::Ptr & arr, const std::map<String, size_t> & key_ids, size_t field_id_size)
{
    size_t num = arr->size();
    std::vector<String> elements;
    elements.reserve(num);
    size_t data_size = 0;
    for (size_t i = 0; i < num; ++i)
    {
        elements.push_back(encodeValue(arr->get(static_cast<unsigned>(i)), key_ids, field_id_size));
        data_size += elements.back().size();
    }

    size_t field_offset_size = bytesForValue(data_size);
    bool is_large = num > 0xFF;

    String out;
    out.push_back(static_cast<char>(
        static_cast<UInt8>(BasicType::Array)
        | (static_cast<UInt8>(field_offset_size - 1) << 2)
        | (static_cast<UInt8>(is_large ? 1 : 0) << 4)));
    writeLE(out, num, is_large ? 4 : 1);

    size_t offset = 0;
    for (const auto & e : elements)
    {
        writeLE(out, offset, field_offset_size);
        offset += e.size();
    }
    writeLE(out, offset, field_offset_size);
    for (const auto & e : elements)
        out.append(e);
    return out;
}

String encodeObject(const Poco::JSON::Object::Ptr & obj, const std::map<String, size_t> & key_ids, size_t field_id_size)
{
    /// Sort fields by key (the metadata dictionary is sorted, so ids are ordered by key too).
    std::vector<String> names;
    for (const auto & pair : *obj)
        names.push_back(pair.first);
    std::sort(names.begin(), names.end());

    std::vector<String> values;
    values.reserve(names.size());
    size_t data_size = 0;
    for (const auto & name : names)
    {
        values.push_back(encodeValue(obj->get(name), key_ids, field_id_size));
        data_size += values.back().size();
    }

    size_t num = names.size();
    size_t field_offset_size = bytesForValue(data_size);
    bool is_large = num > 0xFF;

    String out;
    out.push_back(static_cast<char>(
        static_cast<UInt8>(BasicType::Object)
        | (static_cast<UInt8>(field_offset_size - 1) << 2)
        | (static_cast<UInt8>(field_id_size - 1) << 4)
        | (static_cast<UInt8>(is_large ? 1 : 0) << 6)));
    writeLE(out, num, is_large ? 4 : 1);

    for (const auto & name : names)
        writeLE(out, key_ids.at(name), field_id_size);

    size_t offset = 0;
    for (const auto & v : values)
    {
        writeLE(out, offset, field_offset_size);
        offset += v.size();
    }
    writeLE(out, offset, field_offset_size);
    for (const auto & v : values)
        out.append(v);
    return out;
}

String encodeValue(const Poco::Dynamic::Var & var, const std::map<String, size_t> & key_ids, size_t field_id_size)
{
    if (var.type() == typeid(Poco::JSON::Object::Ptr))
        return encodeObject(var.extract<Poco::JSON::Object::Ptr>(), key_ids, field_id_size);
    if (var.type() == typeid(Poco::JSON::Array::Ptr))
        return encodeArray(var.extract<Poco::JSON::Array::Ptr>(), key_ids, field_id_size);
    return encodeScalar(var);
}

/// ---------- Encoding a ClickHouse value directly into variant binary (type-preserving) ----------

void appendIntBytes(String & out, Int64 v)
{
    if (v >= std::numeric_limits<Int8>::min() && v <= std::numeric_limits<Int8>::max())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int8) << 2));
        writeLE(out, static_cast<UInt64>(v), 1);
    }
    else if (v >= std::numeric_limits<Int16>::min() && v <= std::numeric_limits<Int16>::max())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int16) << 2));
        writeLE(out, static_cast<UInt64>(v), 2);
    }
    else if (v >= std::numeric_limits<Int32>::min() && v <= std::numeric_limits<Int32>::max())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int32) << 2));
        writeLE(out, static_cast<UInt64>(v), 4);
    }
    else
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Int64) << 2));
        writeLE(out, static_cast<UInt64>(v), 8);
    }
}

void appendStringBytes(String & out, std::string_view s)
{
    if (s.size() <= 63)
    {
        out.push_back(static_cast<char>((static_cast<UInt8>(s.size()) << 2) | static_cast<UInt8>(BasicType::ShortString)));
        out.append(s);
    }
    else
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::String) << 2));
        writeLE(out, s.size(), 4);
        out.append(s);
    }
}

const IDataType & unwrapNullable(const IDataType & type)
{
    if (const auto * n = typeid_cast<const DataTypeNullable *>(&type))
        return *n->getNestedType();
    return type;
}

void collectCHKeys(const Field & field, const IDataType & type_in, std::set<String> & keys)
{
    if (field.isNull())
        return;
    const IDataType & type = unwrapNullable(type_in);
    WhichDataType which(type);
    if (which.isTuple())
    {
        const auto & tt = assert_cast<const DataTypeTuple &>(type);
        const auto & tup = field.safeGet<Tuple>();
        for (size_t i = 0; i < tup.size(); ++i)
        {
            keys.insert(tt.getNameByPosition(i + 1));
            collectCHKeys(tup[i], *tt.getElement(i), keys);
        }
    }
    else if (which.isMap())
    {
        const auto & mt = assert_cast<const DataTypeMap &>(type);
        for (const auto & entry_field : field.safeGet<Map>())
        {
            const auto & entry = entry_field.safeGet<Tuple>();
            keys.insert(entry[0].safeGet<String>());
            collectCHKeys(entry[1], *mt.getValueType(), keys);
        }
    }
    else if (which.isArray())
    {
        const auto & at = assert_cast<const DataTypeArray &>(type);
        for (const auto & e : field.safeGet<Array>())
            collectCHKeys(e, *at.getNestedType(), keys);
    }
}

String encodeCHValue(const Field & field, const IDataType & type_in, const std::map<String, size_t> & key_ids, size_t field_id_size);

String buildObjectBytes(std::vector<std::pair<String, String>> entries, const std::map<String, size_t> & key_ids, size_t field_id_size)
{
    std::sort(entries.begin(), entries.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
    size_t data_size = 0;
    for (const auto & e : entries)
        data_size += e.second.size();
    size_t field_offset_size = bytesForValue(data_size);
    bool is_large = entries.size() > 0xFF;

    String out;
    out.push_back(static_cast<char>(
        static_cast<UInt8>(BasicType::Object)
        | (static_cast<UInt8>(field_offset_size - 1) << 2)
        | (static_cast<UInt8>(field_id_size - 1) << 4)
        | (static_cast<UInt8>(is_large ? 1 : 0) << 6)));
    writeLE(out, entries.size(), is_large ? 4 : 1);
    for (const auto & e : entries)
        writeLE(out, key_ids.at(e.first), field_id_size);
    size_t offset = 0;
    for (const auto & e : entries)
    {
        writeLE(out, offset, field_offset_size);
        offset += e.second.size();
    }
    writeLE(out, offset, field_offset_size);
    for (const auto & e : entries)
        out.append(e.second);
    return out;
}

String encodeCHValue(const Field & field, const IDataType & type_in, const std::map<String, size_t> & key_ids, size_t field_id_size)
{
    String out;
    if (field.isNull())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Null) << 2));
        return out;
    }
    const IDataType & type = unwrapNullable(type_in);
    WhichDataType which(type);

    if (type.getName() == "Bool")
    {
        bool b = field.safeGet<UInt64>() != 0;
        out.push_back(static_cast<char>(static_cast<UInt8>(b ? PrimitiveType::BooleanTrue : PrimitiveType::BooleanFalse) << 2));
        return out;
    }
    if (which.isNativeInt() || which.isEnum())
    {
        appendIntBytes(out, field.safeGet<Int64>());
        return out;
    }
    if (which.isNativeUInt())
    {
        appendIntBytes(out, static_cast<Int64>(field.safeGet<UInt64>()));
        return out;
    }
    if (which.isFloat32())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Float) << 2));
        auto f = static_cast<Float32>(field.safeGet<Float64>());
        UInt32 bits = 0;
        memcpy(&bits, &f, 4);
        writeLE(out, bits, 4);
        return out;
    }
    if (which.isFloat64())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Double) << 2));
        Float64 d = field.safeGet<Float64>();
        UInt64 bits = 0;
        memcpy(&bits, &d, 8);
        writeLE(out, bits, 8);
        return out;
    }
    if (which.isStringOrFixedString())
    {
        appendStringBytes(out, field.safeGet<String>());
        return out;
    }
    if (which.isDate())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Date) << 2));
        writeLE(out, field.safeGet<UInt64>(), 4);
        return out;
    }
    if (which.isDate32())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Date) << 2));
        writeLE(out, static_cast<UInt64>(field.safeGet<Int64>()), 4);
        return out;
    }
    if (which.isDateTime())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::TimestampTz) << 2));
        writeLE(out, static_cast<UInt64>(field.safeGet<UInt64>()) * 1000000, 8);
        return out;
    }
    if (which.isDateTime64())
    {
        const auto & dt = assert_cast<const DataTypeDateTime64 &>(type);
        Int64 v = field.safeGet<DecimalField<DateTime64>>().getValue();
        bool nanos = dt.getScale() > 6;
        auto tag = static_cast<UInt8>(nanos ? PrimitiveType::TimestampNanosTz : PrimitiveType::TimestampTz);
        out.push_back(static_cast<char>(tag << 2));
        writeLE(out, static_cast<UInt64>(v), 8);
        return out;
    }
    if (which.isUUID())
    {
        out.push_back(static_cast<char>(static_cast<UInt8>(PrimitiveType::Uuid) << 2));
        UInt128 x = field.safeGet<UUID>().toUnderType();
        for (int i = 15; i >= 0; --i)
            out.push_back(static_cast<char>((x >> (8 * i)) & 0xFF));
        return out;
    }
    if (which.isArray())
    {
        const auto & at = assert_cast<const DataTypeArray &>(type);
        const auto & arr = field.safeGet<Array>();
        std::vector<String> elements;
        elements.reserve(arr.size());
        size_t data_size = 0;
        for (const auto & e : arr)
        {
            elements.push_back(encodeCHValue(e, *at.getNestedType(), key_ids, field_id_size));
            data_size += elements.back().size();
        }
        size_t field_offset_size = bytesForValue(data_size);
        bool is_large = arr.size() > 0xFF;
        out.push_back(static_cast<char>(
            static_cast<UInt8>(BasicType::Array)
            | (static_cast<UInt8>(field_offset_size - 1) << 2)
            | (static_cast<UInt8>(is_large ? 1 : 0) << 4)));
        writeLE(out, arr.size(), is_large ? 4 : 1);
        size_t offset = 0;
        for (const auto & e : elements)
        {
            writeLE(out, offset, field_offset_size);
            offset += e.size();
        }
        writeLE(out, offset, field_offset_size);
        for (const auto & e : elements)
            out.append(e);
        return out;
    }
    if (which.isTuple())
    {
        const auto & tt = assert_cast<const DataTypeTuple &>(type);
        const auto & tup = field.safeGet<Tuple>();
        std::vector<std::pair<String, String>> entries;
        entries.reserve(tup.size());
        for (size_t i = 0; i < tup.size(); ++i)
            entries.emplace_back(tt.getNameByPosition(i + 1), encodeCHValue(tup[i], *tt.getElement(i), key_ids, field_id_size));
        return buildObjectBytes(std::move(entries), key_ids, field_id_size);
    }
    if (which.isMap())
    {
        const auto & mt = assert_cast<const DataTypeMap &>(type);
        std::vector<std::pair<String, String>> entries;
        for (const auto & entry_field : field.safeGet<Map>())
        {
            const auto & entry = entry_field.safeGet<Tuple>();
            entries.emplace_back(entry[0].safeGet<String>(), encodeCHValue(entry[1], *mt.getValueType(), key_ids, field_id_size));
        }
        return buildObjectBytes(std::move(entries), key_ids, field_id_size);
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type {} is not supported for Parquet variant encoding", type.getName());
}

}

void decodeVariantToJSON(std::string_view metadata, std::string_view value, WriteBuffer & out)
{
    Metadata meta = parseMetadata(metadata);
    decodeValueAt(value, 0, meta, out);
}

String decodeVariantToJSONString(std::string_view metadata, std::string_view value)
{
    String result;
    WriteBufferFromString buf(result);
    decodeVariantToJSON(metadata, value, buf);
    buf.finalize();
    return result;
}

VariantField decodeVariantToField(std::string_view metadata, std::string_view value)
{
    Metadata meta = parseMetadata(metadata);
    return decodeValueToField(value, 0, meta);
}

VariantBinary encodeVariant(const Field & field, const IDataType & type)
{
    std::set<String> keys;
    collectCHKeys(field, type, keys);

    std::map<String, size_t> key_ids;
    size_t id = 0;
    for (const auto & key : keys)
        key_ids[key] = id++;

    VariantBinary result;

    String strings;
    std::vector<size_t> offsets;
    offsets.reserve(keys.size() + 1);
    offsets.push_back(0);
    for (const auto & key : keys)
    {
        strings.append(key);
        offsets.push_back(strings.size());
    }
    size_t offset_size = bytesForValue(std::max(strings.size(), keys.size()));
    result.metadata.push_back(static_cast<char>(0x01 | (1 << 4) | (static_cast<UInt8>(offset_size - 1) << 6)));
    writeLE(result.metadata, keys.size(), offset_size);
    for (size_t off : offsets)
        writeLE(result.metadata, off, offset_size);
    result.metadata.append(strings);

    size_t field_id_size = bytesForValue(keys.empty() ? 0 : keys.size() - 1);
    result.value = encodeCHValue(field, type, key_ids, field_id_size);
    return result;
}

VariantBinary encodeJSONToVariant(std::string_view json)
{
    /// Poco's JSON parser only accepts objects/arrays at the top level, not bare scalars
    /// (e.g. `42`, `"x"`, `true`). Wrap in an array and take the single element so scalars,
    /// objects and arrays are all parsed uniformly.
    Poco::JSON::Parser parser;
    auto wrapped = parser.parse("[" + std::string(json) + "]").extract<Poco::JSON::Array::Ptr>();
    Poco::Dynamic::Var parsed = wrapped->get(0);

    std::set<String> keys;
    collectKeys(parsed, keys);

    std::map<String, size_t> key_ids;
    size_t id = 0;
    for (const auto & key : keys)
        key_ids[key] = id++;

    VariantBinary result;

    /// Metadata: header + dictionary_size + offsets + string bytes.
    String strings;
    std::vector<size_t> offsets;
    offsets.reserve(keys.size() + 1);
    offsets.push_back(0);
    for (const auto & key : keys)
    {
        strings.append(key);
        offsets.push_back(strings.size());
    }
    size_t offset_size = bytesForValue(strings.size());

    /// sorted_strings = 1 (keys are stored sorted), version = 1.
    result.metadata.push_back(static_cast<char>(0x01 | (1 << 4) | (static_cast<UInt8>(offset_size - 1) << 6)));
    writeLE(result.metadata, keys.size(), offset_size);
    for (size_t off : offsets)
        writeLE(result.metadata, off, offset_size);
    result.metadata.append(strings);

    size_t field_id_size = bytesForValue(keys.empty() ? 0 : keys.size() - 1);
    result.value = encodeValue(parsed, key_ids, field_id_size);
    return result;
}

}
