#include "MySQLJsonConv.h"
#include "PacketsProtocolText.h"
#include <iostream>
#include <Common/Exception.h>
#include <base/defines.h>
#include <sstream>
#include <memory>
#include <iomanip>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}
// Referenced the opensourced canal and MySQL json_bin
//  This file specifies reading values back from the binary representation.

//   The binary format is as follows:

//   Each JSON value (scalar, object or array) has a one byte type
//   identifier followed by the actual value.

//   If the value is a JSON object, its binary representation will have a
//   header that contains:

//   - the member count
//   - the size of the binary value in bytes
//   - a list of pointers to each key
//   - a list of pointers to each value

//   The actual keys and values will come after the header, in the same
//   order as in the header.

//   Similarly, if the value is a JSON array, the binary representation
//   will have a header with

//   - the element count
//   - the size of the binary value in bytes
//   - a list of pointers to each value

//   followed by the actual values, in the same order as in the header.

//   type value

//   type ::=
//       0x00 |        small JSON object
//       0x01 |        large JSON object
//       0x02 |        small JSON array
//       0x03 |        large JSON array
//       0x04 |        literal (true/false/null)
//       0x05 |        int16
//       0x06 |        uint16
//       0x07 |        int32
//       0x08 |        uint32
//       0x09 |        int64
//       0x0a |        uint64
//       0x0b |        double
//       0x0c |        utf8mb4 string
//       0x0f          custom data (any MySQL data type)

//   value ::=
//       object  |
//       array   |
//       literal |
//       number  |
//       string  |
//       custom-data

//   object ::= element-count size key-entry* value-entry* key* value*

//   array ::= element-count size value-entry* value*

//   // number of members in object or number of elements in array
//   element-count ::=
//       uint16 |   if used in small JSON object/array
//       uint32     if used in large JSON object/array

//   // number of bytes in the binary representation of the object or array
//   size ::=
//       uint16 |   if used in small JSON object/array
//       uint32     if used in large JSON object/array

//   key-entry ::= key-offset key-length

//   key-offset ::=
//       uint16 |   if used in small JSON object
//       uint32     if used in large JSON object

//   key-length ::= uint16    // key length must be less than 64KB

//   value-entry ::= type offset-or-inlined-value

//   This field holds either the offset to where the value is stored,
//   or the value itself if it is small enough to be inlined (that is,
//   if it is a JSON literal or a small enough [u]int).
//   offset-or-inlined-value ::=
//       uint16 |    if used in small JSON object/array
//       uint32      if used in large JSON object/array

//   key ::= utf8mb4-data

//   literal ::=
//       0x00 |    JSON null literal
//       0x01 |    JSON true literal
//       0x02 |    JSON false literal

//   number ::=  ....   little-endian format for [u]int(16|32|64), whereas
//                      double is stored in a platform-independent, eight-byte
//                      format using float8store()

//   string ::= data-length utf8mb4-data

//   custom-data ::= custom-type data-length binary-data

//   custom-type ::= uint8    type identifier that matches the
//                            internal enum_field_types enum

//   data-length ::= uint8*   If the high bit of a byte is 1, the length
//                            field is continued in the next byte,
//                            otherwise it is the last byte of the length
//                            field. So we need 1 byte to represent
//                            lengths up to 127, 2 bytes to represent
//                            lengths up to 16383, and so on...


constexpr char JSONB_TYPE_SMALL_OBJECT = 0x0;
constexpr char JSONB_TYPE_LARGE_OBJECT = 0x1;
constexpr char JSONB_TYPE_SMALL_ARRAY = 0x2;
constexpr char JSONB_TYPE_LARGE_ARRAY = 0x3;
constexpr char JSONB_TYPE_LITERAL = 0x4;
constexpr char JSONB_TYPE_INT16 = 0x5;
constexpr char JSONB_TYPE_UINT16 = 0x6;
constexpr char JSONB_TYPE_INT32 = 0x7;
constexpr char JSONB_TYPE_UINT32 = 0x8;
constexpr char JSONB_TYPE_INT64 = 0x9;
constexpr char JSONB_TYPE_UINT64 = 0xA;
constexpr char JSONB_TYPE_DOUBLE = 0xB;
constexpr char JSONB_TYPE_STRING = 0xC;
constexpr char JSONB_TYPE_OPAQUE = 0xF;

constexpr char JSONB_NULL_LITERAL = 0x0;
constexpr char JSONB_TRUE_LITERAL = 0x1;
constexpr char JSONB_FALSE_LITERAL = 0x2;

/*
 * The size of offset or size fields in the small and the large storage
 * format for JSON objects and JSON arrays.
 */
constexpr UInt8 SMALL_OFFSET_SIZE = 2;
constexpr UInt8 LARGE_OFFSET_SIZE = 4;

constexpr UInt8 DOUBLE_PRECISION = 15;
constexpr UInt8 MAX_TIME_LENGTH = 16;
constexpr UInt8 MAX_DATE_LENGTH = 10;
constexpr UInt8 MAX_DATETIME_LENGTH = 26;

/*
 * The size of key entries for objects when using the small storage format
 * or the large storage format. In the small format it is 4 bytes (2 bytes
 * for key length and 2 bytes for key offset). In the large format it is 6
 * (2 bytes for length, 4 bytes for offset).
 */
constexpr UInt8 KEY_ENTRY_SIZE_SMALL = 2 + SMALL_OFFSET_SIZE;
constexpr UInt8 KEY_ENTRY_SIZE_LARGE = 2 + LARGE_OFFSET_SIZE;

/*
 * The size of value entries for objects or arrays. When using the small
 * storage format, the entry size is 3 (1 byte for type, 2 bytes for
 * offset). When using the large storage format, it is 5 (1 byte for type, 4
 * bytes for offset).
 */
constexpr UInt8 VALUE_ENTRY_SIZE_SMALL = 1 + SMALL_OFFSET_SIZE;
constexpr UInt8 VALUE_ENTRY_SIZE_LARGE = 1 + LARGE_OFFSET_SIZE;

static void checkLength(size_t len, size_t wanted)
{
    if (len < wanted)
    {
        throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
    }
}


static UInt8 getUInt8(const char * buffer)
{
    UInt8 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static Int16 getInt16(const char * buffer)
{
    Int16 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static UInt16 getUInt16(const char * buffer)
{
    UInt16 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static Int32 getInt32(const char * buffer)
{
    Int32 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static UInt32 getUInt32(const char * buffer)
{
    UInt32 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static Int64 getInt64(const char * buffer)
{
    Int64 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static UInt64 getUInt64(const char * buffer)
{
    UInt64 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static double getDouble(const char * buffer)
{
    double ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static bool readVariableLength(const char *data, size_t data_length, UInt32 *length, UInt8 *num)
{
    /*
    It takes five bytes to represent UINT_MAX, which is the largest
    supported length, so don't look any further.
    */
    const size_t max_bytes = std::min(data_length, static_cast<size_t>(5));

    size_t len = 0;

    for (size_t i = 0; i < max_bytes; i++)
    {
        // Get the next 7 bits of the length.
        len |= (data[i] & 0x7f) << (7 * i);
        if ((data[i] & 0x80) == 0)
        {
            // The length shouldn't exceed 32 bits.
            if (len > UINT_MAX)
                return true; /* purecov: inspected */

            // This was the last byte. Return successfully.
            *num = static_cast<UInt8>(i + 1);
            *length = static_cast<UInt32>(len);
            return false;
        }
    }

    // No more available bytes. Return true to signal error.
    return true;
}

static size_t readOffsetOrSize(const char * buffer, bool large)
{
    return large ? getUInt32(buffer) : getUInt16(buffer);
}


static JsonValue parseArrayOrObject(JsonEnumType json_type, const char * data, size_t len, bool large)
{
    size_t offset_size = large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;

    checkLength(len , 2 * offset_size);
    auto element_count = readOffsetOrSize(data, large);

    auto bytes = readOffsetOrSize(data + offset_size, large);

    if (bytes > len)
    {
        throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
    }

    auto header_size = 2 * offset_size;
    if (json_type == JsonEnumType::OBJECT)
    {
        header_size += element_count * (large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL);
    }

    if (header_size > bytes)
    {
        throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
    }

    return JsonValue(json_type, data, bytes, element_count, large);
}

// The compiler will generate a lot of warnings, but there is no fallthrough.
// It will return before label break.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"

static JsonValue parseScalar(Int32 type, const char * data, size_t len)
{
    switch (type)
    {
        case JSONB_TYPE_LITERAL:
            checkLength(len , 1);
            switch (data[0])
            {
                case JSONB_NULL_LITERAL:
                    return JsonValue(JsonEnumType::LITERAL_NULL);
                case JSONB_TRUE_LITERAL:
                    return JsonValue(JsonEnumType::LITERAL_TRUE);
                case JSONB_FALSE_LITERAL:
                    return JsonValue(JsonEnumType::LITERAL_FALSE);
                default:
                    throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
            }

        case JSONB_TYPE_INT16:
            checkLength(len , 2);
            return JsonValue(JsonEnumType::INT, static_cast<Int64>(getInt16(data)));

        case JSONB_TYPE_INT32:
            checkLength(len , 4);
            return JsonValue(JsonEnumType::INT, static_cast<Int64>(getInt32(data)));

        case JSONB_TYPE_INT64:
            checkLength(len , 8);
            return JsonValue(JsonEnumType::INT, static_cast<Int64>(getInt64(data)));

        case JSONB_TYPE_UINT16:
            checkLength(len , 2);
            return JsonValue(JsonEnumType::UINT, static_cast<Int64>(getUInt16(data)));

        case JSONB_TYPE_UINT32:
            checkLength(len , 4);
            return JsonValue(JsonEnumType::UINT, static_cast<Int64>(getUInt32(data)));

        case JSONB_TYPE_UINT64:
            checkLength(len , 8);
            return JsonValue(JsonEnumType::UINT, static_cast<Int64>(getUInt64(data)));

        case JSONB_TYPE_DOUBLE:
            checkLength(len , 8);
            return JsonValue(JsonEnumType::DOUBLE, getDouble(data));

        case JSONB_TYPE_STRING:
        {
            UInt32 str_len;
            UInt8 num_bytes;
            if (readVariableLength(data, len, &str_len, &num_bytes))
            {
                throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
            }
            checkLength(len , num_bytes + str_len);
            return JsonValue(JsonEnumType::STRING, data + num_bytes, static_cast<size_t>(str_len));
        }

        case JSONB_TYPE_OPAQUE:
        {
            checkLength(len , 1);

            UInt8 type_byte = getUInt8(data);

            // Then there's the length of the value.
            UInt32 val_len;
            UInt8 num_bytes;
            if (readVariableLength(data + 1, len - 1, &val_len, &num_bytes))
            {
                throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
            }

            checkLength(len , 1 + num_bytes + val_len);
            return JsonValue(type_byte, data + 1 + num_bytes, static_cast<size_t>(val_len));
        }
    }
     UNREACHABLE();
}
#pragma GCC diagnostic pop

static JsonValue parseValue(Int32 type, const char * data, size_t len)
{
    switch (type)
    {
        case JSONB_TYPE_SMALL_OBJECT:
            return parseArrayOrObject(JsonEnumType::OBJECT, data, len, false);
        case JSONB_TYPE_LARGE_OBJECT:
            return parseArrayOrObject(JsonEnumType::OBJECT, data, len, true);
        case JSONB_TYPE_SMALL_ARRAY:
            return parseArrayOrObject(JsonEnumType::ARRAY, data, len, false);
        case JSONB_TYPE_LARGE_ARRAY:
            return parseArrayOrObject(JsonEnumType::ARRAY, data, len, true);
        default:
            return parseScalar(type, data, len);
    }
}

JsonValue parseBinary(const String &in)
{
    if (in.empty())
    {
        return JsonValue(JsonEnumType::LITERAL_NULL);
    }

    const char * buffer = in.data();
    size_t len = in.size();
    auto type = *buffer;

    buffer += 1;
    len -= 1;
    return parseValue(type, buffer, len);
}

JsonValue JsonValue::getJsonValueForElement(size_t pos)
{
    const auto offset_size = large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
    const auto key_entry_size = large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
    const auto value_entry_size = large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
    auto first_entry_offset = 2 * offset_size;
    if (json_type == JsonEnumType::OBJECT)
    {
        first_entry_offset += element_count * key_entry_size;
    }
    auto entry_offset = first_entry_offset + value_entry_size * pos;
    auto type = getUInt8(data + entry_offset);
    if (type == JSONB_TYPE_INT16 ||
        type == JSONB_TYPE_UINT16 ||
        type == JSONB_TYPE_LITERAL||
        (large && (type == JSONB_TYPE_INT32 || type == JSONB_TYPE_UINT32)))
    {
        return parseScalar(type, data  + entry_offset + 1, value_entry_size - 1);
    }
    auto value_offset = readOffsetOrSize(data + entry_offset + 1, large);
    return parseValue(type, data + value_offset, data_length - value_offset);

}

String JsonValue::getObjectKey(size_t pos)
{
    const auto offset_size = large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
    const auto key_entry_size = large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
    const auto entry_offset = 2 * offset_size + key_entry_size * pos;

    // The offset of the key is the first part of the key entry.
    const auto key_offset = readOffsetOrSize(data + entry_offset, large);

    // The length of the key is the second part of the entry, always two bytes.
    const auto key_length = getUInt16(data + entry_offset + offset_size);
    return String(data + key_offset, key_length);
}

static void escape(String &in, String& out)
{
    size_t end_index = in.length();
    for (size_t i = 0; i < end_index; ++i)
    {
        char c = in.at(i);
        switch (c)
        {
        case '"':
            out +=  "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            out += c;
            break;
        }
    }
}


static Int8 getInt8(const char * buffer)
{
    Int8 ret;
    memcpy(&ret, buffer, sizeof(ret));
    return ret;
}

static Int16 getInt16BE(const UInt8 * buf)
{
    return static_cast<Int16>(static_cast<UInt32>(buf[1]) +
            (static_cast<UInt32>(buf[0]) << 8));
}

static Int32 getInt24BE(const UInt8 * buf)
{
    return static_cast<Int32>((buf[0] & 128) ? ((255U << 24) | (static_cast<UInt32>(buf[0]) << 16) |
            (static_cast<UInt32>(buf[1]) << 8) | static_cast<UInt32>(buf[2]))
            : ((static_cast<UInt32>(buf[0]) << 16) |
            (static_cast<UInt32>(buf[1]) << 8) | (static_cast<UInt32>(buf[2]))));
}

static Int32 getInt32BE(const UInt8 * buf)
{
    return static_cast<Int32>(static_cast<UInt32>(buf[3]) +
        (static_cast<UInt32>(buf[2]) << 8) +
        (static_cast<UInt32>(buf[1]) << 16) +
        (static_cast<UInt32>(buf[0]) << 24));
}

static void parseDecimalAndConv2String(const char * data, size_t data_len, String & out)
{
    /* decimal representation */
    static constexpr Int32 DIG_PER_DEC  = 9;
    static constexpr Int32 DIG_BASE      = 1000000000;
    static constexpr Int32 DIG_MAX       = DIG_BASE - 1;
    static constexpr Int32 dig2bytes[]   = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
    static constexpr Int32 powers10[]    = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

    static constexpr Int32 DIG_PER_INT32 = 9;
    static constexpr Int32 SIZE_OF_INT32 = 4;

    size_t cur_offset = 0;
    Int32 precision = static_cast<UInt8>(getInt8(data));
    cur_offset += 1;
    Int32 scale = static_cast<UInt8>(getInt8(data + cur_offset));
    cur_offset += 1;

    Int32 intg = precision - scale;
    Int32 frac = scale;
    Int32 intg0 = intg / DIG_PER_INT32;
    Int32 frac0 = frac / DIG_PER_INT32;
    Int32 intg0x = intg - intg0 * DIG_PER_INT32;
    Int32 frac0x = frac - frac0 * DIG_PER_INT32;

    Int32 bin_size = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x] + frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];

    if (bin_size + cur_offset > data_len)
    {
        throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
    }

    Int32 mask = (*(data + cur_offset) & 0x80) ? 0 : -1;
    Int32 from = 0;

    /* max string length */
    Int32 len = ((mask != 0) ? 1 : 0) + ((intg != 0) ? intg : 1) + ((frac != 0) ? 1 : 0) + frac;
    // Don't want to manage memory manually, just use the buf
    String dec_result(len, '\0');
    char * buf = dec_result.data();

    Int32 pos = 0;

    if (mask != 0) /* decimal sign */
    {
        buf[pos++] = ('-');
    }

    // Don't want to manage memory manually
    String deep_copy(data + cur_offset, bin_size);
    UInt8 * data_prt = reinterpret_cast<UInt8 *>(deep_copy.data());

    data_prt[from] ^= 0x80; /* clear sign */
    Int32 mark = pos;

    if (intg0x != 0)
    {
        Int32 bytes = dig2bytes[intg0x];
        Int32 val = 0;
        switch (bytes)
        {
            case 1:
                val = data_prt[from] /* one byte */;
                break;
            case 2:
                val = getInt16BE(data_prt + from);
                break;
            case 3:
                val = getInt24BE(data_prt + from);
                break;
            case 4:
                val = getInt32BE(data_prt + from);
                break;
        }
        from += bytes;
        val ^= mask;
        if (val < 0 || val >= powers10[intg0x + 1])
        {
            throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
        }
        if (val != 0)
        {
            for (Int32 j = intg0x; j > 0; j--)
            {
                Int32 divisor = powers10[j - 1];
                Int32 div_val = val / divisor;
                if (mark < pos || div_val != 0)
                {
                    buf[pos++] = (static_cast<char>('0' + div_val));
                }
                val -= div_val * divisor;
            }
        }
    }

    for (Int32 stop = from + intg0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32)
    {
        Int32 val = getInt32BE(data_prt + from);
        val ^= mask;
        if (val < 0 || val > DIG_MAX)
        {
            throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
        }
        if (val != 0)
        {
            if (mark < pos)
            {
                for (Int32 i = DIG_PER_DEC; i > 0; i--)
                {
                    Int32 divisor = powers10[i - 1];
                    Int32 div_val = val / divisor;
                    buf[pos++] = (static_cast<char>('0' + div_val));
                    val -= div_val * divisor;
                }
            }
            else
            {
                for (Int32 i = DIG_PER_DEC; i > 0; i--)
                {
                    Int32 divisor = powers10[i - 1];
                    Int32 div_val = val / divisor;
                    if (mark < pos || div_val != 0)
                    {
                        buf[pos++] = (static_cast<char>('0' + div_val));
                    }
                    val -= div_val * divisor;
                }
            }
        }
        else if (mark < pos)
        {
            for (Int32 i = DIG_PER_DEC; i > 0; i--)
            {
                buf[pos++] = ('0');
            }
        }
    }

    if (mark == pos)
    {
        /* fix 0.0 problem, only '.' may cause BigDecimal parsing exception. */
        buf[pos++] = ('0');
    }

    if (frac > 0)
    {
        buf[pos++] = ('.');
        mark = pos;

        for (Int32 stop = from + frac0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32)
        {
            Int32 val = getInt32BE(data_prt + from);
            val ^= mask;
            if (val < 0 || val > DIG_MAX)
            {
                throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
            }
            if (val != 0)
            {
                for (Int32 i = DIG_PER_DEC; i > 0; i--)
                {
                    Int32 divisor = powers10[i - 1];
                    Int32 div_val = val / divisor;
                    buf[pos++] = (static_cast<char>('0' + div_val));
                    val -= div_val * divisor;
                }
            }
            else
            {
                for (Int32 i = DIG_PER_DEC; i > 0; i--)
                {
                    buf[pos++] = ('0');
                }
            }
        }

        if (frac0x != 0)
        {
            Int32 bytes = dig2bytes[frac0x];
            Int32 val = 0;
            switch (bytes)
            {
                case 1:
                    val = data_prt[from] /* one byte */;
                    break;
                case 2:
                    val = getInt16BE(data_prt + from);
                    break;
                case 3:
                    val = getInt24BE(data_prt + from);
                    break;
                case 4:
                    val = getInt32BE(data_prt + from);
                    break;
            }
            val ^= mask;
            if (val != 0)
            {
                Int32 dig = DIG_PER_DEC - frac0x;
                val *= powers10[dig];
                if (val < 0 || val > DIG_MAX)
                {
                    throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "illegal json data");
                }

                for (Int32 j = DIG_PER_DEC; j > dig; j--)
                {
                    Int32 divisor = powers10[j - 1];
                    Int32 div_val = val / divisor;
                    buf[pos++] = (static_cast<char>('0' + div_val));
                    val -= div_val * divisor;
                }
            }
        }

        if (mark == pos)
        {
            /* make number more friendly */
            buf[pos++] = ('0');
        }
    }
    out += String(buf, pos);
}

// YEAR YYYY 1901 ~ 2155 1 byte
// TIME HH:MM:SS -838:59:59 ~ 838:59:59 3 bytes
// DATE YYYY-MM-DD 1000-01-01 ~ 9999-12-3 3 bytes
// DATETIME YYYY-MM-DD HH:MM:SS 1000-01-01 00:00:00 ~ 9999-12-31 23:59:59 8 bytes
// TIMESTAMP YYYY-MM-DD HH:MM:SS 1980-01-01 00:00:01 UTC ~ 2040-01-19 03:14:07 UTC 4 bytes
void JsonValue::parseOpaque(String & out)
{
    using namespace MySQLProtocol;
    using namespace MySQLProtocol::ProtocolText;
    String text;
    switch (field_type)
    {
        case MYSQL_TYPE_NEWDECIMAL:
            parseDecimalAndConv2String(data, data_length, out);
            break;

        case MYSQL_TYPE_TIME:
        {
            Int64 packed_value = getInt64(data);
            if (packed_value == 0)
            {
                text = String("00:00:00");
            }
            else
            {
                UInt64 ultime = std::abs(packed_value);
                Int64 intpart = static_cast<Int64>(ultime >> 24);
                UInt32 frac = static_cast<UInt32>(ultime % (1L << 24));
                text.reserve(MAX_TIME_LENGTH + 1);
                UInt8 offset = 0;
                out += '"';
                if (packed_value < 0)
                {
                    out += '-';
                }

                Int32 hour = static_cast<Int32>((intpart >> 12) % (1 << 10));
                // -838:59:59 ~ 838:59:59
                if (hour > 100)
                {
                    text.resize(MAX_TIME_LENGTH);
                    (void) sprintf(text.data() + offset, "%3d", hour);
                    offset += 3;
                }
                else
                {
                    text.resize(MAX_TIME_LENGTH -1);
                    (void) sprintf(text.data() + offset, "%02d", hour);
                    offset += 2;
                }
                (void) sprintf(text.data() + offset, ":%02d:%02d.%06d",
                        static_cast<Int32> (intpart >> 6) % (1 << 6),
                        static_cast<Int32> (intpart % (1 << 6)),
                        frac);
            }
            out += (text + '"');
            break;
        }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        {
            Int64 packed_value = getInt64(data);
            if (packed_value == 0)
            {
                text = "0000-00-00 00:00:00";
            }
            else
            {
                UInt64 ultime = std::abs(packed_value);
                Int64 intpart = static_cast<Int64>(ultime >> 24);
                Int32 frac = static_cast<Int32> (ultime % (1L << 24));
                Int64 ymd = intpart >> 17;
                Int64 ym = ymd >> 5;
                Int64 hms = intpart % (1 << 17);
                text.reserve(MAX_DATETIME_LENGTH + 1);
                if (field_type == MYSQL_TYPE_DATE)
                {
                    text.resize(MAX_DATE_LENGTH);
                    (void) sprintf(text.data(), "%04d-%02d-%02d",
                        static_cast<Int32> (ym / 13),
                        static_cast<Int32> (ym % 13),
                        static_cast<Int32> (ymd % (1 << 5)));

                }
                else
                {
                    text.resize(MAX_DATETIME_LENGTH);
                    (void) sprintf(text.data(), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                        static_cast<Int32> (ym / 13),
                        static_cast<Int32> (ym % 13),
                        static_cast<Int32> (ymd % (1 << 5)),
                        static_cast<Int32> (hms >> 12),
                        static_cast<Int32> ((hms >> 6) % (1 << 6)),
                        static_cast<Int32> (hms % (1 << 6)),
                        frac);

                }
            }
            out += ('"' + text +'"');
            break;
        }
        default:
        {
            text = String(data, data_length);
            out += '"';
            escape(text, out);
            out += '"';
            break;
        }
    }
}

void JsonValue::toJsonString(String & out)
{
    switch (json_type)
    {
        case JsonEnumType::OBJECT:
            out += '{';
            for (size_t i = 0; i < element_count; i++)
            {
                if (i > 0)
                {
                    out += ", ";
                }
                out += '"';
                String key = getObjectKey(i);
                escape(key, out);
                out += "\": ";
                getJsonValueForElement(i).toJsonString(out);
            }

            out += '}';
            break;

        case JsonEnumType::ARRAY:
            out += '[';
            for (size_t i = 0; i < element_count; i++)
            {
                if (i > 0)
                {
                    out += ", ";
                }
                getJsonValueForElement(i).toJsonString(out);
            }
            out += ']';
            break;

        case JsonEnumType::DOUBLE:
        {
            // ostringstream will remove the tail '0' automatically
            std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            // the default precision is 6, only 15 precision is valid. bigger precision is invalid data.
            // e.g. 3.1415926535 with precision 32 will get 3.1415926535000000541231202078052
            oss << std::setprecision(DOUBLE_PRECISION) << double_value;
            out += oss.str();
            break;
        }
        case JsonEnumType::INT:
            out += std::to_string(int_value);
            break;

        case JsonEnumType::UINT:
            out += std::to_string(static_cast<UInt64>(int_value));
            break;

        case JsonEnumType::LITERAL_FALSE:
            out += "false";
            break;

        case JsonEnumType::LITERAL_TRUE:
            out += "true";
            break;

        case JsonEnumType::LITERAL_NULL:
            out += "null";
            break;

        case JsonEnumType::OPAQUE:
            parseOpaque(out);
            break;

        case JsonEnumType::STRING:
            out += '"';
            escape(string_value, out);
            out += '"';
            break;

        case JsonEnumType::ERROR:
            throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "unknown json type");
    }
}

}
