#include <Processors/Formats/Impl/Parquet/VariantDecoding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <Common/DateLUT.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/Decimal.h>
#include <base/extended_types.h>

#include <algorithm>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstring>
#include <numeric>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_DEEP_RECURSION;
}

namespace DB::Parquet
{

namespace
{

/// Variant basic types (low 2 bits of the value header byte).
constexpr uint8_t VARIANT_BASIC_PRIMITIVE = 0;
constexpr uint8_t VARIANT_BASIC_SHORT_STRING = 1;
constexpr uint8_t VARIANT_BASIC_OBJECT = 2;
constexpr uint8_t VARIANT_BASIC_ARRAY = 3;

/// Variant primitive type ids (value_header for basic_type == 0).
constexpr uint8_t VARIANT_NULL = 0;
constexpr uint8_t VARIANT_TRUE = 1;
constexpr uint8_t VARIANT_FALSE = 2;
constexpr uint8_t VARIANT_INT8 = 3;
constexpr uint8_t VARIANT_INT16 = 4;
constexpr uint8_t VARIANT_INT32 = 5;
constexpr uint8_t VARIANT_INT64 = 6;
constexpr uint8_t VARIANT_DOUBLE = 7;
constexpr uint8_t VARIANT_DECIMAL4 = 8;
constexpr uint8_t VARIANT_DECIMAL8 = 9;
constexpr uint8_t VARIANT_DECIMAL16 = 10;
constexpr uint8_t VARIANT_DATE = 11;
constexpr uint8_t VARIANT_TIMESTAMP_TZ_MICROS = 12;
constexpr uint8_t VARIANT_TIMESTAMP_NTZ_MICROS = 13;
constexpr uint8_t VARIANT_FLOAT = 14;
constexpr uint8_t VARIANT_BINARY = 15;
constexpr uint8_t VARIANT_STRING = 16;
constexpr uint8_t VARIANT_TIME_NTZ_MICROS = 17;
constexpr uint8_t VARIANT_TIMESTAMP_TZ_NANOS = 18;
constexpr uint8_t VARIANT_TIMESTAMP_NTZ_NANOS = 19;
constexpr uint8_t VARIANT_UUID = 20;

template <typename T>
T readLittleEndian(const char * p)
{
    if constexpr (std::is_same_v<T, float>)
        return std::bit_cast<float>(readLittleEndian<uint32_t>(p));
    else if constexpr (std::is_same_v<T, double>)
        return std::bit_cast<double>(readLittleEndian<uint64_t>(p));
    else
    {
        make_unsigned_t<T> v = 0;
        for (size_t i = 0; i < sizeof(T); ++i)
            v |= make_unsigned_t<T>(static_cast<uint8_t>(p[i])) << (8 * i);
        return T(v);
    }
}

uint32_t readUnsignedLittleEndian(const char * p, size_t size)
{
    uint32_t v = 0;
    for (size_t i = 0; i < size; ++i)
        v |= uint32_t(static_cast<uint8_t>(p[i])) << (8 * i);
    return v;
}

void checkDepth(size_t depth, const FormatSettings & settings)
{
    checkStackSize();
    if (settings.max_parser_depth != 0 && depth > settings.max_parser_depth)
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Variant value is nested deeper than the limit ({})", settings.max_parser_depth);
}

void writeFloatJSON(Float64 x, WriteBuffer & out, const FormatSettings & settings)
{
    /// JSON has no representation for inf/nan.
    if (!isFinite(x))
    {
        out.write("null", 4);
        return;
    }
    writeFloatText(x, out, settings);
}

void variantPrimitiveToJSON(uint8_t primitive_type, std::string_view data, WriteBuffer & out, const FormatSettings & settings)
{
    switch (primitive_type)
    {
        case VARIANT_NULL:
            out.write("null", 4);
            return;
        case VARIANT_TRUE:
            out.write("true", 4);
            return;
        case VARIANT_FALSE:
            out.write("false", 5);
            return;
        case VARIANT_INT8:
        case VARIANT_INT16:
        case VARIANT_INT32:
        case VARIANT_INT64:
        {
            size_t size = 1UZ << (primitive_type - VARIANT_INT8);
            if (data.size() < size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant integer value: {} bytes for int{}", data.size(), size * 8);
            Int64 v;
            switch (size)
            {
                case 1: v = Int64(static_cast<Int8>(data[0])); break;
                case 2: v = Int64(readLittleEndian<Int16>(data.data())); break;
                case 4: v = Int64(readLittleEndian<Int32>(data.data())); break;
                default: v = readLittleEndian<Int64>(data.data()); break;
            }
            writeIntText(v, out);
            return;
        }
        case VARIANT_DOUBLE:
        {
            if (data.size() < 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant double value: {} bytes", data.size());
            writeFloatJSON(readLittleEndian<double>(data.data()), out, settings);
            return;
        }
        case VARIANT_DECIMAL4:
        case VARIANT_DECIMAL8:
        case VARIANT_DECIMAL16:
        {
            size_t size = primitive_type == VARIANT_DECIMAL4 ? 4 : (primitive_type == VARIANT_DECIMAL8 ? 8 : 16);
            if (data.size() < 1 + size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant decimal value: {} bytes", data.size());
            UInt32 scale = static_cast<uint8_t>(data[0]);
            if (scale > 38)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant decimal value: scale {}", scale);
            if (size == 4)
                writeText(Decimal32(readLittleEndian<Int32>(data.data() + 1)), scale, out);
            else if (size == 8)
                writeText(Decimal64(readLittleEndian<Int64>(data.data() + 1)), scale, out);
            else
                writeText(Decimal128(readLittleEndian<Int128>(data.data() + 1)), scale, out);
            return;
        }
        case VARIANT_DATE:
        {
            if (data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant date value: {} bytes", data.size());
            Int32 days = readLittleEndian<Int32>(data.data());
            out.write('"');
            writeDateText(ExtendedDayNum(days), out, DateLUT::instance("UTC"));
            out.write('"');
            return;
        }
        case VARIANT_TIMESTAMP_TZ_MICROS:
        case VARIANT_TIMESTAMP_NTZ_MICROS:
        case VARIANT_TIMESTAMP_TZ_NANOS:
        case VARIANT_TIMESTAMP_NTZ_NANOS:
        {
            if (data.size() < 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant timestamp value: {} bytes", data.size());
            Int64 v = readLittleEndian<Int64>(data.data());
            UInt32 scale = (primitive_type == VARIANT_TIMESTAMP_TZ_MICROS || primitive_type == VARIANT_TIMESTAMP_NTZ_MICROS) ? 6 : 9;
            /// NTZ values are rendered as wall time, which UTC gives us directly.
            out.write('"');
            writeDateTimeTextISO(DateTime64(v), scale, out, DateLUT::instance("UTC"));
            out.write('"');
            return;
        }
        case VARIANT_FLOAT:
        {
            if (data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant float value: {} bytes", data.size());
            writeFloatJSON(static_cast<Float64>(readLittleEndian<float>(data.data())), out, settings);
            return;
        }
        case VARIANT_BINARY:
        case VARIANT_STRING:
        {
            if (data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant string value: {} bytes", data.size());
            uint32_t size = readLittleEndian<uint32_t>(data.data());
            if (data.size() < 4 + size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant string value: size {} exceeds available {} bytes", size, data.size() - 4);
            writeJSONString(std::string_view(data.data() + 4, size), out, settings);
            return;
        }
        case VARIANT_TIME_NTZ_MICROS:
        {
            if (data.size() < 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant time value: {} bytes", data.size());
            Int64 micros = readLittleEndian<Int64>(data.data());
            if (micros < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant time value: {} microseconds", micros);
            UInt64 hour = UInt64(micros) / 3600000000;
            UInt64 minute = UInt64(micros) / 60000000 % 60;
            UInt64 second = UInt64(micros) / 1000000 % 60;
            UInt64 fraction = UInt64(micros) % 1000000;
            writeJSONString(fmt::format("{:02}:{:02}:{:02}.{:06}", hour, minute, second, fraction), out, settings);
            return;
        }
        case VARIANT_UUID:
        {
            if (data.size() < 16)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant UUID value: {} bytes", data.size());
            /// 16 bytes big-endian (RFC 4122 byte order): hex of the bytes in stored order.
            constexpr char hex[] = "0123456789abcdef";
            char buf[36];
            size_t j = 0;
            for (size_t i = 0; i < 16; ++i)
            {
                if (i == 4 || i == 6 || i == 8 || i == 10)
                    buf[j++] = '-';
                uint8_t b = static_cast<uint8_t>(data[i]);
                buf[j++] = hex[b >> 4];
                buf[j++] = hex[b & 0x0F];
            }
            writeJSONString(std::string_view(buf, sizeof(buf)), out, settings);
            return;
        }
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown variant primitive type id {}", primitive_type);
    }
}

/// Parse an object value; calls `visit(key, value_bytes)` for each field.
template <typename F>
void parseVariantObject(std::string_view value, const VariantMetadata & metadata, F && visit)
{
    uint8_t header = static_cast<uint8_t>(value[0]);
    uint8_t value_header = header >> 2;
    bool is_large = (value_header >> 4) & 1;
    size_t field_id_size = ((value_header >> 2) & 3) + 1;
    size_t field_offset_size = (value_header & 3) + 1;

    const char * p = value.data() + 1;
    const char * end = value.data() + value.size();

    size_t num_elements_size = is_large ? 4 : 1;
    if (size_t(end - p) < num_elements_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: truncated element count");
    uint32_t num_elements = readUnsignedLittleEndian(p, num_elements_size);
    p += num_elements_size;

    if (size_t(end - p) < size_t(num_elements) * field_id_size + (size_t(num_elements) + 1) * field_offset_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: truncated field id/offset lists");

    const char * ids = p;
    const char * offsets = p + size_t(num_elements) * field_id_size;
    const char * fields = offsets + (size_t(num_elements) + 1) * field_offset_size;
    size_t fields_size = size_t(end - fields);

    for (uint32_t i = 0; i < num_elements; ++i)
    {
        uint32_t field_id = readUnsignedLittleEndian(ids + size_t(i) * field_id_size, field_id_size);
        if (field_id >= metadata.dictionary.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: field id {} out of dictionary of {} strings", field_id, metadata.dictionary.size());
        uint32_t offset = readUnsignedLittleEndian(offsets + size_t(i) * field_offset_size, field_offset_size);
        uint32_t next_offset = readUnsignedLittleEndian(offsets + (size_t(i) + 1) * field_offset_size, field_offset_size);
        if (offset > next_offset || next_offset > fields_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: field offsets {}..{} out of {} bytes", offset, next_offset, fields_size);
        visit(metadata.dictionary[field_id], std::string_view(fields + offset, next_offset - offset));
    }
}

void variantValueToJSONImpl(std::string_view value, const VariantMetadata & metadata, WriteBuffer & out, const FormatSettings & settings, size_t depth)
{
    checkDepth(depth, settings);
    if (value.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty variant value");

    uint8_t header = static_cast<uint8_t>(value[0]);
    uint8_t basic_type = header & 3;
    switch (basic_type)
    {
        case VARIANT_BASIC_PRIMITIVE:
            variantPrimitiveToJSON(header >> 2, value.substr(1), out, settings);
            return;
        case VARIANT_BASIC_SHORT_STRING:
        {
            size_t size = header >> 2;
            if (value.size() < 1 + size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant short string: size {} exceeds available {} bytes", size, value.size() - 1);
            writeJSONString(value.substr(1, size), out, settings);
            return;
        }
        case VARIANT_BASIC_OBJECT:
        {
            bool first = true;
            out.write('{');
            parseVariantObject(value, metadata, [&](std::string_view key, std::string_view field_value)
            {
                if (!first)
                    out.write(',');
                first = false;
                writeJSONString(key, out, settings);
                out.write(':');
                variantValueToJSONImpl(field_value, metadata, out, settings, depth + 1);
            });
            out.write('}');
            return;
        }
        case VARIANT_BASIC_ARRAY:
        {
            uint8_t value_header = header >> 2;
            bool is_large = (value_header >> 2) & 1;
            size_t field_offset_size = (value_header & 3) + 1;

            const char * p = value.data() + 1;
            const char * end = value.data() + value.size();
            size_t num_elements_size = is_large ? 4 : 1;
            if (size_t(end - p) < num_elements_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant array: truncated element count");
            uint32_t num_elements = readUnsignedLittleEndian(p, num_elements_size);
            p += num_elements_size;
            if (size_t(end - p) < (size_t(num_elements) + 1) * field_offset_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant array: truncated offset list");
            const char * offsets = p;
            const char * elements = offsets + (size_t(num_elements) + 1) * field_offset_size;
            size_t elements_size = size_t(end - elements);

            out.write('[');
            for (uint32_t i = 0; i < num_elements; ++i)
            {
                uint32_t offset = readUnsignedLittleEndian(offsets + size_t(i) * field_offset_size, field_offset_size);
                uint32_t next_offset = readUnsignedLittleEndian(offsets + (size_t(i) + 1) * field_offset_size, field_offset_size);
                if (offset > next_offset || next_offset > elements_size)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant array: element offsets {}..{} out of {} bytes", offset, next_offset, elements_size);
                if (i != 0)
                    out.write(',');
                variantValueToJSONImpl(std::string_view(elements + offset, next_offset - offset), metadata, out, settings, depth + 1);
            }
            out.write(']');
            return;
        }
    }
}

bool variantValueIsObject(std::string_view value)
{
    return !value.empty() && (static_cast<uint8_t>(value[0]) & 3) == VARIANT_BASIC_OBJECT;
}

/// Minimal JSON document validator. Used to distinguish DuckDB's `typed_value (String)`,
/// which holds a JSON document, from a spec-compliant String shredding, which holds a raw
/// (unquoted) string value.
class JSONDocumentValidator
{
public:
    static bool validate(std::string_view s)
    {
        JSONDocumentValidator v(s);
        v.skipWhitespace();
        if (!v.skipValue(0))
            return false;
        v.skipWhitespace();
        return v.pos == s.size();
    }

private:
    static constexpr size_t MAX_DEPTH = 200;

    std::string_view text;
    size_t pos = 0;

    explicit JSONDocumentValidator(std::string_view s) : text(s) {}

    char peek() const { return pos < text.size() ? text[pos] : '\0'; }
    void skipWhitespace()
    {
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n' || text[pos] == '\r'))
            ++pos;
    }
    bool consume(char c)
    {
        if (peek() != c)
            return false;
        ++pos;
        return true;
    }
    bool consumeLiteral(std::string_view lit)
    {
        if (text.substr(pos, lit.size()) != lit)
            return false;
        pos += lit.size();
        return true;
    }

    bool skipString()
    {
        if (!consume('"'))
            return false;
        while (true)
        {
            if (pos >= text.size())
                return false;
            char c = text[pos++];
            if (c == '"')
                return true;
            if (c == '\\')
            {
                if (pos >= text.size())
                    return false;
                char e = text[pos++];
                if (e == 'u')
                {
                    for (int i = 0; i < 4; ++i)
                    {
                        if (pos >= text.size() || !isHexDigit(text[pos]))
                            return false;
                        ++pos;
                    }
                }
                else if (e != '"' && e != '\\' && e != '/' && e != 'b' && e != 'f' && e != 'n' && e != 'r' && e != 't')
                    return false;
            }
            else if (static_cast<uint8_t>(c) < 0x20)
                return false;
        }
    }

    static bool isHexDigit(char c)
    {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    bool skipNumber()
    {
        if (peek() == '-')
            ++pos;
        if (pos >= text.size())
            return false;
        if (peek() == '0')
            ++pos;
        else if (peek() >= '1' && peek() <= '9')
        {
            while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9')
                ++pos;
        }
        else
            return false;
        if (peek() == '.')
        {
            ++pos;
            if (pos >= text.size() || text[pos] < '0' || text[pos] > '9')
                return false;
            while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9')
                ++pos;
        }
        if (peek() == 'e' || peek() == 'E')
        {
            ++pos;
            if (peek() == '+' || peek() == '-')
                ++pos;
            if (pos >= text.size() || text[pos] < '0' || text[pos] > '9')
                return false;
            while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9')
                ++pos;
        }
        return true;
    }

    bool skipValue(size_t depth)
    {
        if (depth > MAX_DEPTH)
            return false;
        switch (peek())
        {
            case '{':
            {
                ++pos;
                skipWhitespace();
                if (consume('}'))
                    return true;
                while (true)
                {
                    skipWhitespace();
                    if (!skipString())
                        return false;
                    skipWhitespace();
                    if (!consume(':'))
                        return false;
                    skipWhitespace();
                    if (!skipValue(depth + 1))
                        return false;
                    skipWhitespace();
                    if (consume('}'))
                        return true;
                    if (!consume(','))
                        return false;
                }
            }
            case '[':
            {
                ++pos;
                skipWhitespace();
                if (consume(']'))
                    return true;
                while (true)
                {
                    skipWhitespace();
                    if (!skipValue(depth + 1))
                        return false;
                    skipWhitespace();
                    if (consume(']'))
                        return true;
                    if (!consume(','))
                        return false;
                }
            }
            case '"':
                return skipString();
            case 't':
                return consumeLiteral("true");
            case 'f':
                return consumeLiteral("false");
            case 'n':
                return consumeLiteral("null");
            default:
                return skipNumber();
        }
    }
};

/// Cheap check for a JSON document shape: first and last non-whitespace characters are matching
/// brackets or the string is a quoted string. DuckDB's `typed_value (String)` holds a whole JSON
/// document, which always has this shape; strings that fail the sniff are validated fully.
bool looksLikeJSONDocument(std::string_view s)
{
    size_t begin = s.find_first_not_of(" \t\n\r");
    if (begin == std::string_view::npos)
        return false;
    size_t end = s.find_last_not_of(" \t\n\r");
    char first = s[begin];
    char last = s[end];
    if ((first == '{' && last == '}') || (first == '[' && last == ']'))
        return true;
    if (first == '"' && last == '"' && end > begin)
    {
        /// The closing quote must not be escaped (even number of preceding backslashes).
        size_t backslashes = 0;
        size_t p = end;
        while (p > begin && s[p - 1] == '\\')
        {
            --p;
            ++backslashes;
        }
        return backslashes % 2 == 0;
    }
    return false;
}

const IColumn & unwrapNullable(const IColumn & column)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        return nullable->getNestedColumn();
    return column;
}

std::string_view getStringAt(const IColumn & column, size_t row)
{
    return unwrapNullable(column).getDataAt(row);
}

/// Look up the `value` and `typed_value` components of a variant-node tuple (a group with
/// optional `value` and `typed_value` fields, per the shredding spec).
/// If the node is not such a tuple (e.g. a nested VARIANT-annotated group that schema conversion
/// already decoded into a Dynamic column), treat the column itself as the shredded value.
ShreddedValueColumns getVariantNodeColumns(const IDataType & tuple_type, const IColumn & tuple_column)
{
    ShreddedValueColumns res;
    const auto * tuple = typeid_cast<const DataTypeTuple *>(&tuple_type);
    if (!tuple)
    {
        res.typed_value = &tuple_column;
        res.typed_value_type = &tuple_type;
        return res;
    }
    const auto & columns = assert_cast<const ColumnTuple &>(tuple_column);
    if (std::optional<size_t> pos = tuple->tryGetPositionByName("value"))
        res.value = &columns.getColumn(*pos);
    if (std::optional<size_t> pos = tuple->tryGetPositionByName("typed_value"))
    {
        res.typed_value = &columns.getColumn(*pos);
        res.typed_value_type = tuple->getElement(*pos).get();
    }
    return res;
}

/// Cheap presence check used before rendering: returns true if this value would produce
/// anything other than a missing value (JSON null). Does not render anything.
bool shreddedValueIsPresent(const ShreddedValueColumns & columns, size_t row)
{
    if (columns.value && !columns.value->isNullAt(row))
        return true;
    if (!columns.typed_value)
        return false;
    TypeIndex kind = columns.typed_value_type->getTypeId();
    if (kind == TypeIndex::Tuple)
    {
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*columns.typed_value_type);
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns.typed_value);
        for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
        {
            ShreddedValueColumns field = getVariantNodeColumns(*tuple_type.getElement(i), tuple_column.getColumn(i));
            if (shreddedValueIsPresent(field, row))
                return true;
        }
        return false;
    }
    if (kind == TypeIndex::Array)
        /// A shredded array typed_value always represents the value (possibly an empty array).
        return true;
    return !columns.typed_value->isNullAt(row);
}

/// Renders a value known to be present (see shreddedValueIsPresent).
void shreddedValueToJSONImpl(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    WriteBuffer & out,
    const FormatSettings & settings,
    size_t depth,
    bool top_level,
    SerializationMemo * memo)
{
    checkDepth(depth, settings);

    bool have_value = columns.value && !columns.value->isNullAt(row);
    std::string_view value_bytes = have_value ? getStringAt(*columns.value, row) : std::string_view();

    if (columns.typed_value)
    {
        TypeIndex kind = columns.typed_value_type->getTypeId();

        /// Shredded object: typed_value is a group whose fields are variant nodes.
        if (kind == TypeIndex::Tuple)
        {
            /// Per spec, value and typed_value are both non-null only for partially shredded objects.
            /// If value is present but not an object, the data is invalid; prefer the value.
            if (have_value && !variantValueIsObject(value_bytes))
            {
                variantValueToJSONImpl(value_bytes, metadata, out, settings, depth + 1);
                return;
            }

            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*columns.typed_value_type);
            const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns.typed_value);

            out.write('{');
            bool first = true;
            for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
            {
                ShreddedValueColumns field = getVariantNodeColumns(*tuple_type.getElement(i), tuple_column.getColumn(i));
                if (!shreddedValueIsPresent(field, row))
                    continue;
                if (!first)
                    out.write(',');
                first = false;
                writeJSONString(std::string_view(tuple_type.getElementNames()[i]), out, settings);
                out.write(':');
                shreddedValueToJSONImpl(field, row, metadata, out, settings, depth + 1, false, memo);
            }

            /// Merge unshredded fields from the value object (partially shredded object).
            if (have_value)
            {
                parseVariantObject(value_bytes, metadata, [&](std::string_view key, std::string_view field_value)
                {
                    if (tuple_type.tryGetPositionByName(key).has_value())
                        return; /// Shredded field; already written from typed_value.
                    if (!first)
                        out.write(',');
                    first = false;
                    writeJSONString(key, out, settings);
                    out.write(':');
                    variantValueToJSONImpl(field_value, metadata, out, settings, depth + 1);
                });
            }
            out.write('}');
            return;
        }

        /// Shredded array: typed_value is a 3-level list of variant-node elements.
        if (kind == TypeIndex::Array)
        {
            /// If value is present, the variant is not an array (a null typed_value list is
            /// normalized to an empty array by the reader, so trust the value first).
            if (have_value)
            {
                variantValueToJSONImpl(value_bytes, metadata, out, settings, depth + 1);
                return;
            }

            const auto & array_type = assert_cast<const DataTypeArray &>(*columns.typed_value_type);
            const auto & array_column = assert_cast<const ColumnArray &>(*columns.typed_value);
            const auto & offsets = array_column.getOffsets();
            size_t begin = row > 0 ? offsets[row - 1] : 0;
            size_t end = offsets[row];

            ShreddedValueColumns element = getVariantNodeColumns(*array_type.getNestedType(), array_column.getData());

            out.write('[');
            for (size_t i = begin; i < end; ++i)
            {
                if (i != begin)
                    out.write(',');
                if (shreddedValueIsPresent(element, i))
                    shreddedValueToJSONImpl(element, i, metadata, out, settings, depth + 1, false, memo);
                else
                    /// Missing array elements are invalid per spec; emit null.
                    out.write("null", 4);
            }
            out.write(']');
            return;
        }

        /// Shredded primitive.
        if (!columns.typed_value->isNullAt(row))
        {
            const IDataType * plain_type = columns.typed_value_type;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(plain_type))
                plain_type = nullable_type->getNestedType().get();

            /// DuckDB writes variants with a top-level `typed_value (String)` holding the JSON
            /// document text of the whole variant (rather than a spec-compliant shredding, where a
            /// String typed_value means the value is that string). If the string is a complete JSON
            /// document, embed it verbatim. Only done at the top level, where DuckDB puts it.
            /// The common case is settled by a cheap bracket sniff; the full JSON parser only runs
            /// over scalars and suspicious strings.
            if (top_level && plain_type->getTypeId() == TypeIndex::String)
            {
                std::string_view text = getStringAt(*columns.typed_value, row);
                if (looksLikeJSONDocument(text) || JSONDocumentValidator::validate(text))
                {
                    out.write(text.data(), text.size());
                    return;
                }
            }

            SerializationPtr serialization;
            if (memo)
            {
                auto & cached = (*memo)[columns.typed_value_type];
                if (!cached)
                    cached = columns.typed_value_type->getDefaultSerialization();
                serialization = cached;
            }
            else
            {
                serialization = columns.typed_value_type->getDefaultSerialization();
            }
            serialization->serializeTextJSON(*columns.typed_value, row, out, settings);
            return;
        }
    }

    if (have_value)
    {
        variantValueToJSONImpl(value_bytes, metadata, out, settings, depth + 1);
        return;
    }

    /// Unreachable when the caller prechecks presence; keep the output valid anyway.
    out.write("null", 4);
}

}

void parseVariantMetadata(std::string_view data, VariantMetadata & out)
{
    if (data.size() < 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant metadata: {} bytes", data.size());

    uint8_t header = static_cast<uint8_t>(data[0]);
    uint8_t version = header & 0x0F;
    if (version != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported variant metadata version {}", version);
    size_t offset_size = ((header >> 6) & 3) + 1;

    const char * p = data.data() + 1;
    const char * end = data.data() + data.size();
    if (size_t(end - p) < offset_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant metadata: truncated dictionary size");
    uint32_t dictionary_size = readUnsignedLittleEndian(p, offset_size);
    p += offset_size;

    if (size_t(end - p) < (size_t(dictionary_size) + 1) * offset_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant metadata: truncated offset list");
    const char * offsets = p;
    const char * bytes = offsets + (size_t(dictionary_size) + 1) * offset_size;
    size_t bytes_size = size_t(end - bytes);

    out.dictionary.clear();
    out.dictionary.reserve(dictionary_size);
    for (uint32_t i = 0; i < dictionary_size; ++i)
    {
        uint32_t offset = readUnsignedLittleEndian(offsets + size_t(i) * offset_size, offset_size);
        uint32_t next_offset = readUnsignedLittleEndian(offsets + (size_t(i) + 1) * offset_size, offset_size);
        if (offset > next_offset || next_offset > bytes_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant metadata: string offsets {}..{} out of {} bytes", offset, next_offset, bytes_size);
        out.dictionary.emplace_back(bytes + offset, next_offset - offset);
    }
    out.raw = data;
}

void variantValueToJSON(std::string_view value, const VariantMetadata & metadata, WriteBuffer & out, const FormatSettings & settings)
{
    variantValueToJSONImpl(value, metadata, out, settings, 1);
}

bool shreddedValueToJSON(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    WriteBuffer & out,
    const FormatSettings & settings,
    SerializationMemo * memo)
{
    if (!shreddedValueIsPresent(columns, row))
    {
        out.write("null", 4);
        return false;
    }
    shreddedValueToJSONImpl(columns, row, metadata, out, settings, 1, true, memo);
    return true;
}

namespace
{

DataTypePtr stringType()
{
    static DataTypePtr type = std::make_shared<DataTypeString>();
    return type;
}

DataTypePtr nullableStringType()
{
    static DataTypePtr type = makeNullable(std::make_shared<DataTypeString>());
    return type;
}

/// If all elements are tuples (no nulls), unify them into a single tuple type with the union of
/// names (defaults for missing fields), mirroring CH JSON inference for arrays of objects.
/// Returns false when unification is not applicable.
bool unifyTupleArray(std::vector<DecodedVariantValue> & elements, DecodedVariantValue & out)
{
    if (elements.empty())
        return false;
    for (const auto & element : elements)
    {
        if (!element.type || element.type->getTypeId() != TypeIndex::Tuple)
            return false;
    }

    /// Sorted union of field names.
    std::set<String> name_set;
    for (const auto & element : elements)
        for (const auto & name : assert_cast<const DataTypeTuple &>(*element.type).getElementNames())
            name_set.insert(name);
    Strings union_names(name_set.begin(), name_set.end());

    /// Per name: per-element (type, field) or missing.
    std::vector<Tuple> tuples_per_element(elements.size());
    DataTypes name_types;
    for (const auto & name : union_names)
    {
        std::vector<std::pair<DataTypePtr, const Field *>> per_element(elements.size());
        std::vector<size_t> present;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*elements[i].type);
            const auto & tuple_fields = elements[i].field.safeGet<Tuple>();
            std::optional<size_t> pos = tuple_type.tryGetPositionByName(name);
            if (pos.has_value())
            {
                per_element[i] = {tuple_type.getElement(*pos), &tuple_fields[*pos]};
                present.push_back(i);
            }
        }

        /// All present values must have the same type to unify; otherwise the name is Dynamic.
        DataTypePtr common;
        bool same = !present.empty();
        if (same)
        {
            common = per_element[present[0]].first;
            for (size_t i : present)
                if (!per_element[i].first->equals(*common))
                    same = false;
        }

        if (!same)
        {
            name_types.push_back(std::make_shared<DataTypeDynamic>());
            for (size_t i = 0; i < elements.size(); ++i)
                tuples_per_element[i].push_back(per_element[i].first ? *per_element[i].second : Field{});
        }
        else
        {
            name_types.push_back(common);
            Field default_value = common->getDefault();
            for (size_t i = 0; i < elements.size(); ++i)
                tuples_per_element[i].push_back(per_element[i].first ? *per_element[i].second : default_value);
        }
    }

    out.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(name_types, union_names));
    Array array;
    array.reserve(elements.size());
    for (auto & tuple : tuples_per_element)
        array.emplace_back(std::move(tuple));
    out.field = Field(std::move(array));
    return true;
}

/// Compute the element type of a decoded array, mirroring Dynamic JSON inference: a common
/// concrete type when all elements agree (Nullable-wrapped if there are nulls), Dynamic otherwise.
/// Arrays of objects are unified (see unifyTupleArray).
DecodedVariantValue makeArrayValue(std::vector<DecodedVariantValue> & elements)
{
    DecodedVariantValue unified;
    if (unifyTupleArray(elements, unified))
        return unified;

    DataTypePtr common;
    bool has_null = false;
    bool mixed = false;
    for (const auto & element : elements)
    {
        if (!element.type)
        {
            has_null = true;
            continue;
        }
        if (!common)
            common = element.type;
        else if (!common->equals(*element.type))
        {
            mixed = true;
            break;
        }
    }

    DataTypePtr nested;
    if (mixed || !common)
        nested = std::make_shared<DataTypeDynamic>();
    else if (has_null && common->canBeInsideNullable())
        nested = makeNullable(common);
    else if (has_null)
        nested = std::make_shared<DataTypeDynamic>();
    else
        nested = common;

    DecodedVariantValue res;
    res.type = std::make_shared<DataTypeArray>(nested);
    Array array;
    array.reserve(elements.size());
    for (const auto & element : elements)
        array.push_back(element.field);
    res.field = Field(std::move(array));
    res.array_value.emplace(nested, std::move(elements));
    return res;
}

DecodedVariantValue variantPrimitiveToDecoded(uint8_t primitive_type, std::string_view data)
{
    switch (primitive_type)
    {
        case VARIANT_NULL:
            return {};
        case VARIANT_TRUE:
        case VARIANT_FALSE:
        {
            static DataTypePtr bool_type = DataTypeFactory::instance().get("Bool");
            return {bool_type, Field(UInt64(primitive_type == VARIANT_TRUE))};
        }
        case VARIANT_INT8:
        case VARIANT_INT16:
        case VARIANT_INT32:
        case VARIANT_INT64:
        {
            size_t size = 1UZ << (primitive_type - VARIANT_INT8);
            if (data.size() < size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant integer value: {} bytes for int{}", data.size(), size * 8);
            Int64 v;
            switch (size)
            {
                case 1: v = Int64(static_cast<Int8>(data[0])); break;
                case 2: v = Int64(readLittleEndian<Int16>(data.data())); break;
                case 4: v = Int64(readLittleEndian<Int32>(data.data())); break;
                default: v = readLittleEndian<Int64>(data.data()); break;
            }
            static DataTypePtr type = std::make_shared<DataTypeInt64>();
            return {type, Field(v)};
        }
        case VARIANT_DOUBLE:
        {
            if (data.size() < 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant double value: {} bytes", data.size());
            static DataTypePtr type = std::make_shared<DataTypeFloat64>();
            return {type, Field(readLittleEndian<double>(data.data()))};
        }
        case VARIANT_DECIMAL4:
        case VARIANT_DECIMAL8:
        case VARIANT_DECIMAL16:
        {
            size_t size = primitive_type == VARIANT_DECIMAL4 ? 4 : (primitive_type == VARIANT_DECIMAL8 ? 8 : 16);
            if (data.size() < 1 + size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant decimal value: {} bytes", data.size());
            UInt32 scale = static_cast<uint8_t>(data[0]);
            if (scale > 38)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant decimal value: scale {}", scale);
            Float64 unscaled;
            if (size == 4)
                unscaled = static_cast<Float64>(readLittleEndian<Int32>(data.data() + 1));
            else if (size == 8)
                unscaled = static_cast<Float64>(readLittleEndian<Int64>(data.data() + 1));
            else
                unscaled = static_cast<Float64>(readLittleEndian<Int128>(data.data() + 1));
            static DataTypePtr type = std::make_shared<DataTypeFloat64>();
            return {type, Field(unscaled / std::pow(10.0, static_cast<int>(scale)))};
        }
        case VARIANT_DATE:
        {
            if (data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant date value: {} bytes", data.size());
            Int32 days = readLittleEndian<Int32>(data.data());
            WriteBufferFromOwnString buf;
            writeDateText(ExtendedDayNum(days), buf, DateLUT::instance("UTC"));
            return {stringType(), Field(buf.str())};
        }
        case VARIANT_TIMESTAMP_TZ_MICROS:
        case VARIANT_TIMESTAMP_NTZ_MICROS:
        case VARIANT_TIMESTAMP_TZ_NANOS:
        case VARIANT_TIMESTAMP_NTZ_NANOS:
        {
            if (data.size() < 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant timestamp value: {} bytes", data.size());
            Int64 v = readLittleEndian<Int64>(data.data());
            UInt32 scale = (primitive_type == VARIANT_TIMESTAMP_TZ_MICROS || primitive_type == VARIANT_TIMESTAMP_NTZ_MICROS) ? 6 : 9;
            auto type = std::make_shared<DataTypeDateTime64>(scale, TimezoneMixin{"UTC"});
            return {type, Field(DecimalField<DateTime64>(DateTime64(v), scale))};
        }
        case VARIANT_FLOAT:
        {
            if (data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant float value: {} bytes", data.size());
            static DataTypePtr type = std::make_shared<DataTypeFloat64>();
            return {type, Field(static_cast<Float64>(readLittleEndian<float>(data.data())))};
        }
        case VARIANT_BINARY:
        case VARIANT_STRING:
        {
            if (data.size() < 4)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant string value: {} bytes", data.size());
            uint32_t size = readLittleEndian<uint32_t>(data.data());
            if (data.size() < 4 + size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant string value: size {} exceeds available {} bytes", size, data.size() - 4);
            return {stringType(), Field(String(data.data() + 4, size))};
        }
        case VARIANT_TIME_NTZ_MICROS:
        {
            if (data.size() < 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant time value: {} bytes", data.size());
            Int64 micros = readLittleEndian<Int64>(data.data());
            if (micros < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant time value: {} microseconds", micros);
            UInt64 hour = UInt64(micros) / 3600000000;
            UInt64 minute = UInt64(micros) / 60000000 % 60;
            UInt64 second = UInt64(micros) / 1000000 % 60;
            UInt64 fraction = UInt64(micros) % 1000000;
            return {stringType(), Field(fmt::format("{:02}:{:02}:{:02}.{:06}", hour, minute, second, fraction))};
        }
        case VARIANT_UUID:
        {
            if (data.size() < 16)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant UUID value: {} bytes", data.size());
            constexpr char hex[] = "0123456789abcdef";
            char buf[36];
            size_t j = 0;
            for (size_t i = 0; i < 16; ++i)
            {
                if (i == 4 || i == 6 || i == 8 || i == 10)
                    buf[j++] = '-';
                uint8_t b = static_cast<uint8_t>(data[i]);
                buf[j++] = hex[b >> 4];
                buf[j++] = hex[b & 0x0F];
            }
            return {stringType(), Field(String(buf, sizeof(buf)))};
        }
        default:
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown variant primitive type id {}", primitive_type);
    }
}

DecodedVariantValue variantValueToDecodedImpl(std::string_view value, const VariantMetadata & metadata, size_t depth, const FormatSettings & settings)
{
    checkDepth(depth, settings);
    if (value.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty variant value");

    uint8_t header = static_cast<uint8_t>(value[0]);
    uint8_t basic_type = header & 3;
    switch (basic_type)
    {
        case VARIANT_BASIC_PRIMITIVE:
            return variantPrimitiveToDecoded(header >> 2, value.substr(1));
        case VARIANT_BASIC_SHORT_STRING:
        {
            size_t size = header >> 2;
            if (value.size() < 1 + size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant short string: size {} exceeds available {} bytes", size, value.size() - 1);
            return {stringType(), Field(String(value.data() + 1, size))};
        }
        case VARIANT_BASIC_OBJECT:
        {
            DataTypes element_types;
            Strings element_names;
            Tuple tuple;
            parseVariantObject(value, metadata, [&](std::string_view key, std::string_view field_value)
            {
                DecodedVariantValue decoded = variantValueToDecodedImpl(field_value, metadata, depth + 1, settings);
                element_types.push_back(decoded.type ? decoded.type : nullableStringType());
                element_names.emplace_back(key);
                tuple.push_back(std::move(decoded.field));
            });
            /// Well-formed files sort fields lexicographically; sort defensively for malformed ones.
            if (!std::is_sorted(element_names.begin(), element_names.end()))
            {
                std::vector<size_t> order(element_names.size());
                std::iota(order.begin(), order.end(), 0);
                std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return element_names[a] < element_names[b]; });
                DataTypes sorted_types;
                Strings sorted_names;
                Tuple sorted_tuple;
                for (size_t i : order)
                {
                    sorted_types.push_back(element_types[i]);
                    sorted_names.push_back(element_names[i]);
                    sorted_tuple.push_back(tuple[i]);
                }
                element_types = std::move(sorted_types);
                element_names = std::move(sorted_names);
                tuple = std::move(sorted_tuple);
            }
            return {std::make_shared<DataTypeTuple>(element_types, element_names), Field(std::move(tuple))};
        }
        case VARIANT_BASIC_ARRAY:
        {
            uint8_t value_header = header >> 2;
            bool is_large = (value_header >> 2) & 1;
            size_t field_offset_size = (value_header & 3) + 1;

            const char * p = value.data() + 1;
            const char * end = value.data() + value.size();
            size_t num_elements_size = is_large ? 4 : 1;
            if (size_t(end - p) < num_elements_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant array: truncated element count");
            uint32_t num_elements = readUnsignedLittleEndian(p, num_elements_size);
            p += num_elements_size;
            if (size_t(end - p) < (size_t(num_elements) + 1) * field_offset_size)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant array: truncated offset list");
            const char * offsets = p;
            const char * elements = offsets + (size_t(num_elements) + 1) * field_offset_size;
            size_t elements_size = size_t(end - elements);

            Array array;
            std::vector<DecodedVariantValue> decoded_elements;
            decoded_elements.reserve(num_elements);
            for (uint32_t i = 0; i < num_elements; ++i)
            {
                uint32_t offset = readUnsignedLittleEndian(offsets + size_t(i) * field_offset_size, field_offset_size);
                uint32_t next_offset = readUnsignedLittleEndian(offsets + (size_t(i) + 1) * field_offset_size, field_offset_size);
                if (offset > next_offset || next_offset > elements_size)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant array: element offsets {}..{} out of {} bytes", offset, next_offset, elements_size);
                decoded_elements.push_back(variantValueToDecodedImpl(std::string_view(elements + offset, next_offset - offset), metadata, depth + 1, settings));
            }
            return makeArrayValue(decoded_elements);
        }
    }
    __builtin_unreachable();
}

/// Lean recursive-descent JSON parser producing typed values for DuckDB's top-level
/// `typed_value (String)` JSON documents. Mirrors Dynamic JSON inference at a fraction of the
/// cost of the generic deserializer: integers that fit Int64, Float64 for other numbers,
/// String (no date/datetime sniffing), Bool, Null, homogeneous or Dynamic arrays, named Tuples.
class LeanJSONParser
{
public:
    LeanJSONParser(std::string_view text_, const FormatSettings & settings_) : text(text_), settings(settings_) {}

    DecodedVariantValue parse()
    {
        skipWhitespace();
        DecodedVariantValue res = parseValue(0);
        skipWhitespace();
        if (pos != text.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed JSON document: trailing characters at offset {}", pos);
        return res;
    }

private:

    std::string_view text;
    const FormatSettings & settings;
    size_t pos = 0;

    char peek() const { return pos < text.size() ? text[pos] : '\0'; }
    void skipWhitespace()
    {
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n' || text[pos] == '\r'))
            ++pos;
    }
    bool consume(char c)
    {
        if (peek() != c)
            return false;
        ++pos;
        return true;
    }

    DataTypePtr stringType() const
    {
        static DataTypePtr type = std::make_shared<DataTypeString>();
        return type;
    }

    DecodedVariantValue parseValue(size_t depth)
    {
        checkDepth(depth, settings);
        switch (peek())
        {
            case '{': return parseObject(depth);
            case '[': return parseArray(depth);
            case '"': return {stringType(), Field(parseString())};
            case 't':
                expectLiteral("true");
                static DataTypePtr bool_type = DataTypeFactory::instance().get("Bool");
                return {bool_type, Field(UInt64(1))};
            case 'f':
                expectLiteral("false");
                static DataTypePtr bool_type_f = DataTypeFactory::instance().get("Bool");
                return {bool_type_f, Field(UInt64(0))};
            case 'n':
                expectLiteral("null");
                return {};
            default:
                return parseNumber();
        }
    }

    [[noreturn]] void throwMalformed(std::string_view what)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed JSON document: expected {} at offset {}", what, pos);
    }

    void expectLiteral(std::string_view lit)
    {
        if (text.substr(pos, lit.size()) != lit)
            throwMalformed("valid literal");
        pos += lit.size();
    }

    String parseString()
    {
        if (!consume('"'))
            throwMalformed("string");
        String res;
        while (true)
        {
            if (pos >= text.size())
                throwMalformed("closing quote");
            char c = text[pos++];
            if (c == '"')
                return res;
            if (c == '\\')
            {
                if (pos >= text.size())
                    throwMalformed("escape sequence");
                char e = text[pos++];
                switch (e)
                {
                    case '"': res.push_back('"'); break;
                    case '\\': res.push_back('\\'); break;
                    case '/': res.push_back('/'); break;
                    case 'b': res.push_back('\b'); break;
                    case 'f': res.push_back('\f'); break;
                    case 'n': res.push_back('\n'); break;
                    case 'r': res.push_back('\r'); break;
                    case 't': res.push_back('\t'); break;
                    case 'u':
                    {
                        if (pos + 4 > text.size())
                            throwMalformed("unicode escape");
                        UInt32 codepoint = 0;
                        for (int i = 0; i < 4; ++i)
                        {
                            char h = text[pos++];
                            codepoint <<= 4;
                            if (h >= '0' && h <= '9') codepoint |= UInt32(h - '0');
                            else if (h >= 'a' && h <= 'f') codepoint |= UInt32(h - 'a' + 10);
                            else if (h >= 'A' && h <= 'F') codepoint |= UInt32(h - 'A' + 10);
                            else throwMalformed("hex digit");
                        }
                        /// Surrogate pair.
                        if (codepoint >= 0xD800 && codepoint <= 0xDBFF)
                        {
                            if (pos + 6 > text.size() || text[pos] != '\\' || text[pos + 1] != 'u')
                                throwMalformed("low surrogate");
                            pos += 2;
                            UInt32 low = 0;
                            for (int i = 0; i < 4; ++i)
                            {
                                char h = text[pos++];
                                low <<= 4;
                                if (h >= '0' && h <= '9') low |= UInt32(h - '0');
                                else if (h >= 'a' && h <= 'f') low |= UInt32(h - 'a' + 10);
                                else if (h >= 'A' && h <= 'F') low |= UInt32(h - 'A' + 10);
                                else throwMalformed("hex digit");
                            }
                            codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + (low - 0xDC00);
                        }
                        /// Encode UTF-8.
                        if (codepoint < 0x80)
                            res.push_back(static_cast<char>(codepoint));
                        else if (codepoint < 0x800)
                        {
                            res.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
                            res.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
                        }
                        else if (codepoint < 0x10000)
                        {
                            res.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
                            res.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
                            res.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
                        }
                        else
                        {
                            res.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
                            res.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
                            res.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
                            res.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
                        }
                        break;
                    }
                    default:
                        throwMalformed("valid escape");
                }
            }
            else
                res.push_back(c);
        }
    }

    DecodedVariantValue parseNumber()
    {
        size_t start = pos;
        if (peek() == '-')
            ++pos;
        bool is_float = false;
        while (pos < text.size() && ((text[pos] >= '0' && text[pos] <= '9') || text[pos] == '.' || text[pos] == 'e' || text[pos] == 'E' || text[pos] == '+' || text[pos] == '-'))
        {
            if (text[pos] != '-' && text[pos] != '+')
                is_float = is_float || text[pos] == '.' || text[pos] == 'e' || text[pos] == 'E';
            ++pos;
        }
        if (pos == start || (pos == start + 1 && text[start] == '-'))
            throwMalformed("value");
        std::string_view number = text.substr(start, pos - start);
        if (!is_float)
        {
            Int64 v;
            if (std::from_chars(number.data(), number.data() + number.size(), v).ec == std::errc{})
            {
                static DataTypePtr type = std::make_shared<DataTypeInt64>();
                return {type, Field(v)};
            }
        }
        Float64 v;
        auto parse_result = std::from_chars(number.data(), number.data() + number.size(), v);
        if (parse_result.ec != std::errc{})
            throwMalformed("number");
        static DataTypePtr type = std::make_shared<DataTypeFloat64>();
        return {type, Field(v)};
    }

    DecodedVariantValue parseArray(size_t depth)
    {
        consume('[');
        std::vector<DecodedVariantValue> elements;
        skipWhitespace();
        if (consume(']'))
            return makeArrayValue(elements);
        while (true)
        {
            skipWhitespace();
            elements.push_back(parseValue(depth + 1));
            skipWhitespace();
            if (consume(']'))
                break;
            if (!consume(','))
                throwMalformed("',' or ']'");
        }
        return makeArrayValue(elements);
    }

    DecodedVariantValue parseObject(size_t depth)
    {
        consume('{');
        DataTypes element_types;
        Strings element_names;
        Tuple tuple;
        skipWhitespace();
        if (!consume('}'))
        {
            while (true)
            {
                skipWhitespace();
                String key = parseString();
                skipWhitespace();
                if (!consume(':'))
                    throwMalformed("':'");
                skipWhitespace();
                DecodedVariantValue decoded = parseValue(depth + 1);
                element_types.push_back(decoded.type ? decoded.type : nullableStringType());
                element_names.push_back(std::move(key));
                tuple.push_back(std::move(decoded.field));
                skipWhitespace();
                if (consume('}'))
                    break;
                if (!consume(','))
                    throwMalformed("',' or '}'");
            }
        }
        /// CH JSON semantics: paths are sorted lexicographically.
        if (!std::is_sorted(element_names.begin(), element_names.end()))
        {
            std::vector<size_t> order(element_names.size());
            std::iota(order.begin(), order.end(), 0);
            std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return element_names[a] < element_names[b]; });
            DataTypes sorted_types;
            Strings sorted_names;
            Tuple sorted_tuple;
            for (size_t i : order)
            {
                sorted_types.push_back(element_types[i]);
                sorted_names.push_back(element_names[i]);
                sorted_tuple.push_back(tuple[i]);
            }
            element_types = std::move(sorted_types);
            element_names = std::move(sorted_names);
            tuple = std::move(sorted_tuple);
        }
        return {std::make_shared<DataTypeTuple>(element_types, element_names), Field(std::move(tuple))};
    }
};


DecodedVariantValue shreddedValueToDecodedImpl(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    const FormatSettings & settings,
    size_t depth,
    bool top_level)
{
    checkDepth(depth, settings);

    bool have_value = columns.value && !columns.value->isNullAt(row);
    std::string_view value_bytes = have_value ? getStringAt(*columns.value, row) : std::string_view();

    if (columns.typed_value)
    {
        TypeIndex kind = columns.typed_value_type->getTypeId();

        if (kind == TypeIndex::Tuple)
        {
            if (have_value && !variantValueIsObject(value_bytes))
                return variantValueToDecodedImpl(value_bytes, metadata, depth + 1, settings);

            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*columns.typed_value_type);
            const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns.typed_value);

            std::vector<std::pair<String, DecodedVariantValue>> merged;
            for (size_t i = 0; i < tuple_type.getElements().size(); ++i)
            {
                ShreddedValueColumns field = getVariantNodeColumns(*tuple_type.getElement(i), tuple_column.getColumn(i));
                if (!shreddedValueIsPresent(field, row))
                    continue;
                merged.emplace_back(
                    tuple_type.getElementNames()[i],
                    shreddedValueToDecodedImpl(field, row, metadata, settings, depth + 1, false));
            }

            if (have_value)
            {
                parseVariantObject(value_bytes, metadata, [&](std::string_view key, std::string_view field_value)
                {
                    if (tuple_type.tryGetPositionByName(key).has_value())
                        return;
                    merged.emplace_back(key, variantValueToDecodedImpl(field_value, metadata, depth + 1, settings));
                });
            }

            std::sort(merged.begin(), merged.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
            DataTypes element_types;
            Strings element_names;
            Tuple tuple;
            element_types.reserve(merged.size());
            element_names.reserve(merged.size());
            tuple.reserve(merged.size());
            for (auto & [name, decoded] : merged)
            {
                element_types.push_back(decoded.type ? decoded.type : nullableStringType());
                element_names.push_back(std::move(name));
                tuple.push_back(std::move(decoded.field));
            }
            return {std::make_shared<DataTypeTuple>(element_types, element_names), Field(std::move(tuple))};
        }

        if (kind == TypeIndex::Array)
        {
            if (have_value)
                return variantValueToDecodedImpl(value_bytes, metadata, depth + 1, settings);

            const auto & array_column = assert_cast<const ColumnArray &>(*columns.typed_value);
            const auto & offsets = array_column.getOffsets();
            size_t begin = row > 0 ? offsets[row - 1] : 0;
            size_t end = offsets[row];

            ShreddedValueColumns element = getVariantNodeColumns(
                *assert_cast<const DataTypeArray &>(*columns.typed_value_type).getNestedType(), array_column.getData());

            std::vector<DecodedVariantValue> decoded_elements;
            decoded_elements.reserve(end - begin);
            for (size_t i = begin; i < end; ++i)
            {
                if (shreddedValueIsPresent(element, i))
                    decoded_elements.push_back(shreddedValueToDecodedImpl(element, i, metadata, settings, depth + 1, false));
                else
                    decoded_elements.emplace_back();
            }
            return makeArrayValue(decoded_elements);
        }

        /// Shredded primitive.
        if (!columns.typed_value->isNullAt(row))
        {
            const IDataType * plain_type = columns.typed_value_type;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(plain_type))
                plain_type = nullable_type->getNestedType().get();

            /// DuckDB's top-level `typed_value (String)` holds a JSON document; parse it with a
            /// lean recursive-descent parser (see LeanJSONParser), avoiding the generic Dynamic
            /// JSON deserializer and its inference machinery.
            if (top_level && plain_type->getTypeId() == TypeIndex::String)
            {
                std::string_view text = getStringAt(*columns.typed_value, row);
                if (text == "null")
                    return {};
                if (looksLikeJSONDocument(text) || JSONDocumentValidator::validate(text))
                    return LeanJSONParser(text, settings).parse();
            }

            DecodedVariantValue res;
            res.type = plain_type->getPtr();
            res.field = (*columns.typed_value)[row];
            return res;
        }
    }

    if (have_value)
        return variantValueToDecodedImpl(value_bytes, metadata, depth + 1, settings);

    return {};
}

/// Find the encoded bytes of the field with id `target_id` in a variant-encoded object.
/// Field ids are sorted per spec, so scan with early exit; if a writer violates the sort
/// order, fall back to a full scan. Returns false if the field is absent.
bool findVariantObjectFieldById(std::string_view value, uint32_t target_id, std::string_view & out)
{
    if (value.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Empty variant value");

    uint8_t header = static_cast<uint8_t>(value[0]);
    uint8_t value_header = header >> 2;
    bool is_large = (value_header >> 4) & 1;
    size_t field_id_size = ((value_header >> 2) & 3) + 1;
    size_t field_offset_size = (value_header & 3) + 1;

    const char * p = value.data() + 1;
    const char * end = value.data() + value.size();

    size_t num_elements_size = is_large ? 4 : 1;
    if (size_t(end - p) < num_elements_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: truncated element count");
    uint32_t num_elements = readUnsignedLittleEndian(p, num_elements_size);
    p += num_elements_size;

    if (size_t(end - p) < size_t(num_elements) * field_id_size + (size_t(num_elements) + 1) * field_offset_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: truncated field id/offset lists");

    const char * ids = p;
    const char * offsets = p + size_t(num_elements) * field_id_size;
    const char * fields = offsets + (size_t(num_elements) + 1) * field_offset_size;
    size_t fields_size = size_t(end - fields);

    bool sorted = true;
    uint32_t prev_id = 0;
    for (uint32_t i = 0; i < num_elements; ++i)
    {
        uint32_t field_id = readUnsignedLittleEndian(ids + size_t(i) * field_id_size, field_id_size);
        if (i > 0 && field_id < prev_id)
            sorted = false;
        prev_id = field_id;
        if (field_id != target_id)
        {
            if (sorted && field_id > target_id)
                return false;
            continue;
        }
        uint32_t offset = readUnsignedLittleEndian(offsets + size_t(i) * field_offset_size, field_offset_size);
        uint32_t next_offset = readUnsignedLittleEndian(offsets + (size_t(i) + 1) * field_offset_size, field_offset_size);
        if (offset > next_offset || next_offset > fields_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed variant object: field offsets {}..{} out of {} bytes", offset, next_offset, fields_size);
        out = std::string_view(fields + offset, next_offset - offset);
        return true;
    }
    return false;
}

/// Navigate `path` (from segment `seg` on) inside a variant-encoded value.
DecodedVariantValue variantValueExtractPathImpl(
    std::string_view value,
    const VariantMetadata & metadata,
    const VariantExtractPath & path,
    size_t seg,
    const FormatSettings & settings,
    size_t depth)
{
    checkDepth(depth, settings);
    if (seg == path.names.size())
        return variantValueToDecodedImpl(value, metadata, depth, settings);

    /// The name is not in the metadata dictionary, so no variant-encoded binary contains it.
    if (!path.ids[seg].has_value())
        return {};
    if (!variantValueIsObject(value))
        return {};

    std::string_view field_bytes;
    if (!findVariantObjectFieldById(value, *path.ids[seg], field_bytes))
        return {};
    return variantValueExtractPathImpl(field_bytes, metadata, path, seg + 1, settings, depth + 1);
}

/// Navigate `path` (from segment `seg` on) inside an already-decoded value (used for DuckDB's
/// JSON-document typed_value, which must be parsed whole before a path can be taken from it).
DecodedVariantValue extractFromDecoded(const DecodedVariantValue & value, const VariantExtractPath & path, size_t seg)
{
    if (seg == path.names.size())
    {
        /// A null sub-value (e.g. JSON null in a DuckDB document) is a Dynamic null.
        if (!value.type || value.field.isNull())
            return {};
        return value;
    }
    if (!value.type || value.type->getTypeId() != TypeIndex::Tuple)
        return {};
    const auto & tuple_type = assert_cast<const DataTypeTuple &>(*value.type);
    std::optional<size_t> pos = tuple_type.tryGetPositionByName(path.names[seg]);
    if (!pos.has_value())
        return {};
    DecodedVariantValue sub;
    sub.type = tuple_type.getElement(*pos);
    sub.field = value.field.safeGet<Tuple>()[*pos];
    return extractFromDecoded(sub, path, seg + 1);
}

/// Navigate `path` (from segment `seg` on) inside a (possibly shredded) Variant value.
DecodedVariantValue shreddedValueExtractPathImpl(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    const VariantExtractPath & path,
    size_t seg,
    const FormatSettings & settings,
    size_t depth,
    bool top_level)
{
    checkDepth(depth, settings);
    if (seg == path.names.size())
    {
        if (!shreddedValueIsPresent(columns, row))
            return {};
        return shreddedValueToDecodedImpl(columns, row, metadata, settings, depth, top_level);
    }

    bool have_value = columns.value && !columns.value->isNullAt(row);
    std::string_view value_bytes = have_value ? getStringAt(*columns.value, row) : std::string_view();

    if (columns.typed_value)
    {
        TypeIndex kind = columns.typed_value_type->getTypeId();

        if (kind == TypeIndex::Tuple)
        {
            /// The value (if any) wins over typed_value, but a non-object value has no fields.
            if (have_value && !variantValueIsObject(value_bytes))
                return {};

            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*columns.typed_value_type);
            const auto & tuple_column = assert_cast<const ColumnTuple &>(*columns.typed_value);
            if (std::optional<size_t> pos = tuple_type.tryGetPositionByName(path.names[seg]))
            {
                /// Shredded field; the value object does not repeat it (per spec).
                ShreddedValueColumns field = getVariantNodeColumns(*tuple_type.getElement(*pos), tuple_column.getColumn(*pos));
                if (!shreddedValueIsPresent(field, row))
                    return {};
                return shreddedValueExtractPathImpl(field, row, metadata, path, seg + 1, settings, depth + 1, false);
            }
            if (have_value)
                return variantValueExtractPathImpl(value_bytes, metadata, path, seg, settings, depth + 1);
            return {};
        }

        /// Object subcolumn paths do not index into arrays (matching DataTypeObject subcolumn
        /// semantics), so a path crossing a shredded array is absent.
        if (kind == TypeIndex::Array)
            return {};

        if (!columns.typed_value->isNullAt(row))
        {
            const IDataType * plain_type = columns.typed_value_type;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(plain_type))
                plain_type = nullable_type->getNestedType().get();

            /// DuckDB's top-level `typed_value (String)` holds a JSON document: parse it whole
            /// and take the path from the parsed value.
            if (top_level && plain_type->getTypeId() == TypeIndex::String)
            {
                std::string_view text = getStringAt(*columns.typed_value, row);
                if (text == "null")
                    return {};
                if (looksLikeJSONDocument(text) || JSONDocumentValidator::validate(text))
                    return extractFromDecoded(LeanJSONParser(text, settings).parse(), path, seg);
            }
            /// A path into a scalar is absent.
            return {};
        }
    }

    if (have_value)
        return variantValueExtractPathImpl(value_bytes, metadata, path, seg, settings, depth + 1);

    return {};
}

}

DecodedVariantValue variantValueToDecoded(std::string_view value, const VariantMetadata & metadata)
{
    static const FormatSettings default_settings;
    return variantValueToDecodedImpl(value, metadata, 1, default_settings);
}

DecodedVariantValue shreddedValueToDecoded(const ShreddedValueColumns & columns, size_t row, const VariantMetadata & metadata)
{
    static const FormatSettings default_settings;
    if (!shreddedValueIsPresent(columns, row))
        return {};
    return shreddedValueToDecodedImpl(columns, row, metadata, default_settings, 1, true);
}

DecodedVariantValue shreddedValueToDecoded(const ShreddedValueColumns & columns, size_t row, const VariantMetadata & metadata, const FormatSettings & settings)
{
    if (!shreddedValueIsPresent(columns, row))
        return {};
    return shreddedValueToDecodedImpl(columns, row, metadata, settings, 1, true);
}

void resolveVariantExtractPath(VariantExtractPath & path, const VariantMetadata & metadata)
{
    path.ids.clear();
    path.ids.reserve(path.names.size());
    for (const String & name : path.names)
    {
        std::optional<uint32_t> id;
        for (uint32_t i = 0; i < metadata.dictionary.size(); ++i)
        {
            if (metadata.dictionary[i] == name)
            {
                id = i;
                break;
            }
        }
        path.ids.push_back(id);
    }
}

DecodedVariantValue shreddedValueExtractPath(
    const ShreddedValueColumns & columns,
    size_t row,
    const VariantMetadata & metadata,
    const VariantExtractPath & path,
    const FormatSettings & settings)
{
    return shreddedValueExtractPathImpl(columns, row, metadata, path, 0, settings, 1, true);
}

void insertIntoDynamicColumn(ColumnDynamic & column, const DataTypePtr & type, const Field & value)
{
    String type_name = type->getName();
    if (column.addNewVariant(type, type_name))
    {
        auto discr = column.getVariantInfo().variant_name_to_discriminator.at(type_name);
        auto & variant_column = column.getVariantColumn();
        auto & nested = variant_column.getVariantByGlobalDiscriminator(discr);
        nested.insert(value);
        variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(discr));
        variant_column.getOffsets().push_back(nested.size() - 1);
        return;
    }

    /// Variant count limit reached: store in the shared variant in binary form
    /// (mirrors SerializationDynamic::deserializeTextImpl).
    auto tmp_column = type->createColumn();
    tmp_column->insert(value);
    column.insertValueIntoSharedVariant(*tmp_column, type, type_name, 0);
}

void insertDecodedIntoDynamic(ColumnDynamic & column, const DecodedVariantValue & value)
{
    /// Arrays with a Dynamic element type are inserted element-wise so per-element types
    /// (e.g. Bool) are preserved; everything else is inserted wholesale.
    if (value.array_value && typeid_cast<const DataTypeDynamic *>(value.array_value->first.get()))
    {
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>());
        String type_name = array_type->getName();
        if (!column.addNewVariant(array_type, type_name))
        {
            insertIntoDynamicColumn(column, value.type, value.field);
            return;
        }
        auto discr = column.getVariantInfo().variant_name_to_discriminator.at(type_name);
        auto & variant_column = column.getVariantColumn();
        auto & array_column = assert_cast<ColumnArray &>(variant_column.getVariantByGlobalDiscriminator(discr));
        auto & nested_dynamic = assert_cast<ColumnDynamic &>(array_column.getData());
        for (const auto & element : value.array_value->second)
        {
            if (!element.type)
                nested_dynamic.insertDefault();
            else
                insertDecodedIntoDynamic(nested_dynamic, element);
        }
        array_column.getOffsets().push_back(nested_dynamic.size());
        variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(discr));
        variant_column.getOffsets().push_back(array_column.size() - 1);
        return;
    }
    insertIntoDynamicColumn(column, value.type, value.field);
}

}
