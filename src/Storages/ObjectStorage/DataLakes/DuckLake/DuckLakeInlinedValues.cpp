#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeInlinedValues.h>

#if USE_PARQUET

#include <Columns/ColumnNullable.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>

#include <Common/Exception.h>

#include <Poco/String.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DuckLake
{

namespace
{

String decodeByteaHex(const String & value)
{
    /// Postgres text protocol renders bytea as \x<hex>.
    if (!value.starts_with("\\x"))
        return value;
    String result;
    result.reserve((value.size() - 2) / 2);
    const auto hex_digit = [](char c) -> UInt8
    {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid hex digit in bytea value");
    };
    if ((value.size() - 2) % 2 != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bytea hex value '{}'", value);
    for (size_t i = 2; i < value.size(); i += 2)
        result.push_back(static_cast<char>((hex_digit(value[i]) << 4) | hex_digit(value[i + 1])));
    return result;
}

/// Parse text as a scalar of `type` using ClickHouse text deserialization.
Field parseScalarText(const String & raw, const DataTypePtr & type)
{
    String value = raw;
    /// Postgres float text output uses 'Infinity'/'-Infinity'/'NaN'.
    if (value == "Infinity")
        value = "inf";
    else if (value == "-Infinity")
        value = "-inf";
    else if (value == "NaN")
        value = "nan";

    try
    {
        auto column = type->createColumn();
        ReadBufferFromString buf(value);
        FormatSettings format_settings;
        type->getDefaultSerialization()->deserializeWholeText(*column, buf, format_settings);
        if (!buf.eof() || column->empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "trailing characters");
        return column->operator[](0);
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot parse DuckLake inlined value '{}' as {}",
            raw,
            type->getName());
    }
}

/// timestamptz values are serialized by DuckDB with a timezone offset suffix
/// ('2024-01-15 10:30:00+00', '...+02:00'); shift to UTC and return a DateTime64 field.
Field parseTimestampWithOffset(const String & value, const DataTypePtr & type)
{
    const auto & date_time = assert_cast<const DataTypeDateTime64 &>(*type);
    const UInt32 scale = date_time.getScale();

    size_t offset_len = 0;
    /// The offset is a [+-]HH[:MM] suffix after the time part; match it from the end so the
    /// '-' characters of the date part are not mistaken for an offset.
    const auto matches_offset = [&](size_t len)
    {
        if (value.size() <= len)
            return false;
        const size_t start = value.size() - len;
        if (value[start] != '+' && value[start] != '-')
            return false;
        for (size_t i = start + 1; i < value.size(); ++i)
        {
            const char c = value[i];
            if (!isdigit(c) && c != ':')
                return false;
        }
        return true;
    };
    if (matches_offset(6))
        offset_len = 6;
    else if (matches_offset(3))
        offset_len = 3;

    if (offset_len == 0)
        return parseScalarText(value, type);

    const String base = value.substr(0, value.size() - offset_len);
    const String offset = value.substr(value.size() - offset_len);
    int sign = offset[0] == '+' ? 1 : -1;
    int hours = 0;
    int minutes = 0;
    const auto colon = offset.find(':');
    try
    {
        if (colon == String::npos)
            hours = std::stoi(offset.substr(1));
        else
        {
            hours = std::stoi(offset.substr(1, colon - 1));
            minutes = std::stoi(offset.substr(colon + 1));
        }
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid timezone offset in DuckLake inlined value '{}'", value);
    }

    Field parsed = parseScalarText(base, type);
    const Int64 offset_ticks = sign * (Int64(hours) * 3600 + Int64(minutes) * 60) * DecimalUtils::scaleMultiplier<DateTime64>(scale);
    auto decimal = parsed.safeGet<DecimalField<DateTime64>>();
    const Int64 ticks = decimal.getValue().value - offset_ticks;
    return Field(DecimalField<DateTime64>(DateTime64(ticks), scale));
}

/// Recursive-descent parser for DuckDB's literal text syntax (used for nested values in
/// catalog backends without native struct/list/map types):
///   struct: {'name': value, ...} (named) or (value, ...) (unnamed)
///   list:   [value, ...]
///   map:    {key=value, ...} (also accepts {key: value, ...})
/// Scalars are bare or 'quoted' ('' escapes a quote) and may carry a '::type' cast suffix.
class LiteralParser
{
public:
    explicit LiteralParser(std::string_view text_)
        : text(text_)
    {
    }

    Field parse(const DataTypePtr & type)
    {
        skipWhitespace();
        Field result = parseValue(type);
        skipWhitespace();
        if (pos != text.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Trailing characters in DuckLake inlined nested value '{}'", text);
        return result;
    }

private:
    std::string_view text;
    size_t pos = 0;

    [[noreturn]] void throwMalformed(std::string_view expected) const
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Malformed DuckLake inlined nested value '{}': expected {} at offset {}",
            text,
            expected,
            pos);
    }

    void skipWhitespace()
    {
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n'))
            ++pos;
    }

    bool tryConsume(char c)
    {
        skipWhitespace();
        if (pos < text.size() && text[pos] == c)
        {
            ++pos;
            return true;
        }
        return false;
    }

    void expect(char c)
    {
        if (!tryConsume(c))
            throwMalformed(fmt::format("'{}'", c));
    }

    bool tryConsumeKeyword(std::string_view keyword)
    {
        skipWhitespace();
        if (text.substr(pos, keyword.size()) == keyword)
        {
            pos += keyword.size();
            return true;
        }
        return false;
    }

    /// Optional '::type' suffix, e.g. ::timestamp, ::decimal(10, 2).
    void skipCastSuffix()
    {
        skipWhitespace();
        if (pos + 1 >= text.size() || text[pos] != ':' || text[pos + 1] != ':')
            return;
        pos += 2;
        while (pos < text.size())
        {
            const char c = text[pos];
            if (c == '(')
            {
                size_t depth = 0;
                do
                {
                    if (text[pos] == '(')
                        ++depth;
                    else if (text[pos] == ')')
                        --depth;
                    ++pos;
                } while (pos < text.size() && depth > 0);
                return;
            }
            if (isalnum(c) || c == '_' || c == ' ')
            {
                ++pos;
                continue;
            }
            return;
        }
    }

    /// 'quoted' (with '' escapes) or a bare token terminated by , ] } ) or end of input.
    /// In map-key position a bare token is also terminated by '='.
    String parseToken(bool map_key = false)
    {
        skipWhitespace();
        if (pos >= text.size())
            throwMalformed("a value");

        if (text[pos] == '\'')
        {
            ++pos;
            String result;
            while (pos < text.size())
            {
                const char c = text[pos++];
                if (c == '\'')
                {
                    if (pos < text.size() && text[pos] == '\'')
                    {
                        result.push_back('\'');
                        ++pos;
                        continue;
                    }
                    return result;
                }
                result.push_back(c);
            }
            throwMalformed("closing quote");
        }

        const size_t start = pos;
        while (pos < text.size() && text[pos] != ',' && text[pos] != ']' && text[pos] != '}' && text[pos] != ')'
               && !(map_key && text[pos] == '='))
            ++pos;
        String result(text.substr(start, pos - start));
        /// Trim trailing whitespace of bare tokens.
        while (!result.empty() && result.back() == ' ')
            result.pop_back();
        return result;
    }

    Field parseValue(const DataTypePtr & type)
    {
        skipWhitespace();
        if (pos >= text.size())
            throwMalformed("a value");

        const WhichDataType which(type);
        if (which.isNullable())
            return parseValue(removeNullable(type));
        if (which.isTuple())
            return parseTuple(assert_cast<const DataTypeTuple &>(*type));
        if (which.isArray())
            return parseArray(assert_cast<const DataTypeArray &>(*type));
        if (which.isMap())
            return parseMap(assert_cast<const DataTypeMap &>(*type));

        if (tryConsumeKeyword("NULL"))
            return Field(Null{});

        String token = parseToken();
        skipCastSuffix();
        if (which.isUInt8() && (token == "true" || token == "false"))
            return Field(UInt64(token == "true" ? 1 : 0));
        return parseScalarText(token, type);
    }

    Field parseTuple(const DataTypeTuple & tuple)
    {
        const auto & element_types = tuple.getElements();

        /// Unnamed/positional form: (v1, v2)
        if (tryConsume('('))
        {
            Array elements;
            for (size_t i = 0; i < element_types.size(); ++i)
            {
                if (i > 0)
                    expect(',');
                elements.push_back(parseValue(element_types[i]));
            }
            expect(')');
            return Field(Tuple(elements.begin(), elements.end()));
        }

        /// Named form: {'name': value, ...}
        expect('{');
        const auto & element_names = tuple.getElementNames();
        std::vector<std::optional<Field>> by_index(element_types.size());
        if (!tryConsume('}'))
        {
            while (true)
            {
                String name = parseToken();
                expect(':');
                size_t index = element_types.size();
                for (size_t i = 0; i < element_names.size(); ++i)
                {
                    if (element_names[i] == name)
                    {
                        index = i;
                        break;
                    }
                }
                if (index == element_types.size())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "DuckLake inlined struct value '{}' has unknown element '{}'",
                        text,
                        name);
                by_index[index] = parseValue(element_types[index]);
                if (tryConsume('}'))
                    break;
                expect(',');
            }
        }
        Array elements;
        elements.reserve(element_types.size());
        for (size_t i = 0; i < element_types.size(); ++i)
            elements.push_back(by_index[i].value_or(Field(Null{})));
        return Field(Tuple(elements.begin(), elements.end()));
    }

    Field parseArray(const DataTypeArray & array)
    {
        const auto & nested = array.getNestedType();
        expect('[');
        Array elements;
        if (!tryConsume(']'))
        {
            while (true)
            {
                elements.push_back(parseValue(nested));
                if (tryConsume(']'))
                    break;
                expect(',');
            }
        }
        return Field(Array(elements.begin(), elements.end()));
    }

    Field parseMap(const DataTypeMap & map)
    {
        const auto & key_type = map.getKeyType();
        const auto & value_type = map.getValueType();
        expect('{');
        Array keys;
        Array values;
        if (!tryConsume('}'))
        {
            while (true)
            {
                /// DuckDB uses '=' in the map display format ({k=v}); keys may be bare.
                String key_token = parseToken(/* map_key */ true);
                skipCastSuffix();
                if (!tryConsume('=') && !tryConsume(':'))
                    throwMalformed("'=' or ':'");
                if (WhichDataType(key_type).isUInt8() && (key_token == "true" || key_token == "false"))
                    keys.push_back(Field(UInt64(key_token == "true" ? 1 : 0)));
                else
                    keys.push_back(parseScalarText(key_token, key_type));
                values.push_back(parseValue(value_type));
                if (tryConsume('}'))
                    break;
                expect(',');
            }
        }
        Array pairs;
        pairs.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i)
            pairs.push_back(Tuple{keys[i], values[i]});
        return Field(Map(pairs.begin(), pairs.end()));
    }
};

}

Field parseInlinedValue(const String & value, const DataTypePtr & type, bool postgres_backend)
{
    if (type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "parseInlinedValue expects a non-nullable type, got {}", type->getName());

    const WhichDataType which(type);

    if (which.isString() || which.isFixedString())
        return Field(postgres_backend ? decodeByteaHex(value) : value);

    if (which.isUInt8() && postgres_backend && (value == "t" || value == "f"))
        return Field(UInt64(value == "t" ? 1 : 0));

    if (which.isTuple() || which.isArray() || which.isMap())
        return LiteralParser(value).parse(type);

    if (isDateTime64(type))
    {
        const auto & date_time = assert_cast<const DataTypeDateTime64 &>(*type);
        if (date_time.hasExplicitTimeZone())
            return parseTimestampWithOffset(value, type);
    }

    return parseScalarText(value, type);
}

ColumnPtr buildInlinedColumn(
    const std::vector<std::optional<String>> & values,
    const DataTypePtr & type,
    bool postgres_backend)
{
    const bool nullable = type->isNullable();
    const DataTypePtr nested_type = nullable ? removeNullable(type) : type;
    /// Like the Parquet reader, a NULL of a struct/list/map column becomes the default
    /// (a tuple/array/map of defaults); only scalars can be real NULLs.
    const bool container = isTuple(nested_type) || isArray(nested_type) || isMap(nested_type);

    auto nested_column = nested_type->createColumn();
    nested_column->reserve(values.size());
    auto null_map = ColumnUInt8::create();
    null_map->reserve(values.size());

    for (const auto & value : values)
    {
        if (!value.has_value())
        {
            if (!nullable && !container)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "DuckLake inlined data has NULL for a non-nullable column of type {}",
                    type->getName());
            nested_column->insertDefault();
            null_map->insertValue(UInt8(nullable ? 1 : 0));
            continue;
        }
        nested_column->insert(parseInlinedValue(*value, nested_type, postgres_backend));
        null_map->insertValue(UInt8(0));
    }

    if (!nullable)
        return nested_column;
    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}

}

}

#endif
