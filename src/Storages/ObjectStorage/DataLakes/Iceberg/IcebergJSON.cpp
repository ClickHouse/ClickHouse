#include "config.h"

#if USE_AVRO

#if !USE_SIMDJSON
#    error "Iceberg requires simdjson"
#endif

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergJSON.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <base/defines.h>
#include <base/find_symbols.h>
#include <double-conversion/double-conversion.h>
#include <simdjson.h>

#include <charconv>
#include <cmath>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int INCORRECT_DATA;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace DB::Iceberg
{

void throwJSONValueOutOfRange(std::string_view type_name)
{
    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "JSON value is out of range of type {}", type_name);
}

namespace
{

const JSONValue null_value;

std::string_view typeName(const JSONValue::Variant & value)
{
    static constexpr std::string_view names[] = {"null", "bool", "Int64", "UInt64", "Float64", "string", "object", "array"};
    return names[value.index()];
}

[[noreturn]] void throwBadType(const JSONValue::Variant & value, std::string_view expected)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected JSON {}, got {}", expected, typeName(value));
}

template <typename T>
T parseNumber(std::string_view str)
{
    std::string_view number = str;
    if (!number.empty() && number.front() == '+')
        number.remove_prefix(1);
    T result{};
    const auto * end = number.data() + number.size();
    auto [ptr, error] = std::from_chars(number.data(), end, result);
    if (number.empty() || error != std::errc() || ptr != end)
        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse JSON string '{}' as a number", str);
    return result;
}

String formatFloat64(Float64 value)
{
    using double_conversion::DoubleToStringConverter;
    static const DoubleToStringConverter converter(
        DoubleToStringConverter::UNIQUE_ZERO | DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN,
        "inf",
        "nan",
        'e',
        -std::numeric_limits<double>::digits10,
        std::numeric_limits<double>::digits10,
        0,
        0);

    char buffer[DoubleToStringConverter::kBase10MaximalLength + 16];
    double_conversion::StringBuilder builder(buffer, sizeof(buffer));
    converter.ToShortest(value, &builder);
    return String(buffer, builder.position());
}

void writeEscapedString(String & out, std::string_view str)
{
    static constexpr char hex_digits[] = "0123456789ABCDEF";

    out += '"';
    const char * pos = str.data();
    const char * end = str.data() + str.size();
    while (pos < end)
    {
        const char * next = find_first_symbols<'"', '\\'>(pos, end);
        for (const char * it = pos; it < next; ++it)
        {
            if (isControlASCII(*it))
            {
                next = it;
                break;
            }
        }
        out.append(pos, next);
        if (next == end)
            break;

        char c = *next;
        switch (c)
        {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\t': out += "\\t"; break;
            case '\r': out += "\\r"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
            {
                unsigned char code = static_cast<unsigned char>(c);
                out += "\\u00";
                out += hex_digits[code >> 4];
                out += hex_digits[code & 0xF];
                break;
            }
        }
        pos = next + 1;
    }
    out += '"';
}

void writeValue(String & out, const JSONValue & value, unsigned indent, unsigned step);

void writeObject(String & out, const JSONObject & object, unsigned indent, unsigned step)
{
    out += '{';
    if (indent > 0)
        out += '\n';

    for (auto it = object.begin(); it != object.end();)
    {
        out.append(indent, ' ');
        writeEscapedString(out, it->first);
        out += indent > 0 ? " : " : ":";
        writeValue(out, it->second, indent + step, step);
        if (++it != object.end())
            out += ',';
        if (step > 0)
            out += '\n';
    }

    if (indent >= step)
        indent -= step;
    out.append(indent, ' ');
    out += '}';
}

void writeArray(String & out, const JSONArray & array, unsigned indent, unsigned step)
{
    out += '[';
    if (indent > 0)
        out += '\n';

    for (auto it = array.begin(); it != array.end();)
    {
        out.append(indent, ' ');
        writeValue(out, *it, indent + step, step);
        if (++it != array.end())
        {
            out += ',';
            if (step > 0)
                out += '\n';
        }
    }

    if (step > 0)
        out += '\n';
    if (indent >= step)
        indent -= step;
    out.append(indent, ' ');
    out += ']';
}

void writeValue(String & out, const JSONValue & value, unsigned indent, unsigned step)
{
    std::visit(
        [&](const auto & v)
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::monostate>)
                out += "null";
            else if constexpr (std::is_same_v<T, bool>)
                out += v ? "true" : "false";
            else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>)
                out += std::to_string(v);
            else if constexpr (std::is_same_v<T, Float64>)
                out += formatFloat64(v);
            else if constexpr (std::is_same_v<T, String>)
                writeEscapedString(out, v);
            else if constexpr (std::is_same_v<T, JSONObjectPtr>)
                writeObject(out, *v, indent, step);
            else
                writeArray(out, *v, indent, step);
        },
        value.getVariant());
}

JSONValue fromSimdJSON(simdjson::dom::element element)
{
    switch (element.type())
    {
        case simdjson::dom::element_type::OBJECT:
        {
            auto object = JSONObject::create();
            for (auto [key, value] : element.get_object().value_unsafe())
                object->set(key, fromSimdJSON(value));
            return object;
        }
        case simdjson::dom::element_type::ARRAY:
        {
            auto array = JSONArray::create();
            for (auto value : element.get_array().value_unsafe())
                array->add(fromSimdJSON(value));
            return array;
        }
        case simdjson::dom::element_type::INT64:
            return element.get_int64().value_unsafe();
        case simdjson::dom::element_type::UINT64:
            return element.get_uint64().value_unsafe();
        case simdjson::dom::element_type::DOUBLE:
            return element.get_double().value_unsafe();
        case simdjson::dom::element_type::STRING:
            return element.get_string().value_unsafe();
        case simdjson::dom::element_type::BOOL:
            return element.get_bool().value_unsafe();
        case simdjson::dom::element_type::NULL_VALUE:
            return nullptr;
        case simdjson::dom::element_type::BIGINT:
            throw Exception(ErrorCodes::INCORRECT_DATA, "JSON number {} does not fit into 64 bits", element.get_bigint().value_unsafe());
    }
    UNREACHABLE();
}

}

JSONValue::JSONValue(JSONObjectPtr value_) : value(std::move(value_))
{
}

JSONValue::JSONValue(JSONArrayPtr value_) : value(std::move(value_))
{
}

const String & JSONValue::getString() const
{
    if (const auto * result = std::get_if<String>(&value))
        return *result;
    throwBadType(value, "string");
}

const JSONObjectPtr & JSONValue::getObject() const
{
    if (const auto * result = std::get_if<JSONObjectPtr>(&value))
        return *result;
    throwBadType(value, "object");
}

const JSONArrayPtr & JSONValue::getArray() const
{
    if (const auto * result = std::get_if<JSONArrayPtr>(&value))
        return *result;
    throwBadType(value, "array");
}

bool JSONValue::toBool() const
{
    return std::visit(
        [&](const auto & v) -> bool
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, bool>)
                return v;
            else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64> || std::is_same_v<T, Float64>)
                return v != 0;
            else if constexpr (std::is_same_v<T, String>)
                return !(v.empty() || v == "0" || equalsCaseInsensitive(v, "false"));
            else
                throwBadType(value, "bool");
        },
        value);
}

Int64 JSONValue::toInt64() const
{
    return std::visit(
        [&](const auto & v) -> Int64
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, bool>)
                return v;
            else if constexpr (std::is_same_v<T, Int64>)
                return v;
            else if constexpr (std::is_same_v<T, UInt64>)
            {
                if (!std::in_range<Int64>(v))
                    throwJSONValueOutOfRange("Int64");
                return static_cast<Int64>(v);
            }
            else if constexpr (std::is_same_v<T, Float64>)
            {
                if (!std::isfinite(v) || v < static_cast<Float64>(std::numeric_limits<Int64>::min())
                    || v >= static_cast<Float64>(std::numeric_limits<Int64>::max()))
                    throwJSONValueOutOfRange("Int64");
                return static_cast<Int64>(v);
            }
            else if constexpr (std::is_same_v<T, String>)
                return parseNumber<Int64>(v);
            else
                throwBadType(value, "integer");
        },
        value);
}

UInt64 JSONValue::toUInt64() const
{
    return std::visit(
        [&](const auto & v) -> UInt64
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, bool>)
                return v;
            else if constexpr (std::is_same_v<T, Int64>)
            {
                if (v < 0)
                    throwJSONValueOutOfRange("UInt64");
                return static_cast<UInt64>(v);
            }
            else if constexpr (std::is_same_v<T, UInt64>)
                return v;
            else if constexpr (std::is_same_v<T, Float64>)
            {
                if (!std::isfinite(v) || v < 0 || v >= static_cast<Float64>(std::numeric_limits<UInt64>::max()))
                    throwJSONValueOutOfRange("UInt64");
                return static_cast<UInt64>(v);
            }
            else if constexpr (std::is_same_v<T, String>)
                return parseNumber<UInt64>(v);
            else
                throwBadType(value, "integer");
        },
        value);
}

Float64 JSONValue::toFloat64() const
{
    return std::visit(
        [&](const auto & v) -> Float64
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, Int64> || std::is_same_v<T, UInt64> || std::is_same_v<T, Float64>)
                return static_cast<Float64>(v);
            else if constexpr (std::is_same_v<T, String>)
                return parseNumber<Float64>(v);
            else
                throwBadType(value, "number");
        },
        value);
}

String JSONValue::toString() const
{
    return std::visit(
        [&](const auto & v) -> String
        {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::monostate>)
                throwBadType(value, "value convertible to string");
            else if constexpr (std::is_same_v<T, bool>)
                return v ? "true" : "false";
            else if constexpr (std::is_same_v<T, Int64> || std::is_same_v<T, UInt64>)
                return std::to_string(v);
            else if constexpr (std::is_same_v<T, Float64>)
                return formatFloat64(v);
            else if constexpr (std::is_same_v<T, String>)
                return v;
            else
                return stringifyJSON(*this);
        },
        value);
}

JSONValue JSONValue::clone() const
{
    if (const auto * object = std::get_if<JSONObjectPtr>(&value))
        return (*object)->clone();
    if (const auto * array = std::get_if<JSONArrayPtr>(&value))
        return (*array)->clone();
    return *this;
}

bool JSONValue::operator==(const JSONValue & other) const
{
    if (value.index() != other.value.index())
        return false;
    if (const auto * object = std::get_if<JSONObjectPtr>(&value))
        return **object == *std::get<JSONObjectPtr>(other.value);
    if (const auto * array = std::get_if<JSONArrayPtr>(&value))
        return **array == *std::get<JSONArrayPtr>(other.value);
    return value == other.value;
}

bool JSONObject::isNull(std::string_view key) const
{
    auto it = values.find(key);
    return it == values.end() || it->second.isNull();
}

const JSONValue & JSONObject::get(std::string_view key) const
{
    auto it = values.find(key);
    return it == values.end() ? null_value : it->second;
}

const JSONValue & JSONObject::getExisting(std::string_view key) const
{
    auto it = values.find(key);
    if (it == values.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON object has no key '{}'", key);
    return it->second;
}

JSONObjectPtr JSONObject::getObject(std::string_view key) const
{
    auto it = values.find(key);
    if (it == values.end() || !it->second.isObject())
        return nullptr;
    return it->second.getObject();
}

JSONArrayPtr JSONObject::getArray(std::string_view key) const
{
    auto it = values.find(key);
    if (it == values.end() || !it->second.isArray())
        return nullptr;
    return it->second.getArray();
}

void JSONObject::set(std::string_view key, JSONValue value)
{
    auto it = values.find(key);
    if (it == values.end())
        values.emplace(String(key), std::move(value));
    else
        it->second = std::move(value);
}

void JSONObject::remove(std::string_view key)
{
    auto it = values.find(key);
    if (it != values.end())
        values.erase(it);
}

std::vector<String> JSONObject::getNames() const
{
    std::vector<String> names;
    names.reserve(values.size());
    for (const auto & [name, _] : values)
        names.push_back(name);
    return names;
}

JSONObjectPtr JSONObject::clone() const
{
    auto result = create();
    for (const auto & [name, value] : values)
        result->values.emplace(name, value.clone());
    return result;
}

const JSONValue & JSONArray::get(size_t index) const
{
    return index < values.size() ? values[index] : null_value;
}

const JSONValue & JSONArray::getExisting(size_t index) const
{
    if (index >= values.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON array index {} is out of range, size is {}", index, values.size());
    return values[index];
}

JSONObjectPtr JSONArray::getObject(size_t index) const
{
    if (index >= values.size() || !values[index].isObject())
        return nullptr;
    return values[index].getObject();
}

JSONArrayPtr JSONArray::getArray(size_t index) const
{
    if (index >= values.size() || !values[index].isArray())
        return nullptr;
    return values[index].getArray();
}

void JSONArray::set(size_t index, JSONValue value)
{
    if (index >= values.size())
        values.resize(index + 1);
    values[index] = std::move(value);
}

void JSONArray::remove(size_t index)
{
    if (index < values.size())
        values.erase(values.begin() + index);
}

JSONArrayPtr JSONArray::clone() const
{
    auto result = create();
    result->values.reserve(values.size());
    for (const auto & value : values)
        result->values.push_back(value.clone());
    return result;
}

JSONValue parseJSON(std::string_view json)
{
    simdjson::dom::parser parser;
    simdjson::dom::element root;
    auto error = parser.parse(json.data(), json.size()).get(root);
    if (error)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON: {}", simdjson::error_message(error));
    return fromSimdJSON(root);
}

JSONObjectPtr parseJSONObject(std::string_view json)
{
    auto result = parseJSON(json);
    if (!result.isObject())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected JSON object at the top level, got {}", typeName(result.getVariant()));
    return result.getObject();
}

String stringifyJSON(const JSONValue & json, unsigned indent)
{
    String result;
    writeValue(result, json, indent, indent);
    return result;
}

}

#endif
