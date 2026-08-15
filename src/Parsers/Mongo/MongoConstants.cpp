#include <Parsers/Mongo/MongoConstants.h>

#include <Core/Field.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Mongo/Utils.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace Mongo
{

namespace
{

ASTPtr makeLiteral(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

std::string_view stringView(const rapidjson::Value & value)
{
    return {value.GetString(), value.GetStringLength()};
}

/// Names the field an Extended JSON wrapper belongs to in an error message, when the caller knows
/// it: the value of a filter or of an update operator is not attached to a field of its own.
std::string describeMongoField(std::string_view field_name)
{
    if (field_name.empty())
        return {};
    return fmt::format(" of the field '{}'", field_name);
}

/// The value of an Extended JSON number wrapper is a string in the canonical form and a number
/// in the relaxed one; both are accepted.
template <typename T>
T extendedJSONNumber(const rapidjson::Value & value, std::string_view wrapper)
{
    if (value.IsString())
    {
        auto text = stringView(value);
        T result{};
        ReadBufferFromMemory buffer(text.data(), text.size());
        if (!tryReadText(result, buffer) || !buffer.eof())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of '{}' is not a number: '{}'", wrapper, text);
        return result;
    }
    if constexpr (std::is_floating_point_v<T>)
    {
        if (value.IsNumber())
            return static_cast<T>(value.GetDouble());
    }
    else
    {
        if (value.IsInt64())
            return static_cast<T>(value.GetInt64());
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of '{}' must be a number or a string", wrapper);
}

}

ASTPtr tryParseMongoConstant(const rapidjson::Value & value)
{
    if (value.IsNull())
        return makeLiteral(Field());
    if (value.IsBool())
        return makeLiteral(Field(value.GetBool()));
    if (value.IsInt())
        return makeLiteral(Field(value.GetInt()));
    if (value.IsInt64())
        return makeLiteral(Field(value.GetInt64()));
    if (value.IsUint64())
        return makeLiteral(Field(value.GetUint64()));
    if (value.IsNumber())
        return makeLiteral(Field(value.GetDouble()));
    if (value.IsString())
        return makeLiteral(Field(String(stringView(value))));

    if (!value.IsObject() || value.MemberCount() != 1)
        return nullptr;

    const auto & member = *value.MemberBegin();
    const std::string_view wrapper = stringView(member.name);

    if (wrapper == "$numberInt")
        return makeLiteral(Field(extendedJSONNumber<Int32>(member.value, wrapper)));
    if (wrapper == "$numberLong")
        return makeLiteral(Field(extendedJSONNumber<Int64>(member.value, wrapper)));
    if (wrapper == "$numberDouble")
        return makeLiteral(Field(extendedJSONNumber<Float64>(member.value, wrapper)));
    if (wrapper == "$numberDecimal")
    {
        if (!member.value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of '$numberDecimal' must be a string");
        /// Mongo's `Decimal128` is a 34 digit decimal floating point number with an exponent of its
        /// own, so no single fixed point type holds all of them: the scale is derived from the
        /// value, and a value that fits no scale is rejected rather than silently rounded.
        std::string_view text = stringView(member.value);
        auto scale = decimalScaleOfNumberDecimal(text);
        if (!scale)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The value '{}' of '$numberDecimal' cannot be represented exactly by a Decimal128", text);
        return makeASTFunction("CAST", makeLiteral(Field(String(text))), makeLiteral(Field(String(fmt::format("Decimal128({})", *scale)))));
    }
    if (wrapper == "$oid")
    {
        if (!member.value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of '$oid' must be a string");
        return makeLiteral(Field(String(stringView(member.value))));
    }
    if (wrapper == "$date")
    {
        /// A Mongo date is an instant in UTC. The canonical Extended JSON spells it as the number
        /// of milliseconds since the epoch, and the relaxed one as an ISO 8601 string - the drivers
        /// send the first and a query written by hand usually the second.
        if (member.value.IsString())
            return makeASTFunction(
                "parseDateTime64BestEffort",
                makeLiteral(Field(String(stringView(member.value)))),
                makeLiteral(Field(UInt64(3))),
                makeLiteral(Field(String("UTC"))));
        /// The canonical form nests the milliseconds in a `$numberLong`. Only that exact shape is
        /// read: any other document - `{"$date": {}}` or `{"$date": {"oops": 1}}` - names no value
        /// to convert, and reading its first member regardless would accept a request that says
        /// something else.
        Int64 milliseconds = 0;
        if (member.value.IsObject())
        {
            if (member.value.MemberCount() != 1 || stringView(member.value.MemberBegin()->name) != "$numberLong")
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The document form of '$date' must be a single '$numberLong' holding the number of milliseconds since the epoch");
            milliseconds = extendedJSONNumber<Int64>(member.value.MemberBegin()->value, "$date");
        }
        else
            milliseconds = extendedJSONNumber<Int64>(member.value, "$date");
        return makeASTFunction("fromUnixTimestamp64Milli", makeLiteral(Field(milliseconds)), makeLiteral(Field(String("UTC"))));
    }

    return nullptr;
}

std::optional<std::string> tryParseMongoRegularExpression(const rapidjson::Value & value)
{
    if (!value.IsObject())
        return std::nullopt;

    std::string_view pattern;
    std::string_view options;

    if (auto it = value.FindMember("$regularExpression"); it != value.MemberEnd())
    {
        if (!it->value.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of '$regularExpression' must be a document");
        auto pattern_it = it->value.FindMember("pattern");
        if (pattern_it == it->value.MemberEnd() || !pattern_it->value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$regularExpression' must have a string 'pattern'");
        pattern = stringView(pattern_it->value);
        if (auto options_it = it->value.FindMember("options"); options_it != it->value.MemberEnd() && options_it->value.IsString())
            options = stringView(options_it->value);
    }
    else if (auto regex_it = value.FindMember("$regex"); regex_it != value.MemberEnd())
    {
        /// The driver may nest the Extended JSON form under `$regex`.
        if (regex_it->value.IsObject())
            return tryParseMongoRegularExpression(regex_it->value);
        if (!regex_it->value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of '$regex' must be a string");
        pattern = stringView(regex_it->value);
        if (auto options_it = value.FindMember("$options"); options_it != value.MemberEnd() && options_it->value.IsString())
            options = stringView(options_it->value);
    }
    else
        return std::nullopt;

    return applyMongoRegularExpressionOptions(pattern, options);
}

std::string applyMongoRegularExpressionOptions(std::string_view pattern, std::string_view options)
{
    /// RE2 spells the Mongo options as inline flags. `x` (extended) and `s` (dot matches newline)
    /// have the same meaning in both; `u` only says the pattern is UTF-8, which RE2 already is.
    std::string flags;
    for (char option : options)
    {
        switch (option)
        {
            case 'i': [[fallthrough]];
            case 'm': [[fallthrough]];
            case 's': [[fallthrough]];
            case 'x': flags.push_back(option); break;
            case 'u': break;
            default: throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported regular expression option '{}'", option);
        }
    }

    if (flags.empty())
        return std::string(pattern);
    return "(?" + flags + ")" + std::string(pattern);
}

bool isMongoExtendedJSONWrapper(const rapidjson::Value & value)
{
    if (!value.IsObject() || value.ObjectEmpty())
        return false;
    auto name = stringView(value.MemberBegin()->name);
    return !name.empty() && name.front() == '$';
}

std::pair<std::string, rapidjson::Value> convertMongoExtendedJSONWrapper(
    const rapidjson::Value & wrapper, std::string_view field_name, rapidjson::Document::AllocatorType & allocator)
{
    const auto & member = *wrapper.MemberBegin();
    auto name = stringView(member.name);

    if (name == "$oid" && member.value.IsString())
    {
        rapidjson::Value value;
        value.CopyFrom(member.value, allocator);
        return {"String", std::move(value)};
    }

    if (name == "$numberDecimal" && member.value.IsString())
    {
        /// The scale is derived from the value, the same way the filters do it for
        /// `$numberDecimal`: a fixed scale would silently round part of the value space of
        /// Mongo's `Decimal128`, which is a decimal floating point type.
        auto text = stringView(member.value);
        auto scale = decimalScaleOfNumberDecimal(text);
        if (!scale)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The value '{}' of '$numberDecimal'{} cannot be represented exactly by a Decimal128",
                text,
                describeMongoField(field_name));
        rapidjson::Value value;
        value.CopyFrom(member.value, allocator);
        return {fmt::format("Decimal128({})", *scale), std::move(value)};
    }

    if (name == "$date")
    {
        /// A Mongo date is an instant in UTC: the legacy Extended JSON spells it as the number of
        /// milliseconds since the epoch and the canonical one wraps that in `$numberLong`. It is
        /// written as ISO 8601 text with the `Z` designator, so that the instant it names does not
        /// depend on any setting, nor on the time zone of whichever context parses it: a value of a
        /// `JSON` column carries no declared type, so a text without the designator would be read
        /// as a local time - and the context that parses it is the session for an insert and the
        /// server for a mutation, which would make the two write different instants.
        std::optional<Int64> milliseconds;
        if (member.value.IsString())
        {
            rapidjson::Value value;
            value.CopyFrom(member.value, allocator);
            return {"DateTime64(3, 'UTC')", std::move(value)};
        }
        if (member.value.IsInt64())
            milliseconds = member.value.GetInt64();
        else if (
            member.value.IsObject() && member.value.MemberCount() == 1 && stringView(member.value.MemberBegin()->name) == "$numberLong"
            && member.value.MemberBegin()->value.IsString())
        {
            Int64 parsed = 0;
            auto text = stringView(member.value.MemberBegin()->value);
            ReadBufferFromMemory buffer(text.data(), text.size());
            if (tryReadText(parsed, buffer) && buffer.eof())
                milliseconds = parsed;
        }
        if (milliseconds)
        {
            WriteBufferFromOwnString formatted;
            writeDateTimeTextISO(DateTime64(*milliseconds), 3, formatted, DateLUT::instance("UTC"));
            rapidjson::Value value;
            value.SetString(formatted.str().c_str(), static_cast<rapidjson::SizeType>(formatted.str().size()), allocator);
            return {"DateTime64(3, 'UTC')", std::move(value)};
        }
    }

    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "The BSON type '{}'{} is not supported by an insert", name, describeMongoField(field_name));
}

rapidjson::Value convertMongoExtendedJSONWrappersDeep(
    const rapidjson::Value & value, std::string_view field_name, rapidjson::Document::AllocatorType & allocator)
{
    if (isMongoExtendedJSONWrapper(value))
        return convertMongoExtendedJSONWrapper(value, field_name, allocator).second;

    if (value.IsObject())
    {
        rapidjson::Value out(rapidjson::kObjectType);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
        {
            rapidjson::Value key;
            key.CopyFrom(it->name, allocator);
            rapidjson::Value converted = convertMongoExtendedJSONWrappersDeep(it->value, field_name, allocator);
            out.AddMember(key, converted, allocator);
        }
        return out;
    }

    if (value.IsArray())
    {
        rapidjson::Value out(rapidjson::kArrayType);
        for (const auto & element : value.GetArray())
        {
            rapidjson::Value converted = convertMongoExtendedJSONWrappersDeep(element, field_name, allocator);
            out.PushBack(converted, allocator);
        }
        return out;
    }

    rapidjson::Value out;
    out.CopyFrom(value, allocator);
    return out;
}

ASTPtr makeMongoJSONValue(const rapidjson::Value & value, std::string_view field_name)
{
    /// The wrappers are converted before the document is serialized, so that a `{"$date": ...}` of
    /// an embedded document becomes the same stored value the wire insert path writes rather than
    /// a `JSON` object with a `$`-named field.
    rapidjson::Document converted_document;
    auto converted = convertMongoExtendedJSONWrappersDeep(value, field_name, converted_document.GetAllocator());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    converted.Accept(writer);

    return makeASTFunction("CAST", makeLiteral(Field(String(buffer.GetString(), buffer.GetSize()))), makeLiteral(Field(String("JSON"))));
}

}

}
