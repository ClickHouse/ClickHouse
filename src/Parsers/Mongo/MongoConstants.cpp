#include <Parsers/Mongo/MongoConstants.h>

#include <Core/Field.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Mongo/Utils.h>
#include <Common/Exception.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
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
        return makeASTFunction(
            "CAST", makeLiteral(Field(String(text))), makeLiteral(Field(String(fmt::format("Decimal128({})", *scale)))));
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
        Int64 milliseconds = member.value.IsObject() ? extendedJSONNumber<Int64>(member.value.MemberBegin()->value, "$date")
                                                     : extendedJSONNumber<Int64>(member.value, "$date");
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
            case 'x':
                flags.push_back(option);
                break;
            case 'u':
                break;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported regular expression option '{}'", option);
        }
    }

    if (flags.empty())
        return std::string(pattern);
    return "(?" + flags + ")" + std::string(pattern);
}

}

}
