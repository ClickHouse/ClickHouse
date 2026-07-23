#include "config.h"
#if USE_AVRO

#include <cctype>
#include <limits>

#include <Common/Exception.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergFieldParseHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Iceberg
{

Int64 fieldToInt64(const Field & value, std::string_view context, std::string_view arg_name)
{
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>();
    if (value.getType() == Field::Types::UInt64)
    {
        UInt64 v = value.safeGet<UInt64>();
        if (v > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' is too large: {}", context, arg_name, v);
        return static_cast<Int64>(v);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be an integer literal", context, arg_name);
}

bool fieldToBool(const Field & value, std::string_view context, std::string_view arg_name)
{
    if (value.getType() == Field::Types::Bool)
        return value.safeGet<bool>();
    if (value.getType() == Field::Types::UInt64)
        return value.safeGet<UInt64>() != 0;
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>() != 0;
    if (value.getType() == Field::Types::String)
    {
        String lower = value.safeGet<String>();
        for (auto & ch : lower)
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));

        if (lower == "true")
            return true;
        if (lower == "false")
            return false;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be a boolean or integer literal", context, arg_name);
}

Int64 fieldToPeriodMs(const Field & value, std::string_view context, std::string_view arg_name)
{
    if (value.getType() != Field::Types::String)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{} expects '{}' to be a duration string like '3d', '12h', '30m', '15s' or '250ms'",
            context, arg_name);

    const String & input = value.safeGet<String>();
    if (input.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} '{}' cannot be empty", context, arg_name);

    Decimal64 parsed_duration_ms;
    try
    {
        /// Scale=3 means the decimal stores milliseconds in the integer payload.
        parsed_duration_ms = parseTimeSeriesDuration(input, /* duration_scale */ 3);
    }
    catch (const Exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: invalid duration '{}' for '{}'", context, input, arg_name);
    }

    Int64 milliseconds = parsed_duration_ms.value;
    if (milliseconds < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be non-negative", context, arg_name);

    return milliseconds;
}

std::vector<Int64> fieldToInt64Array(const Field & value, std::string_view context, std::string_view arg_name)
{
    if (value.getType() != Field::Types::Array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} expects '{}' to be an array literal", context, arg_name);

    const auto & src = value.safeGet<Array>();
    std::vector<Int64> result;
    result.reserve(src.size());
    for (const auto & elem : src)
        result.push_back(fieldToInt64(elem, context, arg_name));
    return result;
}

}
}

#endif
