#include "config.h"
#if USE_AVRO

#include <cctype>
#include <limits>

#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Processors/Formats/Impl/Parquet/Decoding.h>
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

namespace
{

template <typename DecimalType>
std::optional<Field> deserializeDecimalFromBinaryRepr(const String & str, UInt32 scale, bool lower_bound)
{
    using NativeType = typename DecimalType::NativeType;
    if (str.empty() || str.size() > sizeof(NativeType))
        return std::nullopt;

    Parquet::BigEndianHelper<NativeType> big_endian_helper(str.size());
    NativeType unscaled_value = big_endian_helper.convertUnpaddedValue(std::span<const char>(str.data(), str.size()));

    NativeType integral_unit = 1;
    for (UInt32 i = 0; i < scale; ++i)
        integral_unit *= 10;
    if (scale != 0)
        unscaled_value += lower_bound ? -integral_unit : integral_unit;

    return DecimalField<DecimalType>(unscaled_value, scale);
}

}

std::optional<Field> deserializeFieldFromBinaryRepr(const String & str, const DataTypePtr & expected_type, bool lower_bound)
{
    auto non_nullable_type = removeNullable(expected_type);
    if (WhichDataType(non_nullable_type).isDecimal())
    {
        const UInt32 scale = getDecimalScale(*non_nullable_type);
        if (checkDecimal<Decimal32>(*non_nullable_type))
            return deserializeDecimalFromBinaryRepr<Decimal32>(str, scale, lower_bound);
        if (checkDecimal<Decimal64>(*non_nullable_type))
            return deserializeDecimalFromBinaryRepr<Decimal64>(str, scale, lower_bound);
        if (checkDecimal<Decimal128>(*non_nullable_type))
            return deserializeDecimalFromBinaryRepr<Decimal128>(str, scale, lower_bound);
        if (checkDecimal<Decimal256>(*non_nullable_type))
            return deserializeDecimalFromBinaryRepr<Decimal256>(str, scale, lower_bound);
        return std::nullopt;
    }
    if (non_nullable_type->getTypeId() == TypeIndex::Variant)
        return std::nullopt;

    auto column = non_nullable_type->createColumn();
    column->insertData(str.data(), str.length());
    Field result;
    column->get(0, result);
    return result;

}

}

}

#endif
