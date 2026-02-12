#include <Parsers/Prometheus/parseTimeSeriesTypes.h>

#include <Common/IntervalKind.h>
#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeInterval.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DECIMAL_OVERFLOW;
}

namespace
{
    template <is_decimal T>
    std::string_view getTypeName()
    {
        if constexpr (std::is_same_v<T, DateTime64>)
            return "timestamp";
        else
            return "duration";
    }

    template <is_decimal T>
    T getFromInt(Int64 int_value, UInt32 scale)
    {
        T result;
        if (common::mulOverflow(int_value, DecimalUtils::scaleMultiplier<T>(scale), result.value))
        {
            throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                            "Cannot convert {} to {}: Overflow, the number is too big",
                            int_value, getTypeName<T>());
        }
        return result;
    }

    template <is_decimal T>
    T getFromFloat(Float64 float_value, UInt32 scale)
    {
        Float64 scaled_value = float_value * static_cast<Float64>(DecimalUtils::scaleMultiplier<T>(scale));
        if ((scaled_value > static_cast<Float64>(std::numeric_limits<typename T::NativeType>::max())) ||
            (scaled_value < static_cast<Float64>(std::numeric_limits<typename T::NativeType>::min())))
        {
            throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                            "Cannot convert {} to {}: Overflow, the number is too big",
                            float_value, getTypeName<T>());
        }
        return static_cast<typename T::NativeType>(scaled_value);
    }

    template <is_decimal T>
    T getFromIntervalKind(Int64 intervals, IntervalKind interval_kind, UInt32 scale)
    {
        Int64 unit_multiplier = 1;
        Int64 scale_multiplier = 1;
        Int64 scale_divisor = 1;

        switch (interval_kind.kind)
        {
            case IntervalKind::Kind::Nanosecond:
            {
                if (scale > 9)
                    scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale - 9);
                else if (scale < 9)
                    scale_divisor = DecimalUtils::scaleMultiplier<T>(9 - scale);
                break;
            }
            case IntervalKind::Kind::Microsecond:
            {
                if (scale > 6)
                    scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale - 6);
                else if (scale < 6)
                    scale_divisor = DecimalUtils::scaleMultiplier<T>(6 - scale);
                break;
            }
            case IntervalKind::Kind::Millisecond:
            {
                if (scale > 3)
                    scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale - 3);
                else if (scale < 3)
                    scale_divisor = DecimalUtils::scaleMultiplier<T>(3 - scale);
                break;
            }
            case IntervalKind::Kind::Second:
            {
                scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale);
                break;
            }
            case IntervalKind::Kind::Minute:
            {
                unit_multiplier = 60;
                scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale);
                break;
            }
            case IntervalKind::Kind::Hour:
            {
                unit_multiplier = 60 * 60;
                scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale);
                break;
            }
            case IntervalKind::Kind::Day:
            {
                unit_multiplier = 24 * 60 * 60;
                scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale);
                break;
            }
            case IntervalKind::Kind::Week:
            {
                unit_multiplier = 7 * 24 * 60 * 60;
                scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale);
                break;
            }
            case IntervalKind::Kind::Year:
            {
                /// "1y" means exactly 365 days in Prometheus, so we don't take leap years into account here.
                unit_multiplier = 365 * 24 * 60 * 60;
                scale_multiplier = DecimalUtils::scaleMultiplier<T>(scale);
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Cannot extract a {} from an interval of type {}",
                                getTypeName<T>(), interval_kind.toString());
            }
        }

        T result;
        if (common::mulOverflow(intervals, unit_multiplier, result.value)
            || common::mulOverflow(result.value, scale_multiplier, result.value))
        {
            throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                            "Cannot convert {} {}s to {}: Overflow, the number is too big",
                            intervals, interval_kind.toString(), getTypeName<T>());
        }

        result.value /= scale_divisor;
        return result;
    }

    template <is_decimal T>
    T parseFromString(std::string_view str, UInt32 scale)
    {
        T result;
        String error_message;
        size_t error_pos;

        if constexpr (std::is_same_v<T, DateTime64>)
        {
            if (PrometheusQueryParsingUtil::tryParseTimestamp(str, scale, result, &error_message, &error_pos))
                return result;

            ReadBufferFromString buf{str};
            if (tryReadDateTime64Text(result, scale, buf))
                return result;
        }
        else
        {
            if (PrometheusQueryParsingUtil::tryParseDuration(str, scale, result, &error_message, &error_pos))
                return result;
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse a {} from string {}: {}",
                        getTypeName<T>(), quoteString(str), error_message);
    }

    template <is_decimal T>
    T getFromField(const Field & field, const DataTypePtr & field_data_type, UInt32 scale)
    {
        switch (field.getType())
        {
            case Field::Types::Int64:
            {
                if (const auto * interval_type = typeid_cast<const DataTypeInterval *>(field_data_type.get()))
                    return getFromIntervalKind<T>(field.safeGet<Int64>(), interval_type->getKind(), scale);
                else
                    return getFromInt<T>(field.safeGet<Int64>(), scale);
            }
            case Field::Types::UInt64:
            {
                return getFromInt<T>(field.safeGet<UInt64>(), scale);
            }
            case Field::Types::Float64:
            {
                return getFromFloat<T>(field.safeGet<Float64>(), scale);
            }
            case Field::Types::Decimal32:
            {
                auto decimal32 = field.safeGet<Decimal32>();
                return DecimalUtils::convertTo<T>(scale, decimal32.getValue(), decimal32.getScale());
            }
            case Field::Types::Decimal64:
            {
                auto decimal64 = field.safeGet<Decimal64>();
                return DecimalUtils::convertTo<T>(scale, decimal64.getValue(), decimal64.getScale());
            }
            case Field::Types::String:
            {
                return parseFromString<T>(field.safeGet<String>(), scale);
            }
            default:
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Cannot extract a {} from a field of type {}", getTypeName<T>(), field.getType());
            }
        }
    }
}

DateTime64 parseTimeSeriesTimestamp(const String & str, UInt32 timestamp_scale)
{
    return parseFromString<DateTime64>(str, timestamp_scale);
}

DateTime64 parseTimeSeriesTimestamp(const Field & field, UInt32 timestamp_scale)
{
    return getFromField<DateTime64>(field, nullptr, timestamp_scale);
}

DateTime64 parseTimeSeriesTimestamp(const Field & field, const DataTypePtr & field_data_type, UInt32 timestamp_scale)
{
    return getFromField<DateTime64>(field, field_data_type, timestamp_scale);
}

Decimal64 parseTimeSeriesDuration(const String & str, UInt32 duration_scale)
{
    return parseFromString<Decimal64>(str, duration_scale);
}

Decimal64 parseTimeSeriesDuration(const Field & field, UInt32 duration_scale)
{
    return getFromField<Decimal64>(field, nullptr, duration_scale);
}

Decimal64 parseTimeSeriesDuration(const Field & field, const DataTypePtr & field_data_type, UInt32 duration_scale)
{
    return getFromField<Decimal64>(field, field_data_type, duration_scale);
}

}
