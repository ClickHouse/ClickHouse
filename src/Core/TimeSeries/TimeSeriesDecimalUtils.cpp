#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>

#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


UInt32 getTimeseriesTimeScale(const DataTypePtr & time_or_duration_type)
{
    if (isDecimal(time_or_duration_type) || isDateTime64(time_or_duration_type))
        return getDecimalScale(*time_or_duration_type);
    if (const auto * interval_type = checkAndGetDataType<DataTypeInterval>(time_or_duration_type.get()))
    {
        switch(interval_type->getKind())
        {
            case IntervalKind::Kind::Nanosecond: return 9;
            case IntervalKind::Kind::Microsecond: return 6;
            case IntervalKind::Kind::Millisecond: return 3;
            default: return 0;
        }
    }
    return 0;
}


String getTimeseriesTimezone(const DataTypePtr & time_type)
{
    if (isDateTimeOrDateTime64(time_type))
        return getDateTimeTimezone(*time_type);
    return "";
}


DataTypePtr makeTimeseriesTimeDataType(UInt32 scale, const String & timezone)
{
    if (scale == 0)
    {
        if (timezone.empty())
            return std::make_shared<DataTypeDateTime>();
        else
            return std::make_shared<DataTypeDateTime>(timezone);
    }
    else
    {
        if (timezone.empty())
            return std::make_shared<DataTypeDateTime64>(scale);
        else
            return std::make_shared<DataTypeDateTime64>(scale, timezone);
    }
}


namespace
{
    template <typename DecimalType>
    DecimalField<DecimalType> extractFromField(const Field & field, const DataTypePtr & type, UInt32 default_scale)
    {
        constexpr bool result_is_timestamp = std::is_same_v<DecimalType, DateTime64>;
        constexpr std::string_view what = result_is_timestamp ? "timestamp" : "duration";

        switch (field.getType())
        {
            case Field::Types::Int64:
            {
                auto value = field.safeGet<Int64>();
                if (const auto * interval_type = checkAndGetDataType<DataTypeInterval>(type.get()))
                {
                    switch(interval_type->getKind())
                    {
                        case IntervalKind::Kind::Nanosecond: return DecimalField<DecimalType>{value, 9};
                        case IntervalKind::Kind::Microsecond: return DecimalField<DecimalType>{value, 6};
                        case IntervalKind::Kind::Millisecond: return DecimalField<DecimalType>{value, 3};
                        case IntervalKind::Kind::Second: return DecimalField<DecimalType>{value, 0};
                        case IntervalKind::Kind::Minute: return DecimalField<DecimalType>{value * 60, 0};
                        case IntervalKind::Kind::Hour: return DecimalField<DecimalType>{value * (60 * 60), 0};
                        case IntervalKind::Kind::Day: return DecimalField<DecimalType>{value * (24 * 60 * 60), 0};
                        case IntervalKind::Kind::Week: return DecimalField<DecimalType>{value * (7 * 24 * 60 * 60), 0};
                        case IntervalKind::Kind::Year: return DecimalField<DecimalType>{value * (365 * 24 * 60 * 60), 0};
                        default:
                        {
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot extract a {} from an interval of type {}",
                                            what, IntervalKind{interval_type->getKind()}.toString());
                        }
                    }
                }
                else
                {
                    return DecimalField<DecimalType>{value, 0};
                }
            }
            case Field::Types::UInt64:
            {
                return DecimalField<DecimalType>{field.safeGet<UInt64>(), 0};
            }
            case Field::Types::Float64:
            {
                auto float_value = field.safeGet<Float64>();
                auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(default_scale);
                return DecimalField<DecimalType>{static_cast<Int64>(float_value * scale_multiplier + 0.5), default_scale};
            }
            case Field::Types::Decimal32:
            {
                auto decimal32 = field.safeGet<Decimal32>();
                return DecimalField<DecimalType>{decimal32.getValue(), decimal32.getScale()};
            }
            case Field::Types::Decimal64:
            {
                return field.safeGet<DecimalType>();
            }
            case Field::Types::String:
            {
                const auto & str = field.safeGet<String>();
                PrometheusQueryParsingUtil::ScalarOrDuration scalar_or_duration;
                String error_message;
                size_t error_pos;
                if (PrometheusQueryParsingUtil::parseScalarOrDuration(str, scalar_or_duration, error_message, error_pos))
                {
                    if (scalar_or_duration.duration)
                    {
                        const auto & decimal64 = *scalar_or_duration.duration;
                        return DecimalField<DecimalType>{decimal64.getValue(), decimal64.getScale()};
                    }
                    else
                    {
                        auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(default_scale);
                        return DecimalField<DecimalType>{static_cast<Int64>(*scalar_or_duration.scalar * scale_multiplier + 0.5), default_scale};
                    }
                }
                if constexpr (result_is_timestamp)
                {
                    DateTime64 datetime;
                    ReadBufferFromString buf{str};
                    if (tryReadDateTime64Text(datetime, default_scale, buf))
                    {
                        return DecimalField<DateTime64>{datetime, default_scale};
                    }
                }
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse a {} from string {}: {}",
                                what, quoteString(str), error_message);
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Cannot extract a {} from a field of type {}",
                                what, field.getType());
            }
        }
    }
}


DecimalField<DateTime64> getTimeseriesTime(const Field & field, UInt32 default_scale)
{
    return getTimeseriesTime(field, nullptr, default_scale);
}

DecimalField<DateTime64> getTimeseriesTime(const Field & field, const DataTypePtr & type, UInt32 default_scale)
{
    return extractFromField<DateTime64>(field, type, default_scale);
}

DecimalField<Decimal64> getTimeseriesDuration(const Field & field, UInt32 default_scale)
{
    return getTimeseriesDuration(field, nullptr, default_scale);
}

DecimalField<Decimal64> getTimeseriesDuration(const Field & field, const DataTypePtr & type, UInt32 default_scale)
{
    return extractFromField<Decimal64>(field, type, default_scale);
}


ASTPtr timeseriesTimeToAST(const DecimalField<DateTime64> & time)
{
    Decimal64 value = time.getValue();
    UInt32 scale = time.getScale();

    if (scale == 0)
    {
        return makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(static_cast<Int64>(value)));
    }
    else
    {
        String str = toString(value, scale);
        /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to a floating-point.
        if (str.find_first_of(".eE") == String::npos)
            str += ".";
        return makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale));
    }
}


ASTPtr timeseriesTimeToAST(const DecimalField<DateTime64> & time, const DataTypePtr & time_type)
{
    UInt32 scale = getTimeseriesTimeScale(time_type);
    Decimal64 value = DecimalUtils::convertTo<Decimal64>(scale, time.getValue(), time.getScale());

    std::shared_ptr<ASTFunction> ast;
    if (scale == 0)
    {
        ast = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(static_cast<Int64>(value)));
    }
    else
    {
        String str = toString(value, scale);
        /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to a floating-point.
        if (str.find_first_of(".eE") == String::npos)
            str += ".";
        ast = makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale));
    }

    auto timezone = getTimeseriesTimezone(time_type);
    if (!timezone.empty())
        ast->arguments->children.push_back(std::make_shared<ASTLiteral>(timezone));

    return ast;
}


ASTPtr timeseriesDurationToAST(const DecimalField<Decimal64> & duration)
{
    Decimal64 value = duration.getValue();
    UInt32 scale = duration.getScale();

    if (scale == 0)
        return std::make_shared<ASTLiteral>(static_cast<Int64>(value));
    else
        return makeASTFunction("toDecimal64", std::make_shared<ASTLiteral>(toString(value, scale)), std::make_shared<ASTLiteral>(scale));
}  


DecimalField<DateTime64> addTimeseriesDuration(const DecimalField<DateTime64> & left, const DecimalField<Decimal64> & right)
{
    auto max_scale = std::max(left.getScale(), right.getScale());
    auto scaled_left = left.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - left.getScale());
    auto scaled_right = right.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - right.getScale());
    return DecimalField<DateTime64>{scaled_left + scaled_right, max_scale};
}

DecimalField<DateTime64> subtractTimeseriesDuration(const DecimalField<DateTime64> & left, const DecimalField<Decimal64> & right)
{
    auto max_scale = std::max(left.getScale(), right.getScale());
    auto scaled_left = left.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - left.getScale());
    auto scaled_right = right.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - right.getScale());
    return DecimalField<DateTime64>{scaled_left - scaled_right, max_scale};
}

DecimalField<Decimal64> getTimeseriesDuration(const DecimalField<DateTime64> & min_time, const DecimalField<DateTime64> & max_time)
{
    auto max_scale = std::max(min_time.getScale(), max_time.getScale());
    auto scaled_min_time = min_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - min_time.getScale());
    auto scaled_max_time = max_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - max_time.getScale());
    return DecimalField<Decimal64>{scaled_max_time - scaled_min_time, max_scale};
}


size_t countTimeseriesSteps(
    const DecimalField<DateTime64> & start_time, const DecimalField<DateTime64> & end_time, const DecimalField<Decimal64> & step)
{
    UInt32 max_scale = std::max({start_time.getScale(), end_time.getScale(), step.getScale()});

    Int64 scaled_start_time = start_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - start_time.getScale());
    Int64 scaled_end_time = end_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - end_time.getScale());
    Int64 scaled_step = step.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - step.getScale());

    if (scaled_start_time == scaled_end_time)
        return 1;

    if (scaled_end_time < scaled_start_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");
    
    if (scaled_step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");

    return (scaled_end_time - scaled_start_time) / scaled_step + 1;
}


DecimalField<DateTime64> roundUpTimeseriesTime(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step)
{
    auto max_scale = std::max(time.getScale(), step.getScale());
    auto scaled_time = time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - time.getScale());
    auto scaled_step = step.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - step.getScale());
    if (scaled_step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto x = scaled_time % scaled_step;
    if (!x)
        return time;
    return DecimalField<DateTime64>{scaled_time + scaled_step - x, max_scale};
}


DecimalField<DateTime64> roundDownTimeseriesTime(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step)
{
    auto max_scale = std::max(time.getScale(), step.getScale());
    auto scaled_time = time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - time.getScale());
    auto scaled_step = step.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - step.getScale());
    if (scaled_step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto x = scaled_time % scaled_step;
    if (!x)
        return time;
    return DecimalField<DateTime64>{scaled_time - x, max_scale};
}

}
