#pragma once

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Core/AccurateComparison.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FormatImpl.h>
#include <Functions/TransformDateTime64.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Common/IPv6ToBinary.h>
#include <Common/LoggingFormatStringHelpers.h>

namespace DB
{
namespace Setting
{
extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
extern const SettingsBool precise_float_parsing;
extern const SettingsBool date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands;
}


namespace ErrorCodes
{
extern const int CANNOT_PARSE_TEXT;
extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
extern const int CANNOT_CONVERT_TYPE;
}

namespace detail
{

enum class BehaviourOnErrorFromString : uint8_t
{
    ConvertDefaultBehaviorTag,
    ConvertReturnNullOnErrorTag,
    ConvertReturnZeroOnErrorTag
};

enum class ConvertFromStringExceptionMode : uint8_t
{
    Throw, /// Throw exception if value cannot be parsed.
    Zero, /// Fill with zero or default if value cannot be parsed.
    Null /// Return ColumnNullable with NULLs when value cannot be parsed.
};

enum class ConvertFromStringParsingMode : uint8_t
{
    Normal,
    BestEffort, /// Only applicable for DateTime. Will use sophisticated method, that is slower.
    BestEffortUS
};

/// Function toUnixTimestamp has exactly the same implementation as toDateTime of String type.
struct NameToUnixTimestamp
{
    static constexpr auto name = "toUnixTimestamp";
};

/// Declared early because used below.
struct NameToDate
{
    static constexpr auto name = "toDate";
};
struct NameToDate32
{
    static constexpr auto name = "toDate32";
};
struct NameToDateTime
{
    static constexpr auto name = "toDateTime";
};
struct NameToDateTime32
{
    static constexpr auto name = "toDateTime32";
};
struct NameToDateTime64
{
    static constexpr auto name = "toDateTime64";
};
struct NameToString
{
    static constexpr auto name = "toString";
};
struct NameToDecimal32
{
    static constexpr auto name = "toDecimal32";
};
struct NameToDecimal64
{
    static constexpr auto name = "toDecimal64";
};
struct NameToDecimal128
{
    static constexpr auto name = "toDecimal128";
};
struct NameToDecimal256
{
    static constexpr auto name = "toDecimal256";
};


/** Type conversion functions.
  * toType - conversion in "natural way";
  */
UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column);

ColumnUInt8::MutablePtr copyNullMap(ColumnPtr col);

/** Throw exception with verbose message when string value is not parsed completely.
    */
[[noreturn]] inline void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, const IDataType & result_type)
{
    WriteBufferFromOwnString message_buf;
    message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size()) << " as "
                << result_type.getName() << ": syntax error";

    if (read_buffer.offset())
        message_buf << " at position " << read_buffer.offset() << " (parsed just " << quote
                    << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
    else
        message_buf << " at begin of string";

    // Currently there are no functions toIPv{4,6}Or{Null,Zero}
    if (isNativeNumber(result_type) && !(result_type.getName() == "IPv4" || result_type.getName() == "IPv6"))
        message_buf << ". Note: there are to" << result_type.getName() << "OrZero and to" << result_type.getName()
                    << "OrNull functions, which returns zero/NULL instead of throwing exception.";

    throw Exception(PreformattedMessage{message_buf.str(),
        "Cannot parse string {} as {}: syntax error {}",
        {String(read_buffer.buffer().begin(), read_buffer.buffer().size()), result_type.getName()}},
        ErrorCodes::CANNOT_PARSE_TEXT);
}

/** Conversion of time_t to UInt16, Int32, UInt32
  */
template <typename DataType>
void convertFromTime(typename DataType::FieldType & x, time_t & time)
{
    x = time;
}

template <>
inline void convertFromTime<DataTypeDateTime>(DataTypeDateTime::FieldType & x, time_t & time)
{
    if (unlikely(time < 0))
        x = 0;
    else if (unlikely(time > MAX_DATETIME_TIMESTAMP))
        x = MAX_DATETIME_TIMESTAMP;
    else
        x = static_cast<UInt32>(time);
}


/** Conversion of strings to numbers, dates, datetimes: through parsing.
  */
template <typename DataType>
void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool precise_float_parsing)
{
    if constexpr (is_floating_point<typename DataType::FieldType>)
    {
        if (precise_float_parsing)
            readFloatTextPrecise(x, rb);
        else
            readFloatTextFast(x, rb);
    }
    else
        readText(x, rb);
}

template <>
inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    DayNum tmp(0);
    readDateText(tmp, rb, *time_zone);
    x = tmp;
}

template <>
inline void parseImpl<DataTypeDate32>(DataTypeDate32::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    ExtendedDayNum tmp(0);
    readDateText(tmp, rb, *time_zone);
    x = tmp;
}


// NOTE: no need of extra overload of DateTime64, since readDateTimeText64 has different signature and that case is explicitly handled in the calling code.
template <>
inline void parseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    time_t time = 0;
    readDateTimeText(time, rb, *time_zone);
    convertFromTime<DataTypeDateTime>(x, time);
}

template <>
inline void parseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    UUID tmp;
    readUUIDText(tmp, rb);
    x = tmp.toUnderType();
}

template <>
inline void parseImpl<DataTypeIPv4>(DataTypeIPv4::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    IPv4 tmp;
    readIPv4Text(tmp, rb);
    x = tmp.toUnderType();
}

template <>
inline void parseImpl<DataTypeIPv6>(DataTypeIPv6::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    IPv6 tmp;
    readIPv6Text(tmp, rb);
    x = tmp;
}

template <typename DataType>
bool tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool precise_float_parsing)
{
    if constexpr (is_floating_point<typename DataType::FieldType>)
    {
        if (precise_float_parsing)
            return tryReadFloatTextPrecise(x, rb);
        else
            return tryReadFloatTextFast(x, rb);
    }
    else /*if constexpr (is_integral_v<typename DataType::FieldType>)*/
        return tryReadIntText(x, rb);
}

template <>
inline bool tryParseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    DayNum tmp(0);
    if (!tryReadDateText(tmp, rb, *time_zone))
        return false;
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeDate32>(DataTypeDate32::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    ExtendedDayNum tmp(0);
    if (!tryReadDateText(tmp, rb, *time_zone))
        return false;
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone, bool)
{
    time_t time = 0;
    if (!tryReadDateTimeText(time, rb, *time_zone))
        return false;
    convertFromTime<DataTypeDateTime>(x, time);
    return true;
}

template <>
inline bool tryParseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    UUID tmp;
    if (!tryReadUUIDText(tmp, rb))
        return false;

    x = tmp.toUnderType();
    return true;
}

template <>
inline bool tryParseImpl<DataTypeIPv4>(DataTypeIPv4::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    IPv4 tmp;
    if (!tryReadIPv4Text(tmp, rb))
        return false;

    x = tmp.toUnderType();
    return true;
}

template <>
inline bool tryParseImpl<DataTypeIPv6>(DataTypeIPv6::FieldType & x, ReadBuffer & rb, const DateLUTImpl *, bool)
{
    IPv6 tmp;
    if (!tryReadIPv6Text(tmp, rb))
        return false;

    x = tmp;
    return true;
}

/** Conversion of Date to DateTime: adding 00:00:00 time component.
  */
template <FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior>
struct ToDateTimeImpl
{
    static constexpr auto name = "toDateTime";

    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (d > MAX_DATETIME_DAY_NUM) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Day number {} is out of bounds of type DateTime", d);
        }
        else if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
        {
            d = std::min<time_t>(d, MAX_DATETIME_DAY_NUM);
        }
        return static_cast<UInt32>(time_zone.fromDayNum(DayNum(d)));
    }

    static UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
        {
            if (d < 0)
                return 0;
            else if (d > MAX_DATETIME_DAY_NUM)
                d = MAX_DATETIME_DAY_NUM;
        }
        else if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (d < 0 || d > MAX_DATETIME_DAY_NUM) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type DateTime", d);
        }
        return static_cast<UInt32>(time_zone.fromDayNum(ExtendedDayNum(d)));
    }

    static UInt32 execute(UInt32 dt, const DateLUTImpl & /*time_zone*/) { return dt; }

    static UInt32 execute(Int64 dt64, const DateLUTImpl & /*time_zone*/)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Ignore)
            return static_cast<UInt32>(dt64);
        else
        {
            if (dt64 < 0 || dt64 >= MAX_DATETIME_TIMESTAMP)
            {
                if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
                    return dt64 < 0 ? 0 : std::numeric_limits<UInt32>::max();
                else
                    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type DateTime", dt64);
            }
            else
                return static_cast<UInt32>(dt64);
        }
    }
};


/// Implementation of toDate function.

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTransform32Or64
{
    static constexpr auto name = "toDate";

    static NO_SANITIZE_UNDEFINED UInt16 execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from > MAX_DATETIME_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", from);
        }
        /// if value is smaller (or equal) than maximum day value for Date, than treat it as day num,
        /// otherwise treat it as unix timestamp. This is a bit weird, but we leave this behavior.
        if (from <= DATE_LUT_MAX_DAY_NUM)
            return from;
        else
            return time_zone.toDayNum(std::min(time_t(from), time_t(MAX_DATETIME_TIMESTAMP)));
    }
};


template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTransform32Or64Signed
{
    static constexpr auto name = "toDate";

    static NO_SANITIZE_UNDEFINED UInt16 execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        // TODO: decide narrow or extended range based on FromType
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < 0 || from > MAX_DATE_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", from);
        }
        else
        {
            if (from < 0)
                return 0;
        }
        return (from <= DATE_LUT_MAX_DAY_NUM) ? static_cast<UInt16>(from)
                                              : time_zone.toDayNum(std::min(time_t(from), time_t(MAX_DATE_TIMESTAMP)));
    }
};

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTransform8Or16Signed
{
    static constexpr auto name = "toDate";

    static NO_SANITIZE_UNDEFINED UInt16 execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
        {
            if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", from);
            else
                return 0;
        }
        return from;
    }
};

/// Implementation of toDate32 function.

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDate32Transform32Or64
{
    static constexpr auto name = "toDate32";

    static NO_SANITIZE_UNDEFINED Int32 execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        if (from < DATE_LUT_MAX_EXTEND_DAY_NUM)
        {
            return static_cast<Int32>(from);
        }
        else
        {
            if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
            {
                if (from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type Date32", from);
            }
            return time_zone.toDayNum(std::min(time_t(from), time_t(MAX_DATETIME64_TIMESTAMP)));
        }
    }
};

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDate32Transform32Or64Signed
{
    static constexpr auto name = "toDate32";

    static NO_SANITIZE_UNDEFINED Int32 execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        static const Int32 daynum_min_offset = -static_cast<Int32>(DateLUTImpl::getDayNumOffsetEpoch());

        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < daynum_min_offset || from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type Date32", from);
        }

        if (from < daynum_min_offset)
            return daynum_min_offset;

        return (from < DATE_LUT_MAX_EXTEND_DAY_NUM) ? static_cast<Int32>(from)
                                                    : time_zone.toDayNum(std::min(time_t(Int64(from)), time_t(MAX_DATETIME64_TIMESTAMP)));
    }
};

template <typename FromType>
struct ToDate32Transform8Or16Signed
{
    static constexpr auto name = "toDate32";

    static NO_SANITIZE_UNDEFINED Int32 execute(const FromType & from, const DateLUTImpl &) { return from; }
};

template <typename FromType, typename ToType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTimeTransform64
{
    static constexpr auto name = "toDateTime";

    static NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from > MAX_DATETIME_TIMESTAMP) [[unlikely]]
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime", from);
        }
        return static_cast<ToType>(std::min(time_t(from), time_t(MAX_DATETIME_TIMESTAMP)));
    }
};

template <typename FromType, typename ToType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTimeTransformSigned
{
    static constexpr auto name = "toDateTime";

    static NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
        {
            if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime", from);
            else
                return 0;
        }
        return from;
    }
};

template <typename FromType, typename ToType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTimeTransform64Signed
{
    static constexpr auto name = "toDateTime";

    static NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < 0 || from > MAX_DATETIME_TIMESTAMP) [[unlikely]]
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime", from);
        }

        if (from < 0)
            return 0;
        return static_cast<ToType>(std::min(time_t(from), time_t(MAX_DATETIME_TIMESTAMP)));
    }
};

/** Conversion of numeric to DateTime64
    */

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTime64TransformUnsigned
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier;

    ToDateTime64TransformUnsigned(UInt32 scale) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {
    }

    NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime64", from);
            else
                return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(from, 0, scale_multiplier);
        }
        else
            return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(
                std::min<time_t>(from, MAX_DATETIME64_TIMESTAMP), 0, scale_multiplier);
    }
};

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTime64TransformSigned
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier;

    ToDateTime64TransformSigned(UInt32 scale) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {
    }

    NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < MIN_DATETIME64_TIMESTAMP || from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime64", from);
        }
        from = static_cast<FromType>(std::max<time_t>(from, MIN_DATETIME64_TIMESTAMP));
        from = static_cast<FromType>(std::min<time_t>(from, MAX_DATETIME64_TIMESTAMP));

        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(from, 0, scale_multiplier);
    }
};

template <typename FromDataType, typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTime64TransformFloat
{
    static constexpr auto name = "toDateTime64";

    const UInt32 scale;

    ToDateTime64TransformFloat(UInt32 scale_) /// NOLINT
        : scale(scale_)
    {
    }

    NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < MIN_DATETIME64_TIMESTAMP || from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime64", from);
        }

        from = std::max(from, static_cast<FromType>(MIN_DATETIME64_TIMESTAMP));
        from = std::min(from, static_cast<FromType>(MAX_DATETIME64_TIMESTAMP));
        return convertToDecimal<FromDataType, DataTypeDateTime64>(from, scale);
    }
};

struct ToDateTime64Transform
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier;

    ToDateTime64Transform(UInt32 scale) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {
    }

    DateTime64::NativeType execute(UInt16 d, const DateLUTImpl & time_zone) const
    {
        const auto dt = ToDateTimeImpl<>::execute(d, time_zone);
        return execute(dt, time_zone);
    }

    DateTime64::NativeType execute(Int32 d, const DateLUTImpl & time_zone) const
    {
        Int64 dt = static_cast<Int64>(time_zone.fromDayNum(ExtendedDayNum(d)));
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(dt, 0, scale_multiplier);
    }

    DateTime64::NativeType execute(UInt32 dt, const DateLUTImpl & /*time_zone*/) const
    {
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(dt, 0, scale_multiplier);
    }
};


struct AccurateConvertStrategyAdditions
{
    UInt32 scale{0};
};

struct AccurateOrNullConvertStrategyAdditions
{
    UInt32 scale{0};
};

template <
    typename FromDataType,
    typename ToDataType,
    typename Name,
    ConvertFromStringExceptionMode exception_mode,
    ConvertFromStringParsingMode parsing_mode>
struct ConvertThroughParsing
{
    static_assert(
        std::is_same_v<FromDataType, DataTypeString> || std::is_same_v<FromDataType, DataTypeFixedString>,
        "ConvertThroughParsing is only applicable for String or FixedString data types");

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    static bool isAllRead(ReadBuffer & in)
    {
        /// In case of FixedString, skip zero bytes at end.
        if constexpr (std::is_same_v<FromDataType, DataTypeFixedString>)
            while (!in.eof() && *in.position() == 0)
                ++in.position();

        if (in.eof())
            return true;

        /// Special case, that allows to parse string with DateTime or DateTime64 as Date or Date32.
        if constexpr (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>)
        {
            if (!in.eof() && (*in.position() == ' ' || *in.position() == 'T'))
            {
                if (in.buffer().size() == strlen("YYYY-MM-DD hh:mm:ss"))
                    return true;

                if (in.buffer().size() >= strlen("YYYY-MM-DD hh:mm:ss.x") && in.buffer().begin()[19] == '.')
                {
                    in.position() = in.buffer().begin() + 20;

                    while (!in.eof() && isNumericASCII(*in.position()))
                        ++in.position();

                    if (in.eof())
                        return true;
                }
            }
        }

        return false;
    }

    template <typename Additions = void *>
    static ColumnPtr execute(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & res_type,
        size_t input_rows_count,
        Additions additions [[maybe_unused]] = Additions())
    {
        using ColVecTo = typename ToDataType::ColumnType;

        const DateLUTImpl * local_time_zone [[maybe_unused]] = nullptr;
        const DateLUTImpl * utc_time_zone [[maybe_unused]] = nullptr;

        /// For conversion to Date or DateTime type, second argument with time zone could be specified.
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime> || to_datetime64)
        {
            const auto result_type = removeNullable(res_type);
            // Time zone is already figured out during result type resolution, no need to do it here.
            if (const auto dt_col = checkAndGetDataType<ToDataType>(result_type.get()))
                local_time_zone = &dt_col->getTimeZone();
            else
                local_time_zone = &extractTimeZoneFromFunctionArguments(arguments, 1, 0);

            if constexpr (
                parsing_mode == ConvertFromStringParsingMode::BestEffort || parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                utc_time_zone = &DateLUT::instance("UTC");
        }
        else if constexpr (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>)
        {
            // Timezone is more or less dummy when parsing Date/Date32 from string.
            local_time_zone = &DateLUT::instance();
            utc_time_zone = &DateLUT::instance("UTC");
        }

        const IColumn * col_from = arguments[0].column.get();
        const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
        const ColumnFixedString * col_from_fixed_string = checkAndGetColumn<ColumnFixedString>(col_from);

        if (std::is_same_v<FromDataType, DataTypeString> && !col_from_string)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", col_from->getName(), Name::name);

        if (std::is_same_v<FromDataType, DataTypeFixedString> && !col_from_fixed_string)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", col_from->getName(), Name::name);

        size_t size = input_rows_count;
        typename ColVecTo::MutablePtr col_to = nullptr;

        if constexpr (IsDataTypeDecimal<ToDataType>)
        {
            UInt32 scale = additions;
            if constexpr (to_datetime64)
            {
                ToDataType check_bounds_in_ctor(scale, local_time_zone ? local_time_zone->getTimeZone() : String{});
            }
            else
            {
                ToDataType check_bounds_in_ctor(ToDataType::maxPrecision(), scale);
            }
            col_to = ColVecTo::create(size, scale);
        }
        else
            col_to = ColVecTo::create(size);

        typename ColVecTo::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        const ColumnString::Chars * chars = nullptr;
        const IColumn::Offsets * offsets = nullptr;
        size_t fixed_string_size = 0;

        if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            chars = &col_from_string->getChars();
            offsets = &col_from_string->getOffsets();
        }
        else
        {
            chars = &col_from_fixed_string->getChars();
            fixed_string_size = col_from_fixed_string->getN();
        }

        size_t current_offset = 0;

        bool precise_float_parsing = false;

        if (DB::CurrentThread::isInitialized())
        {
            const DB::ContextPtr query_context = DB::CurrentThread::get().getQueryContext();

            if (query_context)
                precise_float_parsing = query_context->getSettingsRef()[Setting::precise_float_parsing];
        }

        for (size_t i = 0; i < size; ++i)
        {
            size_t next_offset = std::is_same_v<FromDataType, DataTypeString> ? (*offsets)[i] : (current_offset + fixed_string_size);
            size_t string_size = std::is_same_v<FromDataType, DataTypeString> ? next_offset - current_offset - 1 : fixed_string_size;

            ReadBufferFromMemory read_buffer(chars->data() + current_offset, string_size);

            if constexpr (exception_mode == ConvertFromStringExceptionMode::Throw)
            {
                if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parseDateTime64BestEffort(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parseDateTimeBestEffort(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i], res);
                    }
                }
                else if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parseDateTime64BestEffortUS(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parseDateTimeBestEffortUS(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i], res);
                    }
                }
                else
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 value = 0;
                        readDateTime64Text(value, col_to->getScale(), read_buffer, *local_time_zone);
                        vec_to[i] = value;
                    }
                    else if constexpr (IsDataTypeDecimal<ToDataType>)
                    {
                        SerializationDecimal<typename ToDataType::FieldType>::readText(
                            vec_to[i], read_buffer, ToDataType::maxPrecision(), col_to->getScale());
                    }
                    else
                    {
                        /// we want to utilize constexpr condition here, which is not mixable with value comparison
                        do
                        {
                            if constexpr (std::is_same_v<FromDataType, DataTypeFixedString> && std::is_same_v<ToDataType, DataTypeIPv6>)
                            {
                                if (fixed_string_size == IPV6_BINARY_LENGTH)
                                {
                                    readBinary(vec_to[i], read_buffer);
                                    break;
                                }
                            }
                            if constexpr (std::is_same_v<Additions, AccurateConvertStrategyAdditions>)
                            {
                                if (!tryParseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone, precise_float_parsing))
                                    throw Exception(
                                        ErrorCodes::CANNOT_PARSE_TEXT,
                                        "Cannot parse string to type {}",
                                        TypeName<typename ToDataType::FieldType>);
                            }
                            else
                                parseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone, precise_float_parsing);
                        } while (false);
                    }
                }

                if (!isAllRead(read_buffer))
                    throwExceptionForIncompletelyParsedValue(read_buffer, *res_type);
            }
            else
            {
                bool parsed;

                if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parsed = tryParseDateTime64BestEffort(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parsed = tryParseDateTimeBestEffort(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i], res);
                    }
                }
                else if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parsed = tryParseDateTime64BestEffortUS(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parsed = tryParseDateTimeBestEffortUS(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i], res);
                    }
                }
                else
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 value = 0;
                        parsed = tryReadDateTime64Text(value, col_to->getScale(), read_buffer, *local_time_zone);
                        vec_to[i] = value;
                    }
                    else if constexpr (IsDataTypeDecimal<ToDataType>)
                    {
                        parsed = SerializationDecimal<typename ToDataType::FieldType>::tryReadText(
                            vec_to[i], read_buffer, ToDataType::maxPrecision(), col_to->getScale());
                    }
                    else if (
                        std::is_same_v<FromDataType, DataTypeFixedString> && std::is_same_v<ToDataType, DataTypeIPv6>
                        && fixed_string_size == IPV6_BINARY_LENGTH)
                    {
                        readBinary(vec_to[i], read_buffer);
                        parsed = true;
                    }
                    else
                    {
                        parsed = tryParseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone, precise_float_parsing);
                    }
                }

                if (!isAllRead(read_buffer))
                    parsed = false;

                if (!parsed)
                {
                    if constexpr (std::is_same_v<ToDataType, DataTypeDate32>)
                    {
                        vec_to[i] = -static_cast<Int32>(
                            DateLUT::instance().getDayNumOffsetEpoch()); /// NOLINT(readability-static-accessed-through-instance)
                    }
                    else
                    {
                        vec_to[i] = static_cast<typename ToDataType::FieldType>(0);
                    }
                }

                if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
                    (*vec_null_map_to)[i] = !parsed;
            }

            current_offset = next_offset;
        }

        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
            return ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            return col_to;
    }
};

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <
    typename FromDataType,
    typename ToDataType,
    typename Name,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior>
struct ConvertImpl
{
    template <typename Additions = void *>
    static ColumnPtr NO_SANITIZE_UNDEFINED execute(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type [[maybe_unused]],
        size_t input_rows_count,
        BehaviourOnErrorFromString from_string_tag [[maybe_unused]],
        Additions additions = Additions())
    {
        const ColumnWithTypeAndName & named_from = arguments[0];

        if constexpr (
            (std::is_same_v<FromDataType, ToDataType> && !FromDataType::is_parametric)
            || (std::is_same_v<FromDataType, DataTypeEnum8> && std::is_same_v<ToDataType, DataTypeInt8>)
            || (std::is_same_v<FromDataType, DataTypeEnum16> && std::is_same_v<ToDataType, DataTypeInt16>))
        {
            /// If types are the same, reuse the columns.
            /// Conversions between Enum and the underlying type are also free.
            return named_from.column;
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeDateTime> || std::is_same_v<FromDataType, DataTypeDate32>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            /// Conversion of DateTime to Date: throw off time component.
            /// Conversion of Date32 to Date.
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateImpl<date_time_overflow_behavior>, false>::template execute<
                Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime> && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            /// Conversion of DateTime to Date: throw off time component.
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDate32Impl, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeDate> || std::is_same_v<FromDataType, DataTypeDate32>)
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            /// Conversion from Date/Date32 to DateTime.
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTimeImpl<date_time_overflow_behavior>, false>::template execute<
                Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64> && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, TransformDateTime64<ToDate32Impl>, false>::template execute<
                Additions>(arguments, result_type, input_rows_count, additions);
        }
        /** Special case of converting Int8, Int16, (U)Int32 or (U)Int64 (and also, for convenience,
        * Float32, Float64) to Date. If the
        * number is less than 65536, then it is treated as DayNum, and if it's greater or equals to 65536,
        * then treated as unix timestamp. If the number exceeds UInt32, saturate to MAX_UINT32 then as DayNum.
        * It's a bit illogical, as we actually have two functions in one.
        * But allows to support frequent case,
        *  when user write toDate(UInt32), expecting conversion of unix timestamp to Date.
        *  (otherwise such usage would be frequent mistake).
        */
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeUInt32> || std::is_same_v<FromDataType, DataTypeUInt64>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTransform32Or64<typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt8> || std::is_same_v<FromDataType, DataTypeInt16>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTransform8Or16Signed<typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt32> || std::is_same_v<FromDataType, DataTypeInt64>
             || std::is_same_v<FromDataType, DataTypeFloat32> || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTransform32Or64Signed<typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeUInt32> || std::is_same_v<FromDataType, DataTypeUInt64>)
            && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDate32Transform32Or64<typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt8> || std::is_same_v<FromDataType, DataTypeInt16>)
            && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDate32Transform8Or16Signed<typename FromDataType::FieldType>, false>::
                template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt32> || std::is_same_v<FromDataType, DataTypeInt64>
             || std::is_same_v<FromDataType, DataTypeFloat32> || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDate32Transform32Or64Signed<typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        /// Special case of converting Int8, Int16, Int32 or (U)Int64 (and also, for convenience, Float32, Float64) to DateTime.
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt8> || std::is_same_v<FromDataType, DataTypeInt16>
             || std::is_same_v<FromDataType, DataTypeInt32>)
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTimeTransformSigned<typename FromDataType::FieldType, UInt32, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeUInt64> && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTimeTransform64<typename FromDataType::FieldType, UInt32, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt64> || std::is_same_v<FromDataType, DataTypeFloat32>
             || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTimeTransform64Signed<typename FromDataType::FieldType, UInt32, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeInt8> || std::is_same_v<FromDataType, DataTypeInt16>
             || std::is_same_v<FromDataType, DataTypeInt32> || std::is_same_v<FromDataType, DataTypeInt64>)
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTime64TransformSigned<typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeUInt64> && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTime64TransformUnsigned<UInt64, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeFloat32> || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                ToDateTime64TransformFloat<FromDataType, typename FromDataType::FieldType, default_date_time_overflow_behavior>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count, additions);
        }
        /// Conversion of DateTime64 to Date or DateTime: discards fractional part.
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64> && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, TransformDateTime64<ToDateImpl<date_time_overflow_behavior>>, false>::
                template execute<Additions>(arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64> && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<
                FromDataType,
                ToDataType,
                TransformDateTime64<ToDateTimeImpl<date_time_overflow_behavior>>,
                false>::template execute<Additions>(arguments, result_type, input_rows_count, additions);
        }
        /// Conversion of Date or DateTime to DateTime64: add zero sub-second part.
        else if constexpr (
            (std::is_same_v<FromDataType, DataTypeDate> || std::is_same_v<FromDataType, DataTypeDate32>
             || std::is_same_v<FromDataType, DataTypeDateTime>)
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTime64Transform, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (IsDataTypeDateOrDateTime<FromDataType> && std::is_same_v<ToDataType, DataTypeString>)
        {
            /// Date or DateTime to String

            using FromFieldType = typename FromDataType::FieldType;
            using ColVecType = ColumnVectorOrDecimal<FromFieldType>;

            auto datetime_arg = arguments[0];

            const DateLUTImpl * time_zone = nullptr;
            const ColumnConst * time_zone_column = nullptr;

            if (arguments.size() == 1)
            {
                auto non_null_args = createBlockWithNestedColumns(arguments);
                time_zone = &extractTimeZoneFromFunctionArguments(non_null_args, 1, 0);
            }
            else /// When we have a column for timezone
            {
                datetime_arg.column = datetime_arg.column->convertToFullColumnIfConst();

                if constexpr (std::is_same_v<FromDataType, DataTypeDate> || std::is_same_v<FromDataType, DataTypeDate32>)
                    time_zone = &DateLUT::instance();
                /// For argument of Date or DateTime type, second argument with time zone could be specified.
                if constexpr (std::is_same_v<FromDataType, DataTypeDateTime> || std::is_same_v<FromDataType, DataTypeDateTime64>)
                {
                    if ((time_zone_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get())))
                    {
                        auto non_null_args = createBlockWithNestedColumns(arguments);
                        time_zone = &extractTimeZoneFromFunctionArguments(non_null_args, 1, 0);
                    }
                }
            }
            const auto & col_with_type_and_name = columnGetNested(datetime_arg);

            if (const auto col_from = checkAndGetColumn<ColVecType>(col_with_type_and_name.column.get()))
            {
                auto col_to = ColumnString::create();

                const typename ColVecType::Container & vec_from = col_from->getData();
                ColumnString::Chars & data_to = col_to->getChars();
                ColumnString::Offsets & offsets_to = col_to->getOffsets();
                size_t size = vec_from.size();

                if constexpr (std::is_same_v<FromDataType, DataTypeDate>)
                    data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
                else if constexpr (std::is_same_v<FromDataType, DataTypeDate32>)
                    data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
                else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime>)
                    data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss") + 1));
                else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
                    data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss.") + col_from->getScale() + 1));
                else
                    data_to.resize(size * 3); /// Arbitrary

                offsets_to.resize(size);

                WriteBufferFromVector<ColumnString::Chars> write_buffer(data_to);
                const FromDataType & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

                ColumnUInt8::MutablePtr null_map = copyNullMap(datetime_arg.column);

                bool cut_trailing_zeros_align_to_groups_of_thousands = false;
                if (DB::CurrentThread::isInitialized())
                {
                    const DB::ContextPtr query_context = DB::CurrentThread::get().getQueryContext();

                    if (query_context)
                        cut_trailing_zeros_align_to_groups_of_thousands
                            = query_context
                                  ->getSettingsRef()[Setting::date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands];
                }

                if (!null_map && arguments.size() > 1)
                    null_map = copyNullMap(arguments[1].column->convertToFullColumnIfConst());

                if (null_map)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (!time_zone_column && arguments.size() > 1)
                        {
                            if (!arguments[1].column.get()->getDataAt(i).toString().empty())
                                time_zone = &DateLUT::instance(arguments[1].column.get()->getDataAt(i).toString());
                            else
                                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Provided time zone must be non-empty");
                        }
                        bool is_ok = true;
                        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
                        {
                            if (cut_trailing_zeros_align_to_groups_of_thousands)
                                writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(
                                    DateTime64(vec_from[i]), type.getScale(), write_buffer, *time_zone);
                            else
                                is_ok = FormatImpl<FromDataType>::template execute<bool>(vec_from[i], write_buffer, &type, time_zone);
                        }
                        else
                        {
                            is_ok = FormatImpl<FromDataType>::template execute<bool>(vec_from[i], write_buffer, &type, time_zone);
                        }
                        null_map->getData()[i] |= !is_ok;
                        writeChar(0, write_buffer);
                        offsets_to[i] = write_buffer.count();
                    }
                }
                else
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (!time_zone_column && arguments.size() > 1)
                        {
                            if (!arguments[1].column.get()->getDataAt(i).toString().empty())
                                time_zone = &DateLUT::instance(arguments[1].column.get()->getDataAt(i).toString());
                            else
                                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Provided time zone must be non-empty");
                        }
                        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
                        {
                            if (cut_trailing_zeros_align_to_groups_of_thousands)
                                writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(
                                    DateTime64(vec_from[i]), type.getScale(), write_buffer, *time_zone);
                            else
                                FormatImpl<FromDataType>::template execute<bool>(vec_from[i], write_buffer, &type, time_zone);
                        }
                        else
                        {
                            FormatImpl<FromDataType>::template execute<bool>(vec_from[i], write_buffer, &type, time_zone);
                        }
                        writeChar(0, write_buffer);
                        offsets_to[i] = write_buffer.count();
                    }
                }

                write_buffer.finalize();

                if (null_map)
                    return ColumnNullable::create(std::move(col_to), std::move(null_map));
                return col_to;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    Name::name);
        }
        /// Conversion from FixedString to String.
        /// Cutting sequences of zero bytes from end of strings.
        else if constexpr (std::is_same_v<ToDataType, DataTypeString> && std::is_same_v<FromDataType, DataTypeFixedString>)
        {
            ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);
            const auto & nested = columnGetNested(arguments[0]);
            if (const ColumnFixedString * col_from = checkAndGetColumn<ColumnFixedString>(nested.column.get()))
            {
                auto col_to = ColumnString::create();

                const ColumnFixedString::Chars & data_from = col_from->getChars();
                ColumnString::Chars & data_to = col_to->getChars();
                ColumnString::Offsets & offsets_to = col_to->getOffsets();
                size_t size = col_from->size();
                size_t n = col_from->getN();
                data_to.resize(size * (n + 1)); /// + 1 - zero terminator
                offsets_to.resize(size);

                size_t offset_from = 0;
                size_t offset_to = 0;
                for (size_t i = 0; i < size; ++i)
                {
                    if (!null_map || !null_map->getData()[i])
                    {
                        size_t bytes_to_copy = n;
                        while (bytes_to_copy > 0 && data_from[offset_from + bytes_to_copy - 1] == 0)
                            --bytes_to_copy;

                        memcpy(&data_to[offset_to], &data_from[offset_from], bytes_to_copy);
                        offset_to += bytes_to_copy;
                    }
                    data_to[offset_to] = 0;
                    ++offset_to;
                    offsets_to[i] = offset_to;
                    offset_from += n;
                }

                data_to.resize(offset_to);
                if (result_type->isNullable() && null_map)
                    return ColumnNullable::create(std::move(col_to), std::move(null_map));
                return col_to;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    Name::name);
        }
        else if constexpr (std::is_same_v<ToDataType, DataTypeString>)
        {
            /// Anything else to String.

            using FromFieldType = typename FromDataType::FieldType;
            using ColVecType = ColumnVectorOrDecimal<FromFieldType>;

            ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);

            const auto & col_with_type_and_name = columnGetNested(arguments[0]);
            const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

            if (const auto col_from = checkAndGetColumn<ColVecType>(col_with_type_and_name.column.get()))
            {
                auto col_to = ColumnString::create();

                const typename ColVecType::Container & vec_from = col_from->getData();
                ColumnString::Chars & data_to = col_to->getChars();
                ColumnString::Offsets & offsets_to = col_to->getOffsets();
                size_t size = vec_from.size();

                data_to.resize(size * 3);
                offsets_to.resize(size);

                WriteBufferFromVector<ColumnString::Chars> write_buffer(data_to);

                if (null_map)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        bool is_ok = FormatImpl<FromDataType>::template execute<bool>(vec_from[i], write_buffer, &type, nullptr);
                        /// We don't use timezones in this branch
                        null_map->getData()[i] |= !is_ok;
                        writeChar(0, write_buffer);
                        offsets_to[i] = write_buffer.count();
                    }
                }
                else
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        FormatImpl<FromDataType>::template execute<void>(vec_from[i], write_buffer, &type, nullptr);
                        writeChar(0, write_buffer);
                        offsets_to[i] = write_buffer.count();
                    }
                }

                write_buffer.finalize();

                if (null_map)
                    return ColumnNullable::create(std::move(col_to), std::move(null_map));
                return col_to;
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    arguments[0].column->getName(),
                    Name::name);
        }
        else if constexpr (
            std::is_same_v<Name, NameToUnixTimestamp> && std::is_same_v<FromDataType, DataTypeString>
            && std::is_same_v<ToDataType, DataTypeUInt32>)
        {
            return ConvertImpl<FromDataType, DataTypeDateTime, Name, date_time_overflow_behavior>::template execute<Additions>(
                arguments, result_type, input_rows_count, from_string_tag);
        }
        else if constexpr ((std::is_same_v<FromDataType, DataTypeString> || std::is_same_v<FromDataType, DataTypeFixedString>))
        {
            switch (from_string_tag)
            {
                case BehaviourOnErrorFromString::ConvertDefaultBehaviorTag:
                    return ConvertThroughParsing<
                        FromDataType,
                        ToDataType,
                        Name,
                        ConvertFromStringExceptionMode::Throw,
                        ConvertFromStringParsingMode::Normal>::execute(arguments, result_type, input_rows_count, additions);
                case BehaviourOnErrorFromString::ConvertReturnNullOnErrorTag:
                    return ConvertThroughParsing<
                        FromDataType,
                        ToDataType,
                        Name,
                        ConvertFromStringExceptionMode::Null,
                        ConvertFromStringParsingMode::Normal>::execute(arguments, result_type, input_rows_count, additions);
                case BehaviourOnErrorFromString::ConvertReturnZeroOnErrorTag:
                    return ConvertThroughParsing<
                        FromDataType,
                        ToDataType,
                        Name,
                        ConvertFromStringExceptionMode::Zero,
                        ConvertFromStringParsingMode::Normal>::execute(arguments, result_type, input_rows_count, additions);
            }
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeInterval> && std::is_same_v<ToDataType, DataTypeInterval>)
        {
            IntervalKind to = typeid_cast<const DataTypeInterval *>(result_type.get())->getKind();
            IntervalKind from = typeid_cast<const DataTypeInterval *>(arguments[0].type.get())->getKind();

            if (from == to || arguments[0].column->empty())
                return arguments[0].column;

            Int64 conversion_factor = 1;
            Int64 result_value;

            int from_position = static_cast<int>(from.kind);
            int to_position = static_cast<int>(to.kind); /// Positions of each interval according to granularity map

            if (from_position < to_position)
            {
                for (int i = from_position; i < to_position; ++i)
                    conversion_factor *= interval_conversions[i];
                result_value = arguments[0].column->getInt(0) / conversion_factor;
            }
            else
            {
                for (int i = from_position; i > to_position; --i)
                    conversion_factor *= interval_conversions[i];
                result_value = arguments[0].column->getInt(0) * conversion_factor;
            }

            return ColumnConst::create(ColumnInt64::create(1, result_value), input_rows_count);
        }
        else
        {
            using FromFieldType = typename FromDataType::FieldType;
            using ToFieldType = typename ToDataType::FieldType;
            using ColVecFrom = typename FromDataType::ColumnType;
            using ColVecTo = typename ToDataType::ColumnType;

            if constexpr (
                (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
                && !(std::is_same_v<DataTypeDateTime64, FromDataType> || std::is_same_v<DataTypeDateTime64, ToDataType>)
                && (!IsDataTypeDecimalOrNumber<FromDataType> || !IsDataTypeDecimalOrNumber<ToDataType>))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {}/{} of first argument of function {}",
                    named_from.column->getName(),
                    typeid(FromDataType).name(),
                    Name::name);
            }

            const ColVecFrom * col_from = checkAndGetColumn<ColVecFrom>(named_from.column.get());
            if (!col_from)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}",
                    named_from.column->getName(),
                    Name::name);

            typename ColVecTo::MutablePtr col_to = nullptr;

            if constexpr (IsDataTypeDecimal<ToDataType>)
            {
                UInt32 scale;

                if constexpr (
                    std::is_same_v<Additions, AccurateConvertStrategyAdditions>
                    || std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                {
                    scale = additions.scale;
                }
                else
                {
                    scale = additions;
                }

                col_to = ColVecTo::create(0, scale);
            }
            else
                col_to = ColVecTo::create();

            const auto & vec_from = col_from->getData();
            auto & vec_to = col_to->getData();
            vec_to.resize(input_rows_count);

            ColumnUInt8::MutablePtr col_null_map_to;
            ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
            if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                vec_null_map_to = &col_null_map_to->getData();
            }

            bool result_is_bool = isBool(result_type);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if constexpr (std::is_same_v<ToDataType, DataTypeUInt8>)
                {
                    if (result_is_bool)
                    {
                        vec_to[i] = vec_from[i] != FromFieldType(0);
                        continue;
                    }
                }

                if constexpr (std::is_same_v<FromDataType, DataTypeUUID> && std::is_same_v<ToDataType, DataTypeUInt128>)
                {
                    static_assert(
                        std::is_same_v<DataTypeUInt128::FieldType, DataTypeUUID::FieldType::UnderlyingType>,
                        "UInt128 and UUID types must be same");

                    vec_to[i].items[1] = vec_from[i].toUnderType().items[0];
                    vec_to[i].items[0] = vec_from[i].toUnderType().items[1];
                }
                else if constexpr (std::is_same_v<FromDataType, DataTypeIPv6> && std::is_same_v<ToDataType, DataTypeUInt128>)
                {
                    static_assert(
                        std::is_same_v<DataTypeUInt128::FieldType, DataTypeIPv6::FieldType::UnderlyingType>,
                        "UInt128 and IPv6 types must be same");

                    vec_to[i].items[1] = std::byteswap(vec_from[i].toUnderType().items[0]);
                    vec_to[i].items[0] = std::byteswap(vec_from[i].toUnderType().items[1]);
                }
                else if constexpr (std::is_same_v<FromDataType, DataTypeUInt128> && std::is_same_v<ToDataType, DataTypeIPv6>)
                {
                    static_assert(
                        std::is_same_v<DataTypeUInt128::FieldType, DataTypeIPv6::FieldType::UnderlyingType>,
                        "IPv6 and UInt128 types must be same");

                    vec_to[i].toUnderType().items[1] = std::byteswap(vec_from[i].items[0]);
                    vec_to[i].toUnderType().items[0] = std::byteswap(vec_from[i].items[1]);
                }
                else if constexpr (std::is_same_v<FromDataType, DataTypeUUID> != std::is_same_v<ToDataType, DataTypeUUID>)
                {
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Conversion between numeric types and UUID is not supported. "
                        "Probably the passed UUID is unquoted");
                }
                else if constexpr (
                    (std::is_same_v<FromDataType, DataTypeIPv4> != std::is_same_v<ToDataType, DataTypeIPv4>)
                    && !(
                        is_any_of<FromDataType, DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeIPv6>
                        || is_any_of<ToDataType, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256, DataTypeIPv6>))
                {
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Conversion from {} to {} is not supported",
                        TypeName<typename FromDataType::FieldType>,
                        TypeName<typename ToDataType::FieldType>);
                }
                else if constexpr (
                    std::is_same_v<FromDataType, DataTypeIPv6> != std::is_same_v<ToDataType, DataTypeIPv6>
                    && !(std::is_same_v<ToDataType, DataTypeIPv4> || std::is_same_v<FromDataType, DataTypeIPv4>))
                {
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Conversion between numeric types and IPv6 is not supported. "
                        "Probably the passed IPv6 is unquoted");
                }
                else if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
                {
                    if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                    {
                        ToFieldType result;
                        bool convert_result = false;

                        if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                            convert_result = tryConvertDecimals<FromDataType, ToDataType>(
                                vec_from[i], col_from->getScale(), col_to->getScale(), result);
                        else if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>)
                            convert_result = tryConvertFromDecimal<FromDataType, ToDataType>(vec_from[i], col_from->getScale(), result);
                        else if constexpr (IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>)
                            convert_result = tryConvertToDecimal<FromDataType, ToDataType>(vec_from[i], col_to->getScale(), result);

                        if (convert_result)
                            vec_to[i] = result;
                        else
                        {
                            vec_to[i] = static_cast<ToFieldType>(0);
                            (*vec_null_map_to)[i] = true;
                        }
                    }
                    else
                    {
                        if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                            vec_to[i] = convertDecimals<FromDataType, ToDataType>(vec_from[i], col_from->getScale(), col_to->getScale());
                        else if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>)
                            vec_to[i] = convertFromDecimal<FromDataType, ToDataType>(vec_from[i], col_from->getScale());
                        else if constexpr (IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>)
                            vec_to[i] = convertToDecimal<FromDataType, ToDataType>(vec_from[i], col_to->getScale());
                        else
                            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Unsupported data type in conversion function");
                    }
                }
                else if constexpr (std::is_same_v<ToDataType, DataTypeIPv4> && std::is_same_v<FromDataType, DataTypeIPv6>)
                {
                    const uint8_t ip4_cidr[]{
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00};
                    const uint8_t * src = reinterpret_cast<const uint8_t *>(&vec_from[i].toUnderType());
                    if (!matchIPv6Subnet(src, ip4_cidr, 96))
                    {
                        char addr[IPV6_MAX_TEXT_LENGTH + 1]{};
                        char * paddr = addr;
                        formatIPv6(src, paddr);

                        throw Exception(
                            ErrorCodes::CANNOT_CONVERT_TYPE,
                            "IPv6 {} in column {} is not in IPv4 mapping block",
                            addr,
                            named_from.column->getName());
                    }

                    uint8_t * dst = reinterpret_cast<uint8_t *>(&vec_to[i].toUnderType());
                    if constexpr (std::endian::native == std::endian::little)
                    {
                        dst[0] = src[15];
                        dst[1] = src[14];
                        dst[2] = src[13];
                        dst[3] = src[12];
                    }
                    else
                    {
                        dst[0] = src[12];
                        dst[1] = src[13];
                        dst[2] = src[14];
                        dst[3] = src[15];
                    }
                }
                else if constexpr (std::is_same_v<ToDataType, DataTypeIPv6> && std::is_same_v<FromDataType, DataTypeIPv4>)
                {
                    const uint8_t * src = reinterpret_cast<const uint8_t *>(&vec_from[i].toUnderType());
                    uint8_t * dst = reinterpret_cast<uint8_t *>(&vec_to[i].toUnderType());
                    std::memset(dst, '\0', IPV6_BINARY_LENGTH);
                    dst[10] = dst[11] = 0xff;

                    if constexpr (std::endian::native == std::endian::little)
                    {
                        dst[12] = src[3];
                        dst[13] = src[2];
                        dst[14] = src[1];
                        dst[15] = src[0];
                    }
                    else
                    {
                        dst[12] = src[0];
                        dst[13] = src[1];
                        dst[14] = src[2];
                        dst[15] = src[3];
                    }
                }
                else if constexpr (std::is_same_v<ToDataType, DataTypeIPv4> && std::is_same_v<FromDataType, DataTypeUInt64>)
                {
                    vec_to[i] = static_cast<ToFieldType>(static_cast<IPv4::UnderlyingType>(vec_from[i]));
                }
                else if constexpr (
                    std::is_same_v<Name, NameToUnixTimestamp>
                    && (std::is_same_v<FromDataType, DataTypeDate> || std::is_same_v<FromDataType, DataTypeDate32>))
                {
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i] * DATE_SECONDS_PER_DAY);
                }
                else
                {
                    /// If From Data is Nan or Inf and we convert to integer type, throw exception
                    if constexpr (is_floating_point<FromFieldType> && !is_floating_point<ToFieldType>)
                    {
                        if (!isFinite(vec_from[i]))
                        {
                            if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                            {
                                vec_to[i] = 0;
                                (*vec_null_map_to)[i] = true;
                                continue;
                            }
                            else
                                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Unexpected inf or nan to integer conversion");
                        }
                    }

                    if constexpr (
                        std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>
                        || std::is_same_v<Additions, AccurateConvertStrategyAdditions>)
                    {
                        bool convert_result = accurate::convertNumeric(vec_from[i], vec_to[i]);

                        if (!convert_result)
                        {
                            if (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                            {
                                vec_to[i] = 0;
                                (*vec_null_map_to)[i] = true;
                            }
                            else
                            {
                                throw Exception(
                                    ErrorCodes::CANNOT_CONVERT_TYPE,
                                    "Value in column {} cannot be safely converted into type {}",
                                    named_from.column->getName(),
                                    result_type->getName());
                            }
                        }
                    }
                    else
                    {
                        vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                    }
                }
            }

            if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                return ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                return col_to;
        }
    }
};


/// Generic conversion of any type to String or FixedString via serialization to text.
template <typename StringColumnType>
struct ConvertImplGenericToString
{
    static ColumnPtr execute(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/, const ContextPtr & context)
    {
        static_assert(
            std::is_same_v<StringColumnType, ColumnString> || std::is_same_v<StringColumnType, ColumnFixedString>,
            "Can be used only to serialize to ColumnString or ColumnFixedString");

        ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);

        const auto & col_with_type_and_name = columnGetNested(arguments[0]);
        const IDataType & type = *col_with_type_and_name.type;
        const IColumn & col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();
        auto col_to = removeNullable(result_type)->createColumn();

        {
            ColumnStringHelpers::WriteHelper write_helper(assert_cast<StringColumnType &>(*col_to), size);

            auto & write_buffer = write_helper.getWriteBuffer();

            FormatSettings format_settings = context ? getFormatSettings(context) : FormatSettings{};
            auto serialization = type.getDefaultSerialization();
            for (size_t row = 0; row < size; ++row)
            {
                serialization->serializeText(col_from, row, write_buffer, format_settings);
                write_helper.rowWritten();
            }

            write_helper.finalize();
        }

        if (result_type->isNullable() && null_map)
            return ColumnNullable::create(std::move(col_to), std::move(null_map));
        return col_to;
    }
};


/// Generic conversion of any type from String. Used for complex types: Array and Tuple or types with custom serialization.
template <bool throw_on_error>
struct ConvertImplGenericFromString
{
    static ColumnPtr execute(
        ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const ColumnNullable * column_nullable,
        size_t input_rows_count,
        const ContextPtr & context)
    {
        const IColumn & column_from = *arguments[0].column;
        const IDataType & data_type_to = *result_type;
        auto res = data_type_to.createColumn();
        auto serialization = data_type_to.getDefaultSerialization();
        const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;

        executeImpl(column_from, *res, *serialization, input_rows_count, null_map, result_type.get(), context);
        return res;
    }

    static void executeImpl(
        const IColumn & column_from,
        IColumn & column_to,
        const ISerialization & serialization_from,
        size_t input_rows_count,
        const PaddedPODArray<UInt8> * null_map,
        const IDataType * result_type,
        const ContextPtr & context)
    {
        column_to.reserve(input_rows_count);

        FormatSettings format_settings = context ? getFormatSettings(context) : FormatSettings{};
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                column_to.insertDefault();
                continue;
            }

            const auto & val = column_from.getDataAt(i);
            ReadBufferFromMemory read_buffer(val.data, val.size);
            try
            {
                serialization_from.deserializeWholeText(column_to, read_buffer, format_settings);
            }
            catch (const Exception &)
            {
                if constexpr (throw_on_error)
                    throw;
                /// Check if exception happened after we inserted the value
                /// (deserializeWholeText should not do it, but let's check anyway).
                if (column_to.size() > i)
                    column_to.popBack(column_to.size() - i);
                column_to.insertDefault();
            }

            /// Usually deserializeWholeText checks for eof after parsing, but let's check one more time just in case.
            if (!read_buffer.eof())
            {
                if constexpr (throw_on_error)
                {
                    if (result_type)
                        throwExceptionForIncompletelyParsedValue(read_buffer, *result_type);
                    else
                        throw Exception(
                            ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse string to column {}. Expected eof", column_to.getName());
                }
                else
                {
                    if (column_to.size() > i)
                        column_to.popBack(column_to.size() - i);
                    column_to.insertDefault();
                }
            }
        }
    }
};


struct ConvertImplFromDynamicToColumn
{
    static ColumnPtr execute(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count,
        const std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr)> & nested_convert)
    {
        /// When casting Dynamic to regular column we should cast all variants from current Dynamic column
        /// and construct the result based on discriminators.
        const auto & column_dynamic = assert_cast<const ColumnDynamic &>(*arguments.front().column.get());
        const auto & variant_column = column_dynamic.getVariantColumn();
        const auto & variant_info = column_dynamic.getVariantInfo();

        /// First, cast usual variants to result type.
        const auto & variant_types = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
        std::vector<ColumnPtr> cast_variant_columns(variant_types.size());
        for (size_t i = 0; i != variant_types.size(); ++i)
        {
            /// Skip shared variant, it will be processed later.
            if (i == column_dynamic.getSharedVariantDiscriminator())
                continue;

            ColumnsWithTypeAndName new_args = arguments;
            new_args[0] = {variant_column.getVariantPtrByGlobalDiscriminator(i), variant_types[i], ""};
            cast_variant_columns[i] = nested_convert(new_args, result_type);
        }

        /// Second, collect all variants stored in shared variant and cast them to result type.
        std::vector<MutableColumnPtr> variant_columns_from_shared_variant;
        DataTypes variant_types_from_shared_variant;
        /// We will need to know what variant to use when we see discriminator of a shared variant.
        /// To do it, we remember what variant was extracted from each row and what was it's offset.
        PaddedPODArray<UInt64> shared_variant_indexes;
        PaddedPODArray<UInt64> shared_variant_offsets;
        std::unordered_map<String, UInt64> shared_variant_to_index;
        const auto & shared_variant = column_dynamic.getSharedVariant();
        const auto shared_variant_discr = column_dynamic.getSharedVariantDiscriminator();
        const auto & local_discriminators = variant_column.getLocalDiscriminators();
        const auto & offsets = variant_column.getOffsets();
        if (!shared_variant.empty())
        {
            shared_variant_indexes.reserve(input_rows_count);
            shared_variant_offsets.reserve(input_rows_count);
            FormatSettings format_settings;
            const auto shared_variant_local_discr = variant_column.localDiscriminatorByGlobal(shared_variant_discr);
            for (size_t i = 0; i != input_rows_count; ++i)
            {
                if (local_discriminators[i] == shared_variant_local_discr)
                {
                    auto value = shared_variant.getDataAt(offsets[i]);
                    ReadBufferFromMemory buf(value.data, value.size);
                    auto type = decodeDataType(buf);
                    auto type_name = type->getName();
                    auto it = shared_variant_to_index.find(type_name);
                    /// Check if we didn't create column for this variant yet.
                    if (it == shared_variant_to_index.end())
                    {
                        it = shared_variant_to_index.emplace(type_name, variant_columns_from_shared_variant.size()).first;
                        variant_columns_from_shared_variant.push_back(type->createColumn());
                        variant_types_from_shared_variant.push_back(type);
                    }

                    shared_variant_indexes.push_back(it->second);
                    shared_variant_offsets.push_back(variant_columns_from_shared_variant[it->second]->size());
                    type->getDefaultSerialization()->deserializeBinary(
                        *variant_columns_from_shared_variant[it->second], buf, format_settings);
                }
                else
                {
                    shared_variant_indexes.emplace_back();
                    shared_variant_offsets.emplace_back();
                }
            }
        }

        /// Cast all extracted variants into result type.
        std::vector<ColumnPtr> cast_shared_variant_columns(variant_types_from_shared_variant.size());
        for (size_t i = 0; i != variant_types_from_shared_variant.size(); ++i)
        {
            ColumnsWithTypeAndName new_args = arguments;
            new_args[0] = {variant_columns_from_shared_variant[i]->getPtr(), variant_types_from_shared_variant[i], ""};
            cast_shared_variant_columns[i] = nested_convert(new_args, result_type);
        }

        /// Construct result column from all cast variants.
        auto res = result_type->createColumn();
        res->reserve(input_rows_count);
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            auto global_discr = variant_column.globalDiscriminatorByLocal(local_discriminators[i]);
            if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
            {
                res->insertDefault();
            }
            else if (global_discr == shared_variant_discr)
            {
                if (cast_shared_variant_columns[shared_variant_indexes[i]])
                    res->insertFrom(*cast_shared_variant_columns[shared_variant_indexes[i]], shared_variant_offsets[i]);
                else
                    res->insertDefault();
            }
            else
            {
                if (cast_variant_columns[global_discr])
                    res->insertFrom(*cast_variant_columns[global_discr], offsets[i]);
                else
                    res->insertDefault();
            }
        }

        return res;
    }
};

}
}
