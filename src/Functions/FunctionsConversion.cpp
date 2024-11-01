#include <type_traits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObjectDeprecated.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnsCommon.h>
#include <Core/AccurateComparison.h>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <DataTypes/getLeastSupertype.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsCodingIP.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/TransformDateTime64.h>
#include <Functions/castTypeToEither.h>
#include <Functions/toFixedString.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Interpreters/Context.h>
#include <Common/Concepts.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/IPv6ToBinary.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool cast_ipv4_ipv6_default_on_conversion_error;
    extern const SettingsBool cast_string_to_dynamic_use_inference;
    extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
    extern const SettingsBool input_format_ipv4_default_on_conversion_error;
    extern const SettingsBool input_format_ipv6_default_on_conversion_error;
    extern const SettingsBool precise_float_parsing;
    extern const SettingsBool date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands;
}

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

/** Type conversion functions.
  * toType - conversion in "natural way";
  */

UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column)
{
    const auto * arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type)
        || checkAndGetDataType<DataTypeUInt32>(arg_type)
        || checkAndGetDataType<DataTypeUInt16>(arg_type)
        || checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of toDecimal() scale {}", named_column.type->getName());

    Field field;
    named_column.column->get(0, field);
    return static_cast<UInt32>(field.safeGet<UInt32>());
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

    static UInt32 execute(UInt32 dt, const DateLUTImpl & /*time_zone*/)
    {
        return dt;
    }

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
        return (from <= DATE_LUT_MAX_DAY_NUM)
            ? static_cast<UInt16>(from)
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
                    throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type Date32", from);
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

        return (from < DATE_LUT_MAX_EXTEND_DAY_NUM)
            ? static_cast<Int32>(from)
            : time_zone.toDayNum(std::min(time_t(Int64(from)), time_t(MAX_DATETIME64_TIMESTAMP)));
    }
};

template <typename FromType>
struct ToDate32Transform8Or16Signed
{
    static constexpr auto name = "toDate32";

    static NO_SANITIZE_UNDEFINED Int32 execute(const FromType & from, const DateLUTImpl &)
    {
        return from;
    }
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
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime", from);
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
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime", from);
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
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime", from);
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
    {}

    NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime64", from);
            else
                return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(from, 0, scale_multiplier);
        }
        else
            return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(std::min<time_t>(from, MAX_DATETIME64_TIMESTAMP), 0, scale_multiplier);
    }
};

template <typename FromType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct ToDateTime64TransformSigned
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier;

    ToDateTime64TransformSigned(UInt32 scale) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < MIN_DATETIME64_TIMESTAMP || from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime64", from);
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
    {}

    NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (from < MIN_DATETIME64_TIMESTAMP || from > MAX_DATETIME64_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type DateTime64", from);
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
    {}

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

/** Transformation of numbers, dates, datetimes to strings: through formatting.
  */
template <typename DataType>
struct FormatImpl
{
    template <typename ReturnType = void>
    static ReturnType execute(const typename DataType::FieldType x, WriteBuffer & wb, const DataType *, const DateLUTImpl *)
    {
        writeText(x, wb);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDate>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDate::FieldType x, WriteBuffer & wb, const DataTypeDate *, const DateLUTImpl * time_zone)
    {
        writeDateText(DayNum(x), wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDate32>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDate32::FieldType x, WriteBuffer & wb, const DataTypeDate32 *, const DateLUTImpl * time_zone)
    {
        writeDateText(ExtendedDayNum(x), wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDateTime>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDateTime::FieldType x, WriteBuffer & wb, const DataTypeDateTime *, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(x, wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDateTime64>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDateTime64::FieldType x, WriteBuffer & wb, const DataTypeDateTime64 * type, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(DateTime64(x), type->getScale(), wb, *time_zone);
        return ReturnType(true);
    }
};


template <typename FieldType>
struct FormatImpl<DataTypeEnum<FieldType>>
{
    template <typename ReturnType = void>
    static ReturnType execute(const FieldType x, WriteBuffer & wb, const DataTypeEnum<FieldType> * type, const DateLUTImpl *)
    {
        static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

        if constexpr (throw_exception)
        {
            writeString(type->getNameForValue(x), wb);
        }
        else
        {
            StringRef res;
            bool is_ok = type->getNameForValue(x, res);
            if (is_ok)
                writeString(res, wb);
            return ReturnType(is_ok);
        }
    }
};

template <typename FieldType>
struct FormatImpl<DataTypeDecimal<FieldType>>
{
    template <typename ReturnType = void>
    static ReturnType execute(const FieldType x, WriteBuffer & wb, const DataTypeDecimal<FieldType> * type, const DateLUTImpl *)
    {
        writeText(x, type->getScale(), wb, false);
        return ReturnType(true);
    }
};

ColumnUInt8::MutablePtr copyNullMap(ColumnPtr col)
{
    ColumnUInt8::MutablePtr null_map = nullptr;
    if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col.get()))
    {
        null_map = ColumnUInt8::create();
        null_map->insertRangeFrom(col_nullable->getNullMapColumn(), 0, col_nullable->size());
    }
    return null_map;
}


/// Generic conversion of any type to String or FixedString via serialization to text.
template <typename StringColumnType>
struct ConvertImplGenericToString
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/, const ContextPtr & context)
    {
        static_assert(std::is_same_v<StringColumnType, ColumnString> || std::is_same_v<StringColumnType, ColumnFixedString>,
                "Can be used only to serialize to ColumnString or ColumnFixedString");

        ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);

        const auto & col_with_type_and_name = columnGetNested(arguments[0]);
        const IDataType & type = *col_with_type_and_name.type;
        const IColumn & col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();
        auto col_to = removeNullable(result_type)->createColumn();

        {
            ColumnStringHelpers::WriteHelper write_helper(
                    assert_cast<StringColumnType &>(*col_to),
                    size);

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
    if constexpr (std::is_floating_point_v<typename DataType::FieldType>)
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
    if constexpr (std::is_floating_point_v<typename DataType::FieldType>)
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


/** Throw exception with verbose message when string value is not parsed completely.
  */
[[noreturn]] inline void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, const IDataType & result_type)
{
    WriteBufferFromOwnString message_buf;
    message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
                << " as " << result_type.getName()
                << ": syntax error";

    if (read_buffer.offset())
        message_buf << " at position " << read_buffer.offset()
                    << " (parsed just " << quote << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
    else
        message_buf << " at begin of string";

    // Currently there are no functions toIPv{4,6}Or{Null,Zero}
    if (isNativeNumber(result_type) && !(result_type.getName() == "IPv4" || result_type.getName() == "IPv6"))
        message_buf << ". Note: there are to" << result_type.getName() << "OrZero and to" << result_type.getName() << "OrNull functions, which returns zero/NULL instead of throwing exception.";

    throw Exception(PreformattedMessage{message_buf.str(), "Cannot parse string {} as {}: syntax error {}", {String(read_buffer.buffer().begin(), read_buffer.buffer().size()), result_type.getName()}}, ErrorCodes::CANNOT_PARSE_TEXT);
}


enum class ConvertFromStringExceptionMode : uint8_t
{
    Throw,  /// Throw exception if value cannot be parsed.
    Zero,   /// Fill with zero or default if value cannot be parsed.
    Null    /// Return ColumnNullable with NULLs when value cannot be parsed.
};

enum class ConvertFromStringParsingMode : uint8_t
{
    Normal,
    BestEffort,  /// Only applicable for DateTime. Will use sophisticated method, that is slower.
    BestEffortUS
};

struct AccurateConvertStrategyAdditions
{
    UInt32 scale { 0 };
};

struct AccurateOrNullConvertStrategyAdditions
{
    UInt32 scale { 0 };
};

template <typename FromDataType, typename ToDataType, typename Name,
    ConvertFromStringExceptionMode exception_mode, ConvertFromStringParsingMode parsing_mode>
struct ConvertThroughParsing
{
    static_assert(std::is_same_v<FromDataType, DataTypeString> || std::is_same_v<FromDataType, DataTypeFixedString>,
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

                if (in.buffer().size() >= strlen("YYYY-MM-DD hh:mm:ss.x")
                    && in.buffer().begin()[19] == '.')
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
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t input_rows_count,
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

            if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort || parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
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
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                col_from->getName(), Name::name);

        if (std::is_same_v<FromDataType, DataTypeFixedString> && !col_from_fixed_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                col_from->getName(), Name::name);

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
                                    throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse string to type {}", TypeName<typename ToDataType::FieldType>);
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
                        convertFromTime<ToDataType>(vec_to[i],res);
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
                        convertFromTime<ToDataType>(vec_to[i],res);
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
                    else if (std::is_same_v<FromDataType, DataTypeFixedString> && std::is_same_v<ToDataType, DataTypeIPv6>
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
                        vec_to[i] = -static_cast<Int32>(DateLUT::instance().getDayNumOffsetEpoch()); /// NOLINT(readability-static-accessed-through-instance)
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


/// Function toUnixTimestamp has exactly the same implementation as toDateTime of String type.
struct NameToUnixTimestamp { static constexpr auto name = "toUnixTimestamp"; };

enum class BehaviourOnErrorFromString : uint8_t
{
    ConvertDefaultBehaviorTag,
    ConvertReturnNullOnErrorTag,
    ConvertReturnZeroOnErrorTag
};

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior>
struct ConvertImpl
{
    template <typename Additions = void *>
    static ColumnPtr NO_SANITIZE_UNDEFINED execute(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type [[maybe_unused]], size_t input_rows_count,
        BehaviourOnErrorFromString from_string_tag [[maybe_unused]], Additions additions = Additions())
    {
        const ColumnWithTypeAndName & named_from = arguments[0];

        if constexpr ((std::is_same_v<FromDataType, ToDataType> && !FromDataType::is_parametric)
            || (std::is_same_v<FromDataType, DataTypeEnum8> && std::is_same_v<ToDataType, DataTypeInt8>)
            || (std::is_same_v<FromDataType, DataTypeEnum16> && std::is_same_v<ToDataType, DataTypeInt16>))
        {
            /// If types are the same, reuse the columns.
            /// Conversions between Enum and the underlying type are also free.
            return named_from.column;
        }
        else if constexpr ((std::is_same_v<FromDataType, DataTypeDateTime> || std::is_same_v<FromDataType, DataTypeDate32>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            /// Conversion of DateTime to Date: throw off time component.
            /// Conversion of Date32 to Date.
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateImpl<date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime> && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            /// Conversion of DateTime to Date: throw off time component.
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDate32Impl, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((std::is_same_v<FromDataType, DataTypeDate> || std::is_same_v<FromDataType, DataTypeDate32>)
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            /// Conversion from Date/Date32 to DateTime.
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTimeImpl<date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64> && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, TransformDateTime64<ToDate32Impl>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
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
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeUInt32>
                || std::is_same_v<FromDataType, DataTypeUInt64>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTransform32Or64<typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt8>
                || std::is_same_v<FromDataType, DataTypeInt16>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTransform8Or16Signed<typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt32>
                || std::is_same_v<FromDataType, DataTypeInt64>
                || std::is_same_v<FromDataType, DataTypeFloat32>
                || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTransform32Or64Signed<typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeUInt32>
                || std::is_same_v<FromDataType, DataTypeUInt64>)
            && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDate32Transform32Or64<typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt8>
                || std::is_same_v<FromDataType, DataTypeInt16>)
            && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDate32Transform8Or16Signed<typename FromDataType::FieldType>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt32>
                || std::is_same_v<FromDataType, DataTypeInt64>
                || std::is_same_v<FromDataType, DataTypeFloat32>
                || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDate32>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDate32Transform32Or64Signed<typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        /// Special case of converting Int8, Int16, Int32 or (U)Int64 (and also, for convenience, Float32, Float64) to DateTime.
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt8>
                || std::is_same_v<FromDataType, DataTypeInt16>
                || std::is_same_v<FromDataType, DataTypeInt32>)
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTimeTransformSigned<typename FromDataType::FieldType, UInt32, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeUInt64>
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTimeTransform64<typename FromDataType::FieldType, UInt32, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt64>
                || std::is_same_v<FromDataType, DataTypeFloat32>
                || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTimeTransform64Signed<typename FromDataType::FieldType, UInt32, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeInt8>
                || std::is_same_v<FromDataType, DataTypeInt16>
                || std::is_same_v<FromDataType, DataTypeInt32>
                || std::is_same_v<FromDataType, DataTypeInt64>)
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTime64TransformSigned<typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeUInt64>
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTime64TransformUnsigned<UInt64, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeFloat32>
                || std::is_same_v<FromDataType, DataTypeFloat64>)
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTime64TransformFloat<FromDataType, typename FromDataType::FieldType, default_date_time_overflow_behavior>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        /// Conversion of DateTime64 to Date or DateTime: discards fractional part.
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>
            && std::is_same_v<ToDataType, DataTypeDate>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, TransformDateTime64<ToDateImpl<date_time_overflow_behavior>>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>
            && std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, TransformDateTime64<ToDateTimeImpl<date_time_overflow_behavior>>, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        /// Conversion of Date or DateTime to DateTime64: add zero sub-second part.
        else if constexpr ((
                std::is_same_v<FromDataType, DataTypeDate>
                || std::is_same_v<FromDataType, DataTypeDate32>
                || std::is_same_v<FromDataType, DataTypeDateTime>)
            && std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            return DateTimeTransformImpl<FromDataType, ToDataType, ToDateTime64Transform, false>::template execute<Additions>(
                arguments, result_type, input_rows_count, additions);
        }
        else if constexpr (IsDataTypeDateOrDateTime<FromDataType>
            && std::is_same_v<ToDataType, DataTypeString>)
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
                    data_to.resize(size * 3);   /// Arbitrary

                offsets_to.resize(size);

                WriteBufferFromVector<ColumnString::Chars> write_buffer(data_to);
                const FromDataType & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

                ColumnUInt8::MutablePtr null_map = copyNullMap(datetime_arg.column);

                bool cut_trailing_zeros_align_to_groups_of_thousands = false;
                if (DB::CurrentThread::isInitialized())
                {
                    const DB::ContextPtr query_context = DB::CurrentThread::get().getQueryContext();

                    if (query_context)
                        cut_trailing_zeros_align_to_groups_of_thousands = query_context->getSettingsRef()[Setting::date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands];
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
                                writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(DateTime64(vec_from[i]), type.getScale(), write_buffer, *time_zone);
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
                                writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(DateTime64(vec_from[i]), type.getScale(), write_buffer, *time_zone);
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
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                        arguments[0].column->getName(), Name::name);
        }
        /// Conversion from FixedString to String.
        /// Cutting sequences of zero bytes from end of strings.
        else if constexpr (std::is_same_v<ToDataType, DataTypeString>
            && std::is_same_v<FromDataType, DataTypeFixedString>)
        {
            ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);
            const auto & nested =  columnGetNested(arguments[0]);
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
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                        arguments[0].column->getName(), Name::name);
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
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                        arguments[0].column->getName(), Name::name);
        }
        else if constexpr (std::is_same_v<Name, NameToUnixTimestamp>
            && std::is_same_v<FromDataType, DataTypeString>
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
                return ConvertThroughParsing<FromDataType,
                                             ToDataType,
                                             Name,
                                             ConvertFromStringExceptionMode::Throw,
                                             ConvertFromStringParsingMode::Normal>::execute(
                        arguments, result_type, input_rows_count, additions);
            case BehaviourOnErrorFromString::ConvertReturnNullOnErrorTag:
                return ConvertThroughParsing<FromDataType,
                                             ToDataType,
                                             Name,
                                             ConvertFromStringExceptionMode::Null,
                                             ConvertFromStringParsingMode::Normal>::execute(
                        arguments, result_type, input_rows_count, additions);
            case BehaviourOnErrorFromString::ConvertReturnZeroOnErrorTag:
                return ConvertThroughParsing<FromDataType,
                                             ToDataType,
                                             Name,
                                             ConvertFromStringExceptionMode::Zero,
                                             ConvertFromStringParsingMode::Normal>::execute(
                        arguments, result_type, input_rows_count, additions);
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

            if constexpr ((IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
                && !(std::is_same_v<DataTypeDateTime64, FromDataType> || std::is_same_v<DataTypeDateTime64, ToDataType>)
                && (!IsDataTypeDecimalOrNumber<FromDataType> || !IsDataTypeDecimalOrNumber<ToDataType>))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    named_from.column->getName(), Name::name);
            }

            const ColVecFrom * col_from = checkAndGetColumn<ColVecFrom>(named_from.column.get());
            if (!col_from)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    named_from.column->getName(), Name::name);

            typename ColVecTo::MutablePtr col_to = nullptr;

            if constexpr (IsDataTypeDecimal<ToDataType>)
            {
                UInt32 scale;

                if constexpr (std::is_same_v<Additions, AccurateConvertStrategyAdditions>
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
                        std::is_same_v<DataTypeUInt128::FieldType, DataTypeUUID::FieldType::UnderlyingType>,
                        "UInt128 and IPv6 types must be same");

                    vec_to[i].items[1] = std::byteswap(vec_from[i].toUnderType().items[0]);
                    vec_to[i].items[0] = std::byteswap(vec_from[i].toUnderType().items[1]);
                }
                else if constexpr (std::is_same_v<FromDataType, DataTypeUUID> != std::is_same_v<ToDataType, DataTypeUUID>)
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                    "Conversion between numeric types and UUID is not supported. "
                                    "Probably the passed UUID is unquoted");
                }
                else if constexpr (
                    (std::is_same_v<FromDataType, DataTypeIPv4> != std::is_same_v<ToDataType, DataTypeIPv4>)
                    && !(is_any_of<FromDataType, DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeIPv6>
                        || is_any_of<ToDataType, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256, DataTypeIPv6>))
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Conversion from {} to {} is not supported",
                                    TypeName<typename FromDataType::FieldType>, TypeName<typename ToDataType::FieldType>);
                }
                else if constexpr (std::is_same_v<FromDataType, DataTypeIPv6> != std::is_same_v<ToDataType, DataTypeIPv6>
                    && !(std::is_same_v<ToDataType, DataTypeIPv4> || std::is_same_v<FromDataType, DataTypeIPv4>))
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
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
                            convert_result = tryConvertDecimals<FromDataType, ToDataType>(vec_from[i], col_from->getScale(), col_to->getScale(), result);
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
                    const uint8_t ip4_cidr[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00};
                    const uint8_t * src = reinterpret_cast<const uint8_t *>(&vec_from[i].toUnderType());
                    if (!matchIPv6Subnet(src, ip4_cidr, 96))
                    {
                        char addr[IPV6_MAX_TEXT_LENGTH + 1] {};
                        char * paddr = addr;
                        formatIPv6(src, paddr);

                        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "IPv6 {} in column {} is not in IPv4 mapping block", addr, named_from.column->getName());
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
                else if constexpr (std::is_same_v<Name, NameToUnixTimestamp>
                    && (std::is_same_v<FromDataType, DataTypeDate> || std::is_same_v<FromDataType, DataTypeDate32>))
                {
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i] * DATE_SECONDS_PER_DAY);
                }
                else
                {
                    /// If From Data is Nan or Inf and we convert to integer type, throw exception
                    if constexpr (std::is_floating_point_v<FromFieldType> && !std::is_floating_point_v<ToFieldType>)
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

                    if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>
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
                                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Value in column {} cannot be safely converted into type {}",
                                    named_from.column->getName(), result_type->getName());
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


/// Generic conversion of any type from String. Used for complex types: Array and Tuple or types with custom serialization.
template <bool throw_on_error>
struct ConvertImplGenericFromString
{
    static ColumnPtr execute(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count, const ContextPtr & context)
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


/// Declared early because used below.
struct NameToDate { static constexpr auto name = "toDate"; };
struct NameToDate32 { static constexpr auto name = "toDate32"; };
struct NameToDateTime { static constexpr auto name = "toDateTime"; };
struct NameToDateTime32 { static constexpr auto name = "toDateTime32"; };
struct NameToDateTime64 { static constexpr auto name = "toDateTime64"; };
struct NameToString { static constexpr auto name = "toString"; };
struct NameToDecimal32 { static constexpr auto name = "toDecimal32"; };
struct NameToDecimal64 { static constexpr auto name = "toDecimal64"; };
struct NameToDecimal128 { static constexpr auto name = "toDecimal128"; };
struct NameToDecimal256 { static constexpr auto name = "toDecimal256"; };


#define DEFINE_NAME_TO_INTERVAL(INTERVAL_KIND) \
    struct NameToInterval ## INTERVAL_KIND \
    { \
        static constexpr auto name = "toInterval" #INTERVAL_KIND; \
        static constexpr auto kind = IntervalKind::Kind::INTERVAL_KIND; \
    };

DEFINE_NAME_TO_INTERVAL(Nanosecond)
DEFINE_NAME_TO_INTERVAL(Microsecond)
DEFINE_NAME_TO_INTERVAL(Millisecond)
DEFINE_NAME_TO_INTERVAL(Second)
DEFINE_NAME_TO_INTERVAL(Minute)
DEFINE_NAME_TO_INTERVAL(Hour)
DEFINE_NAME_TO_INTERVAL(Day)
DEFINE_NAME_TO_INTERVAL(Week)
DEFINE_NAME_TO_INTERVAL(Month)
DEFINE_NAME_TO_INTERVAL(Quarter)
DEFINE_NAME_TO_INTERVAL(Year)

#undef DEFINE_NAME_TO_INTERVAL

struct NameParseDateTimeBestEffort;
struct NameParseDateTimeBestEffortOrZero;
struct NameParseDateTimeBestEffortOrNull;

template <typename Name, typename ToDataType>
constexpr bool mightBeDateTime()
{
    if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        return true;
    else if constexpr (
        std::is_same_v<Name, NameToDateTime> || std::is_same_v<Name, NameParseDateTimeBestEffort>
        || std::is_same_v<Name, NameParseDateTimeBestEffortOrZero> || std::is_same_v<Name, NameParseDateTimeBestEffortOrNull>)
        return true;

    return false;
}

template<typename Name, typename ToDataType>
inline bool isDateTime64(const ColumnsWithTypeAndName & arguments)
{
    if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        return true;
    else if constexpr (std::is_same_v<Name, NameToDateTime> || std::is_same_v<Name, NameParseDateTimeBestEffort>
        || std::is_same_v<Name, NameParseDateTimeBestEffortOrZero> || std::is_same_v<Name, NameParseDateTimeBestEffortOrNull>)
    {
        return (arguments.size() == 2 && isUInt(arguments[1].type)) || arguments.size() == 3;
    }

    return false;
}

template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction
{
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;
    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;
    static constexpr bool to_decimal = IsDataTypeDecimal<ToDataType> && !to_datetime64;

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionConvert>(context); }
    explicit FunctionConvert(ContextPtr context_) : context(context_) {}

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return std::is_same_v<Name, NameToString>; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return !(IsDataTypeDateOrDateTime<ToDataType> && isNumber(*arguments[0].type));
    }

    using DefaultReturnTypeGetter = std::function<DataTypePtr(const ColumnsWithTypeAndName &)>;
    static DataTypePtr getReturnTypeDefaultImplementationForNulls(const ColumnsWithTypeAndName & arguments, const DefaultReturnTypeGetter & getter)
    {
        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant)
        {
            return makeNullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presence.has_nullable)
        {
            auto nested_columns = Block(createBlockWithNestedColumns(arguments));
            auto return_type = getter(ColumnsWithTypeAndName(nested_columns.begin(), nested_columns.end()));
            return makeNullable(return_type);
        }

        return getter(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto getter = [&] (const auto & args) { return getReturnTypeImplRemovedNullable(args); };
        auto res = getReturnTypeDefaultImplementationForNulls(arguments, getter);
        to_nullable = res->isNullable();
        checked_return_type = true;
        return res;
    }

    DataTypePtr getReturnTypeImplRemovedNullable(const ColumnsWithTypeAndName & arguments) const
    {
        FunctionArgumentDescriptors mandatory_args = {{"Value", nullptr, nullptr, "any type"}};
        FunctionArgumentDescriptors optional_args;

        if constexpr (to_decimal)
        {
            mandatory_args.push_back({"scale", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), &isColumnConst, "const Integer"});
        }

        if (!to_decimal && isDateTime64<Name, ToDataType>(arguments))
        {
            mandatory_args.push_back({"scale", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), &isColumnConst, "const Integer"});
        }

        // toString(DateTime or DateTime64, [timezone: String])
        if ((std::is_same_v<Name, NameToString> && !arguments.empty() && (isDateTime64(arguments[0].type) || isDateTime(arguments[0].type)))
            // toUnixTimestamp(value[, timezone : String])
            || std::is_same_v<Name, NameToUnixTimestamp>
            // toDate(value[, timezone : String])
            || std::is_same_v<ToDataType, DataTypeDate> // TODO: shall we allow timestamp argument for toDate? DateTime knows nothing about timezones and this argument is ignored below.
            // toDate32(value[, timezone : String])
            || std::is_same_v<ToDataType, DataTypeDate32>
            // toDateTime(value[, timezone: String])
            || std::is_same_v<ToDataType, DataTypeDateTime>
            // toDateTime64(value, scale : Integer[, timezone: String])
            || std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            optional_args.push_back({"timezone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"});
        }

            validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        if constexpr (std::is_same_v<ToDataType, DataTypeInterval>)
        {
            return std::make_shared<DataTypeInterval>(Name::kind);
        }
        else if constexpr (to_decimal)
        {
            UInt64 scale = extractToDecimalScale(arguments[1]);

            if constexpr (std::is_same_v<Name, NameToDecimal32>)
                return createDecimalMaxPrecision<Decimal32>(scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal64>)
                return createDecimalMaxPrecision<Decimal64>(scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal128>)
                return createDecimalMaxPrecision<Decimal128>(scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal256>)
                return createDecimalMaxPrecision<Decimal256>(scale);

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected branch in code of conversion function: it is a bug.");
        }
        else
        {
            // Optional second argument with time zone for DateTime.
            UInt8 timezone_arg_position = 1;
            UInt32 scale [[maybe_unused]] = DataTypeDateTime64::default_scale;

            // DateTime64 requires more arguments: scale and timezone. Since timezone is optional, scale should be first.
            if (isDateTime64<Name, ToDataType>(arguments))
            {
                timezone_arg_position += 1;
                scale = static_cast<UInt32>(arguments[1].column->get64(0));

                if (to_datetime64 || scale != 0) /// toDateTime('xxxx-xx-xx xx:xx:xx', 0) return DateTime
                    return std::make_shared<DataTypeDateTime64>(scale,
                        extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0, false));

                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0, false));
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0, false));
            else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected branch in code of conversion function: it is a bug.");
            else
                return std::make_shared<ToDataType>();
        }
    }

    /// Function actually uses default implementation for nulls,
    /// but we need to know if return type is Nullable or not,
    /// so we use checked_return_type only to intercept the first call to getReturnTypeImpl(...).
    bool useDefaultImplementationForNulls() const override
    {
        bool to_nullable_string = to_nullable && std::is_same_v<ToDataType, DataTypeString>;
        return checked_return_type && !to_nullable_string;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (std::is_same_v<ToDataType, DataTypeString>)
            return {};
        else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
            return {2};
        return {1};
    }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        try
        {
            return executeInternal(arguments, result_type, input_rows_count);
        }
        catch (Exception & e)
        {
            /// More convenient error message.
            if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            {
                e.addMessage("Cannot parse "
                    + result_type->getName() + " from "
                    + arguments[0].type->getName()
                    + ", because value is too short");
            }
            else if (e.code() == ErrorCodes::CANNOT_PARSE_NUMBER
                || e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
                || e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
                || e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
                || e.code() == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
                || e.code() == ErrorCodes::CANNOT_PARSE_DATE
                || e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
                || e.code() == ErrorCodes::CANNOT_PARSE_UUID
                || e.code() == ErrorCodes::CANNOT_PARSE_IPV4
                || e.code() == ErrorCodes::CANNOT_PARSE_IPV6)
            {
                e.addMessage("Cannot parse "
                    + result_type->getName() + " from "
                    + arguments[0].type->getName());
            }

            throw;
        }
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return Monotonic::has();
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return Monotonic::get(type, left, right);
    }

private:
    ContextPtr context;
    mutable bool checked_return_type = false;
    mutable bool to_nullable = false;

    ColumnPtr executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects at least 1 argument", getName());

        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const DataTypePtr from_type = removeNullable(arguments[0].type);
        ColumnPtr result_column;

        FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior;

        if (context)
            date_time_overflow_behavior = context->getSettingsRef()[Setting::date_time_overflow_behavior].value;

        auto call = [&](const auto & types, BehaviourOnErrorFromString from_string_tag) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if constexpr (IsDataTypeDecimal<RightDataType>)
            {
                if constexpr (std::is_same_v<RightDataType, DataTypeDateTime64>)
                {
                    /// Account for optional timezone argument.
                    if (arguments.size() != 2 && arguments.size() != 3)
                        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects 2 or 3 arguments for DataTypeDateTime64.", getName());
                }
                else if (arguments.size() != 2)
                {
                    throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects 2 arguments for Decimal.", getName());
                }

                const ColumnWithTypeAndName & scale_column = arguments[1];
                UInt32 scale = extractToDecimalScale(scale_column);

                switch (date_time_overflow_behavior)
                {
                    case FormatSettings::DateTimeOverflowBehavior::Throw:
                        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::Throw>::execute(arguments, result_type, input_rows_count, from_string_tag, scale);
                        break;
                    case FormatSettings::DateTimeOverflowBehavior::Ignore:
                        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::Ignore>::execute(arguments, result_type, input_rows_count, from_string_tag, scale);
                        break;
                    case FormatSettings::DateTimeOverflowBehavior::Saturate:
                        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::Saturate>::execute(arguments, result_type, input_rows_count, from_string_tag, scale);
                        break;
                }
            }
            else if constexpr (IsDataTypeDateOrDateTime<RightDataType> && std::is_same_v<LeftDataType, DataTypeDateTime64>)
            {
                const auto * dt64 = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
                switch (date_time_overflow_behavior)
                {
                    case FormatSettings::DateTimeOverflowBehavior::Throw:
                        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::Throw>::execute(arguments, result_type, input_rows_count, from_string_tag, dt64->getScale());
                        break;
                    case FormatSettings::DateTimeOverflowBehavior::Ignore:
                        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::Ignore>::execute(arguments, result_type, input_rows_count, from_string_tag, dt64->getScale());
                        break;
                    case FormatSettings::DateTimeOverflowBehavior::Saturate:
                        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::Saturate>::execute(arguments, result_type, input_rows_count, from_string_tag, dt64->getScale());
                        break;
                }
            }
            else if constexpr ((IsDataTypeNumber<LeftDataType>
                                || IsDataTypeDateOrDateTime<LeftDataType>)&&IsDataTypeDateOrDateTime<RightDataType>)
            {
#define GENERATE_OVERFLOW_MODE_CASE(OVERFLOW_MODE) \
    case FormatSettings::DateTimeOverflowBehavior::OVERFLOW_MODE: \
        result_column = ConvertImpl<LeftDataType, RightDataType, Name, FormatSettings::DateTimeOverflowBehavior::OVERFLOW_MODE>::execute( \
            arguments, result_type, input_rows_count, from_string_tag); \
        break;
                switch (date_time_overflow_behavior)
                {
                    GENERATE_OVERFLOW_MODE_CASE(Throw)
                    GENERATE_OVERFLOW_MODE_CASE(Ignore)
                    GENERATE_OVERFLOW_MODE_CASE(Saturate)
                }

#undef GENERATE_OVERFLOW_MODE_CASE
            }
            else if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftT = typename LeftDataType::FieldType;
                using RightT = typename RightDataType::FieldType;

                static constexpr bool bad_left =
                    is_decimal<LeftT> || std::is_floating_point_v<LeftT> || is_big_int_v<LeftT> || is_signed_v<LeftT>;
                static constexpr bool bad_right =
                    is_decimal<RightT> || std::is_floating_point_v<RightT> || is_big_int_v<RightT> || is_signed_v<RightT>;

                /// Disallow int vs UUID conversion (but support int vs UInt128 conversion)
                if constexpr ((bad_left && std::is_same_v<RightDataType, DataTypeUUID>) ||
                              (bad_right && std::is_same_v<LeftDataType, DataTypeUUID>))
                {
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Wrong UUID conversion");
                }
                else
                {
                    result_column = ConvertImpl<LeftDataType, RightDataType, Name>::execute(
                        arguments, result_type, input_rows_count, from_string_tag);
                }
            }
            else
                result_column = ConvertImpl<LeftDataType, RightDataType, Name>::execute(arguments, result_type, input_rows_count, from_string_tag);

            return true;
        };

        if constexpr (mightBeDateTime<Name, ToDataType>())
        {
            if (isDateTime64<Name, ToDataType>(arguments))
            {
                /// For toDateTime('xxxx-xx-xx xx:xx:xx.00', 2[, 'timezone']) we need to it convert to DateTime64
                const ColumnWithTypeAndName & scale_column = arguments[1];
                UInt32 scale = extractToDecimalScale(scale_column);

                if (to_datetime64 || scale != 0) /// When scale = 0, the data type is DateTime otherwise the data type is DateTime64
                {
                    if (!callOnIndexAndDataType<DataTypeDateTime64>(
                            from_type->getTypeId(), call, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag))
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument of function {}",
                            arguments[0].type->getName(),
                            getName());

                    return result_column;
                }
            }
        }

        if constexpr (std::is_same_v<ToDataType, DataTypeString>)
        {
            if (from_type->getCustomSerialization())
                return ConvertImplGenericToString<ColumnString>::execute(arguments, result_type, input_rows_count, context);
        }

        bool done = false;
        if constexpr (is_any_of<ToDataType, DataTypeString, DataTypeFixedString>)
        {
            done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag);
        }
        else
        {
            bool cast_ipv4_ipv6_default_on_conversion_error = false;
            if constexpr (is_any_of<ToDataType, DataTypeIPv4, DataTypeIPv6>)
            {
                if (context && (cast_ipv4_ipv6_default_on_conversion_error = context->getSettingsRef()[Setting::cast_ipv4_ipv6_default_on_conversion_error]))
                    done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, BehaviourOnErrorFromString::ConvertReturnZeroOnErrorTag);
            }

            if (!cast_ipv4_ipv6_default_on_conversion_error)
            {
                /// We should use ConvertFromStringExceptionMode::Null mode when converting from String (or FixedString)
                /// to Nullable type, to avoid 'value is too short' error on attempt to parse empty string from NULL values.
                if (to_nullable && WhichDataType(from_type).isStringOrFixedString())
                    done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, BehaviourOnErrorFromString::ConvertReturnNullOnErrorTag);
                else
                    done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag);
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeInterval>)
                if (WhichDataType(from_type).isInterval())
                    done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag);
        }

        if (!done)
        {
            /// Generic conversion of any type to String.
            if (std::is_same_v<ToDataType, DataTypeString>)
            {
                return ConvertImplGenericToString<ColumnString>::execute(arguments, result_type, input_rows_count, context);
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arguments[0].type->getName(), getName());
        }

        return result_column;
    }
};


/** Function toTOrZero (where T is number of date or datetime type):
  *  try to convert from String to type T through parsing,
  *  if cannot parse, return default value instead of throwing exception.
  * Function toTOrNull will return Nullable type with NULL when cannot parse.
  * NOTE Also need to implement tryToUnixTimestamp with timezone.
  */
template <typename ToDataType, typename Name,
    ConvertFromStringExceptionMode exception_mode,
    ConvertFromStringParsingMode parsing_mode = ConvertFromStringParsingMode::Normal>
class FunctionConvertFromString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;
    static constexpr bool to_decimal = IsDataTypeDecimal<ToDataType> && !to_datetime64;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConvertFromString>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypePtr res;

        if (isDateTime64<Name, ToDataType>(arguments))
        {
            validateFunctionArguments(*this, arguments,
                FunctionArgumentDescriptors{{"string", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}},
                // optional
                FunctionArgumentDescriptors{
                    {"precision", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const UInt8"},
                    {"timezone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), isColumnConst, "const String or FixedString"},
                });

            UInt64 scale = to_datetime64 ? DataTypeDateTime64::default_scale : 0;
            if (arguments.size() > 1)
                scale = extractToDecimalScale(arguments[1]);
            const auto timezone = extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false);

            res = scale == 0 ? res = std::make_shared<DataTypeDateTime>(timezone) : std::make_shared<DataTypeDateTime64>(scale, timezone);
        }
        else
        {
            if ((arguments.size() != 1 && arguments.size() != 2) || (to_decimal && arguments.size() != 2))
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2. "
                    "Second argument only make sense for DateTime (time zone, optional) and Decimal (scale).",
                    getName(), arguments.size());

            if (!isStringOrFixedString(arguments[0].type))
            {
                if (this->getName().find("OrZero") != std::string::npos ||
                    this->getName().find("OrNull") != std::string::npos)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}. "
                            "Conversion functions with postfix 'OrZero' or 'OrNull' should take String argument",
                            arguments[0].type->getName(), getName());
                else
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}",
                            arguments[0].type->getName(), getName());
            }

            if (arguments.size() == 2)
            {
                if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                {
                    if (!isString(arguments[1].type))
                        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 2nd argument of function {}",
                            arguments[1].type->getName(), getName());
                }
                else if constexpr (to_decimal)
                {
                    if (!isInteger(arguments[1].type))
                        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 2nd argument of function {}",
                            arguments[1].type->getName(), getName());
                    if (!arguments[1].column)
                        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be constant", getName());
                }
                else
                {
                    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Number of arguments for function {} doesn't match: passed {}, should be 1. "
                        "Second argument makes sense only for DateTime and Decimal.",
                        getName(), arguments.size());
                }
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                res = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false));
            else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedMySQL is a bug.");
            else if constexpr (to_decimal)
            {
                UInt64 scale = extractToDecimalScale(arguments[1]);
                res = createDecimalMaxPrecision<typename ToDataType::FieldType>(scale);
                if (!res)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Something wrong with toDecimalNNOrZero() or toDecimalNNOrNull()");
            }
            else
                res = std::make_shared<ToDataType>();
        }

        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
            res = std::make_shared<DataTypeNullable>(res);

        return res;
    }

    template <typename ConvertToDataType>
    ColumnPtr executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, UInt32 scale) const
    {
        const IDataType * from_type = arguments[0].type.get();

        if (checkAndGetDataType<DataTypeString>(from_type))
        {
            return ConvertThroughParsing<DataTypeString, ConvertToDataType, Name, exception_mode, parsing_mode>::execute(
                arguments, result_type, input_rows_count, scale);
        }
        else if (checkAndGetDataType<DataTypeFixedString>(from_type))
        {
            return ConvertThroughParsing<DataTypeFixedString, ConvertToDataType, Name, exception_mode, parsing_mode>::execute(
                arguments, result_type, input_rows_count, scale);
        }

        return nullptr;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr result_column;

        if constexpr (to_decimal)
        {
            result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count,
                assert_cast<const ToDataType &>(*removeNullable(result_type)).getScale());
        }
        else if constexpr (mightBeDateTime<Name, ToDataType>())
        {
            if (isDateTime64<Name, ToDataType>(arguments))
            {
                UInt64 scale = to_datetime64 ? DataTypeDateTime64::default_scale : 0;
                if (arguments.size() > 1)
                    scale = extractToDecimalScale(arguments[1]);

                if (scale == 0)
                {
                    result_column = executeInternal<DataTypeDateTime>(arguments, result_type, input_rows_count, 0);
                }
                else
                {
                    result_column
                        = executeInternal<DataTypeDateTime64>(arguments, result_type, input_rows_count, static_cast<UInt32>(scale));
                }
            }
            else
            {
                result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count, 0);
            }
        }
        else
        {
            result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count, 0);
        }

        if (!result_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                "Only String or FixedString argument is accepted for try-conversion function. For other arguments, "
                "use function without 'orZero' or 'orNull'.", arguments[0].type->getName(), getName());

        return result_column;
    }
};


/// Monotonicity.

struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { .is_monotonic = true };
    }
};

struct UnknownMonotonicity
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { };
    }
};

template <typename T>
struct ToNumberMonotonicity
{
    static bool has() { return true; }

    static UInt64 divideByRangeOfType(UInt64 x)
    {
        if constexpr (sizeof(T) < sizeof(UInt64))
            return x >> (sizeof(T) * 8);
        else
            return 0;
    }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        if (!type.isValueRepresentedByNumber())
            return {};

        /// If type is same, the conversion is always monotonic.
        /// (Enum has separate case, because it is different data type)
        if (checkAndGetDataType<DataTypeNumber<T>>(&type) ||
            checkAndGetDataType<DataTypeEnum<T>>(&type))
            return { .is_monotonic = true, .is_always_monotonic = true };

        /// Float cases.

        /// When converting to Float, the conversion is always monotonic.
        if constexpr (std::is_floating_point_v<T>)
            return { .is_monotonic = true, .is_always_monotonic = true };

        const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(&type);
        const IDataType * low_cardinality_dictionary_type = nullptr;
        if (low_cardinality)
            low_cardinality_dictionary_type = low_cardinality->getDictionaryType().get();

        WhichDataType which_type(type);
        WhichDataType which_inner_type = low_cardinality
            ? WhichDataType(low_cardinality_dictionary_type)
            : WhichDataType(type);

        /// If converting from Float, for monotonicity, arguments must fit in range of result type.
        if (which_inner_type.isFloat())
        {
            if (left.isNull() || right.isNull())
                return {};

            Float64 left_float = left.safeGet<Float64>();
            Float64 right_float = right.safeGet<Float64>();

            if (left_float >= static_cast<Float64>(std::numeric_limits<T>::min())
                && left_float <= static_cast<Float64>(std::numeric_limits<T>::max())
                && right_float >= static_cast<Float64>(std::numeric_limits<T>::min())
                && right_float <= static_cast<Float64>(std::numeric_limits<T>::max()))
                return { .is_monotonic = true };

            return {};
        }

        /// Integer cases.

        /// Only support types represented by native integers.
        /// It can be extended to big integers, decimals and DateTime64 later.
        /// By the way, NULLs are representing unbounded ranges.
        if (!((left.isNull() || left.getType() == Field::Types::UInt64 || left.getType() == Field::Types::Int64)
            && (right.isNull() || right.getType() == Field::Types::UInt64 || right.getType() == Field::Types::Int64)))
            return {};

        const bool from_is_unsigned = type.isValueRepresentedByUnsignedInteger();
        const bool to_is_unsigned = is_unsigned_v<T>;

        const size_t size_of_from = type.getSizeOfValueInMemory();
        const size_t size_of_to = sizeof(T);

        const bool left_in_first_half = left.isNull()
            ? from_is_unsigned
            : (left.safeGet<Int64>() >= 0);

        const bool right_in_first_half = right.isNull()
            ? !from_is_unsigned
            : (right.safeGet<Int64>() >= 0);

        /// Size of type is the same.
        if (size_of_from == size_of_to)
        {
            if (from_is_unsigned == to_is_unsigned)
                return { .is_monotonic = true, .is_always_monotonic = true };

            if (left_in_first_half == right_in_first_half)
                return { .is_monotonic = true };

            return {};
        }

        /// Size of type is expanded.
        if (size_of_from < size_of_to)
        {
            if (from_is_unsigned == to_is_unsigned)
                return { .is_monotonic = true, .is_always_monotonic = true };

            if (!to_is_unsigned)
                return { .is_monotonic = true, .is_always_monotonic = true };

            /// signed -> unsigned. If arguments from the same half, then function is monotonic.
            if (left_in_first_half == right_in_first_half)
                return { .is_monotonic = true };

            return {};
        }

        /// Size of type is shrunk.
        if (size_of_from > size_of_to)
        {
            /// Function cannot be monotonic on unbounded ranges.
            if (left.isNull() || right.isNull())
                return {};

            /// Function cannot be monotonic when left and right are not on the same ranges.
            if (divideByRangeOfType(left.safeGet<UInt64>()) != divideByRangeOfType(right.safeGet<UInt64>()))
                return {};

            if (to_is_unsigned)
                return { .is_monotonic = true };
            else
            {
                // If To is signed, it's possible that the signedness is different after conversion. So we check it explicitly.
                const bool is_monotonic = (T(left.safeGet<UInt64>()) >= 0) == (T(right.safeGet<UInt64>()) >= 0);

                return { .is_monotonic = is_monotonic };
            }
        }

        UNREACHABLE();
    }
};

struct ToDateMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        auto which = WhichDataType(type);
        if (which.isDateOrDate32() || which.isDateTime() || which.isDateTime64() || which.isInt8() || which.isInt16() || which.isUInt8()
            || which.isUInt16())
        {
            return {.is_monotonic = true, .is_always_monotonic = true};
        }
        else if (
            ((left.getType() == Field::Types::UInt64 || left.isNull()) && (right.getType() == Field::Types::UInt64 || right.isNull())
             && ((left.isNull() || left.safeGet<UInt64>() < 0xFFFF) && (right.isNull() || right.safeGet<UInt64>() >= 0xFFFF)))
            || ((left.getType() == Field::Types::Int64 || left.isNull()) && (right.getType() == Field::Types::Int64 || right.isNull())
                && ((left.isNull() || left.safeGet<Int64>() < 0xFFFF) && (right.isNull() || right.safeGet<Int64>() >= 0xFFFF)))
            || ((
                (left.getType() == Field::Types::Float64 || left.isNull())
                && (right.getType() == Field::Types::Float64 || right.isNull())
                && ((left.isNull() || left.safeGet<Float64>() < 0xFFFF) && (right.isNull() || right.safeGet<Float64>() >= 0xFFFF))))
            || !isNativeNumber(type))
        {
            return {};
        }
        else
        {
            return {.is_monotonic = true, .is_always_monotonic = true};
        }
    }
};

struct ToDateTimeMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field &, const Field &)
    {
        if (type.isValueRepresentedByNumber())
            return {.is_monotonic = true, .is_always_monotonic = true};
        else
            return {};
    }
};

/** The monotonicity for the `toString` function is mainly determined for test purposes.
  * It is doubtful that anyone is looking to optimize queries with conditions `toString(CounterID) = 34`.
  */
struct ToStringMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        IFunction::Monotonicity positive{ .is_monotonic = true };
        IFunction::Monotonicity not_monotonic;

        const auto * type_ptr = &type;
        if (const auto * low_cardinality_type = checkAndGetDataType<DataTypeLowCardinality>(type_ptr))
            type_ptr = low_cardinality_type->getDictionaryType().get();

        /// Order on enum values (which is the order on integers) is completely arbitrary in respect to the order on strings.
        if (WhichDataType(type).isEnum())
            return not_monotonic;

        /// `toString` function is monotonous if the argument is Date or Date32 or DateTime or String, or non-negative numbers with the same number of symbols.
        if (checkDataTypes<DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeString>(type_ptr))
            return positive;

        if (left.isNull() || right.isNull())
            return {};

        if (left.getType() == Field::Types::UInt64
            && right.getType() == Field::Types::UInt64)
        {
            return (left.safeGet<Int64>() == 0 && right.safeGet<Int64>() == 0)
                || (floor(log10(left.safeGet<UInt64>())) == floor(log10(right.safeGet<UInt64>())))
                ? positive : not_monotonic;
        }

        if (left.getType() == Field::Types::Int64
            && right.getType() == Field::Types::Int64)
        {
            return (left.safeGet<Int64>() == 0 && right.safeGet<Int64>() == 0)
                || (left.safeGet<Int64>() > 0 && right.safeGet<Int64>() > 0 && floor(log10(left.safeGet<Int64>())) == floor(log10(right.safeGet<Int64>())))
                ? positive : not_monotonic;
        }

        return not_monotonic;
    }
};


struct NameToUInt8 { static constexpr auto name = "toUInt8"; };
struct NameToUInt16 { static constexpr auto name = "toUInt16"; };
struct NameToUInt32 { static constexpr auto name = "toUInt32"; };
struct NameToUInt64 { static constexpr auto name = "toUInt64"; };
struct NameToUInt128 { static constexpr auto name = "toUInt128"; };
struct NameToUInt256 { static constexpr auto name = "toUInt256"; };
struct NameToInt8 { static constexpr auto name = "toInt8"; };
struct NameToInt16 { static constexpr auto name = "toInt16"; };
struct NameToInt32 { static constexpr auto name = "toInt32"; };
struct NameToInt64 { static constexpr auto name = "toInt64"; };
struct NameToInt128 { static constexpr auto name = "toInt128"; };
struct NameToInt256 { static constexpr auto name = "toInt256"; };
struct NameToFloat32 { static constexpr auto name = "toFloat32"; };
struct NameToFloat64 { static constexpr auto name = "toFloat64"; };
struct NameToUUID { static constexpr auto name = "toUUID"; };
struct NameToIPv4 { static constexpr auto name = "toIPv4"; };
struct NameToIPv6 { static constexpr auto name = "toIPv6"; };

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8, ToNumberMonotonicity<UInt8>>;
using FunctionToUInt16 = FunctionConvert<DataTypeUInt16, NameToUInt16, ToNumberMonotonicity<UInt16>>;
using FunctionToUInt32 = FunctionConvert<DataTypeUInt32, NameToUInt32, ToNumberMonotonicity<UInt32>>;
using FunctionToUInt64 = FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;
using FunctionToUInt128 = FunctionConvert<DataTypeUInt128, NameToUInt128, ToNumberMonotonicity<UInt128>>;
using FunctionToUInt256 = FunctionConvert<DataTypeUInt256, NameToUInt256, ToNumberMonotonicity<UInt256>>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8, NameToInt8, ToNumberMonotonicity<Int8>>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16, NameToInt16, ToNumberMonotonicity<Int16>>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32, NameToInt32, ToNumberMonotonicity<Int32>>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64, NameToInt64, ToNumberMonotonicity<Int64>>;
using FunctionToInt128 = FunctionConvert<DataTypeInt128, NameToInt128, ToNumberMonotonicity<Int128>>;
using FunctionToInt256 = FunctionConvert<DataTypeInt256, NameToInt256, ToNumberMonotonicity<Int256>>;
using FunctionToFloat32 = FunctionConvert<DataTypeFloat32, NameToFloat32, ToNumberMonotonicity<Float32>>;
using FunctionToFloat64 = FunctionConvert<DataTypeFloat64, NameToFloat64, ToNumberMonotonicity<Float64>>;

using FunctionToDate = FunctionConvert<DataTypeDate, NameToDate, ToDateMonotonicity>;

using FunctionToDate32 = FunctionConvert<DataTypeDate32, NameToDate32, ToDateMonotonicity>;

using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime, ToDateTimeMonotonicity>;

using FunctionToDateTime32 = FunctionConvert<DataTypeDateTime, NameToDateTime32, ToDateTimeMonotonicity>;

using FunctionToDateTime64 = FunctionConvert<DataTypeDateTime64, NameToDateTime64, ToDateTimeMonotonicity>;

using FunctionToUUID = FunctionConvert<DataTypeUUID, NameToUUID, ToNumberMonotonicity<UInt128>>;
using FunctionToIPv4 = FunctionConvert<DataTypeIPv4, NameToIPv4, ToNumberMonotonicity<UInt32>>;
using FunctionToIPv6 = FunctionConvert<DataTypeIPv6, NameToIPv6, ToNumberMonotonicity<UInt128>>;
using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToUnixTimestamp = FunctionConvert<DataTypeUInt32, NameToUnixTimestamp, ToNumberMonotonicity<UInt32>>;
using FunctionToDecimal32 = FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
using FunctionToDecimal64 = FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
using FunctionToDecimal128 = FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;
using FunctionToDecimal256 = FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256, UnknownMonotonicity>;

template <typename DataType, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior> struct FunctionTo;

template <> struct FunctionTo<DataTypeUInt8> { using Type = FunctionToUInt8; };
template <> struct FunctionTo<DataTypeUInt16> { using Type = FunctionToUInt16; };
template <> struct FunctionTo<DataTypeUInt32> { using Type = FunctionToUInt32; };
template <> struct FunctionTo<DataTypeUInt64> { using Type = FunctionToUInt64; };
template <> struct FunctionTo<DataTypeUInt128> { using Type = FunctionToUInt128; };
template <> struct FunctionTo<DataTypeUInt256> { using Type = FunctionToUInt256; };
template <> struct FunctionTo<DataTypeInt8> { using Type = FunctionToInt8; };
template <> struct FunctionTo<DataTypeInt16> { using Type = FunctionToInt16; };
template <> struct FunctionTo<DataTypeInt32> { using Type = FunctionToInt32; };
template <> struct FunctionTo<DataTypeInt64> { using Type = FunctionToInt64; };
template <> struct FunctionTo<DataTypeInt128> { using Type = FunctionToInt128; };
template <> struct FunctionTo<DataTypeInt256> { using Type = FunctionToInt256; };
template <> struct FunctionTo<DataTypeFloat32> { using Type = FunctionToFloat32; };
template <> struct FunctionTo<DataTypeFloat64> { using Type = FunctionToFloat64; };

template <FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct FunctionTo<DataTypeDate, date_time_overflow_behavior> { using Type = FunctionToDate; };

template <FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct FunctionTo<DataTypeDate32, date_time_overflow_behavior> { using Type = FunctionToDate32; };

template <FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct FunctionTo<DataTypeDateTime, date_time_overflow_behavior> { using Type = FunctionToDateTime; };

template <FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior>
struct FunctionTo<DataTypeDateTime64, date_time_overflow_behavior> { using Type = FunctionToDateTime64; };

template <> struct FunctionTo<DataTypeUUID> { using Type = FunctionToUUID; };
template <> struct FunctionTo<DataTypeIPv4> { using Type = FunctionToIPv4; };
template <> struct FunctionTo<DataTypeIPv6> { using Type = FunctionToIPv6; };
template <> struct FunctionTo<DataTypeString> { using Type = FunctionToString; };
template <> struct FunctionTo<DataTypeFixedString> { using Type = FunctionToFixedString; };
template <> struct FunctionTo<DataTypeDecimal<Decimal32>> { using Type = FunctionToDecimal32; };
template <> struct FunctionTo<DataTypeDecimal<Decimal64>> { using Type = FunctionToDecimal64; };
template <> struct FunctionTo<DataTypeDecimal<Decimal128>> { using Type = FunctionToDecimal128; };
template <> struct FunctionTo<DataTypeDecimal<Decimal256>> { using Type = FunctionToDecimal256; };

template <typename FieldType> struct FunctionTo<DataTypeEnum<FieldType>>
    : FunctionTo<DataTypeNumber<FieldType>>
{
};

struct NameToUInt8OrZero { static constexpr auto name = "toUInt8OrZero"; };
struct NameToUInt16OrZero { static constexpr auto name = "toUInt16OrZero"; };
struct NameToUInt32OrZero { static constexpr auto name = "toUInt32OrZero"; };
struct NameToUInt64OrZero { static constexpr auto name = "toUInt64OrZero"; };
struct NameToUInt128OrZero { static constexpr auto name = "toUInt128OrZero"; };
struct NameToUInt256OrZero { static constexpr auto name = "toUInt256OrZero"; };
struct NameToInt8OrZero { static constexpr auto name = "toInt8OrZero"; };
struct NameToInt16OrZero { static constexpr auto name = "toInt16OrZero"; };
struct NameToInt32OrZero { static constexpr auto name = "toInt32OrZero"; };
struct NameToInt64OrZero { static constexpr auto name = "toInt64OrZero"; };
struct NameToInt128OrZero { static constexpr auto name = "toInt128OrZero"; };
struct NameToInt256OrZero { static constexpr auto name = "toInt256OrZero"; };
struct NameToFloat32OrZero { static constexpr auto name = "toFloat32OrZero"; };
struct NameToFloat64OrZero { static constexpr auto name = "toFloat64OrZero"; };
struct NameToDateOrZero { static constexpr auto name = "toDateOrZero"; };
struct NameToDate32OrZero { static constexpr auto name = "toDate32OrZero"; };
struct NameToDateTimeOrZero { static constexpr auto name = "toDateTimeOrZero"; };
struct NameToDateTime64OrZero { static constexpr auto name = "toDateTime64OrZero"; };
struct NameToDecimal32OrZero { static constexpr auto name = "toDecimal32OrZero"; };
struct NameToDecimal64OrZero { static constexpr auto name = "toDecimal64OrZero"; };
struct NameToDecimal128OrZero { static constexpr auto name = "toDecimal128OrZero"; };
struct NameToDecimal256OrZero { static constexpr auto name = "toDecimal256OrZero"; };
struct NameToUUIDOrZero { static constexpr auto name = "toUUIDOrZero"; };
struct NameToIPv4OrZero { static constexpr auto name = "toIPv4OrZero"; };
struct NameToIPv6OrZero { static constexpr auto name = "toIPv6OrZero"; };

using FunctionToUInt8OrZero = FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt16OrZero = FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt32OrZero = FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt64OrZero = FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt128OrZero = FunctionConvertFromString<DataTypeUInt128, NameToUInt128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt256OrZero = FunctionConvertFromString<DataTypeUInt256, NameToUInt256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt8OrZero = FunctionConvertFromString<DataTypeInt8, NameToInt8OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt16OrZero = FunctionConvertFromString<DataTypeInt16, NameToInt16OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt32OrZero = FunctionConvertFromString<DataTypeInt32, NameToInt32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt64OrZero = FunctionConvertFromString<DataTypeInt64, NameToInt64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt128OrZero = FunctionConvertFromString<DataTypeInt128, NameToInt128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt256OrZero = FunctionConvertFromString<DataTypeInt256, NameToInt256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToFloat32OrZero = FunctionConvertFromString<DataTypeFloat32, NameToFloat32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToFloat64OrZero = FunctionConvertFromString<DataTypeFloat64, NameToFloat64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateOrZero = FunctionConvertFromString<DataTypeDate, NameToDateOrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDate32OrZero = FunctionConvertFromString<DataTypeDate32, NameToDate32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateTimeOrZero = FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateTime64OrZero = FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal32OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal64OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal128OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal256OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUUIDOrZero = FunctionConvertFromString<DataTypeUUID, NameToUUIDOrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToIPv4OrZero = FunctionConvertFromString<DataTypeIPv4, NameToIPv4OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToIPv6OrZero = FunctionConvertFromString<DataTypeIPv6, NameToIPv6OrZero, ConvertFromStringExceptionMode::Zero>;

struct NameToUInt8OrNull { static constexpr auto name = "toUInt8OrNull"; };
struct NameToUInt16OrNull { static constexpr auto name = "toUInt16OrNull"; };
struct NameToUInt32OrNull { static constexpr auto name = "toUInt32OrNull"; };
struct NameToUInt64OrNull { static constexpr auto name = "toUInt64OrNull"; };
struct NameToUInt128OrNull { static constexpr auto name = "toUInt128OrNull"; };
struct NameToUInt256OrNull { static constexpr auto name = "toUInt256OrNull"; };
struct NameToInt8OrNull { static constexpr auto name = "toInt8OrNull"; };
struct NameToInt16OrNull { static constexpr auto name = "toInt16OrNull"; };
struct NameToInt32OrNull { static constexpr auto name = "toInt32OrNull"; };
struct NameToInt64OrNull { static constexpr auto name = "toInt64OrNull"; };
struct NameToInt128OrNull { static constexpr auto name = "toInt128OrNull"; };
struct NameToInt256OrNull { static constexpr auto name = "toInt256OrNull"; };
struct NameToFloat32OrNull { static constexpr auto name = "toFloat32OrNull"; };
struct NameToFloat64OrNull { static constexpr auto name = "toFloat64OrNull"; };
struct NameToDateOrNull { static constexpr auto name = "toDateOrNull"; };
struct NameToDate32OrNull { static constexpr auto name = "toDate32OrNull"; };
struct NameToDateTimeOrNull { static constexpr auto name = "toDateTimeOrNull"; };
struct NameToDateTime64OrNull { static constexpr auto name = "toDateTime64OrNull"; };
struct NameToDecimal32OrNull { static constexpr auto name = "toDecimal32OrNull"; };
struct NameToDecimal64OrNull { static constexpr auto name = "toDecimal64OrNull"; };
struct NameToDecimal128OrNull { static constexpr auto name = "toDecimal128OrNull"; };
struct NameToDecimal256OrNull { static constexpr auto name = "toDecimal256OrNull"; };
struct NameToUUIDOrNull { static constexpr auto name = "toUUIDOrNull"; };
struct NameToIPv4OrNull { static constexpr auto name = "toIPv4OrNull"; };
struct NameToIPv6OrNull { static constexpr auto name = "toIPv6OrNull"; };

using FunctionToUInt8OrNull = FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt16OrNull = FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt32OrNull = FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt64OrNull = FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt128OrNull = FunctionConvertFromString<DataTypeUInt128, NameToUInt128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt256OrNull = FunctionConvertFromString<DataTypeUInt256, NameToUInt256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt8OrNull = FunctionConvertFromString<DataTypeInt8, NameToInt8OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt16OrNull = FunctionConvertFromString<DataTypeInt16, NameToInt16OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt32OrNull = FunctionConvertFromString<DataTypeInt32, NameToInt32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt64OrNull = FunctionConvertFromString<DataTypeInt64, NameToInt64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt128OrNull = FunctionConvertFromString<DataTypeInt128, NameToInt128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt256OrNull = FunctionConvertFromString<DataTypeInt256, NameToInt256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToFloat32OrNull = FunctionConvertFromString<DataTypeFloat32, NameToFloat32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToFloat64OrNull = FunctionConvertFromString<DataTypeFloat64, NameToFloat64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateOrNull = FunctionConvertFromString<DataTypeDate, NameToDateOrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDate32OrNull = FunctionConvertFromString<DataTypeDate32, NameToDate32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateTimeOrNull = FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateTime64OrNull = FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal32OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal64OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal128OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal256OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUUIDOrNull = FunctionConvertFromString<DataTypeUUID, NameToUUIDOrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToIPv4OrNull = FunctionConvertFromString<DataTypeIPv4, NameToIPv4OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToIPv6OrNull = FunctionConvertFromString<DataTypeIPv6, NameToIPv6OrNull, ConvertFromStringExceptionMode::Null>;

struct NameParseDateTimeBestEffort { static constexpr auto name = "parseDateTimeBestEffort"; };
struct NameParseDateTimeBestEffortOrZero { static constexpr auto name = "parseDateTimeBestEffortOrZero"; };
struct NameParseDateTimeBestEffortOrNull { static constexpr auto name = "parseDateTimeBestEffortOrNull"; };
struct NameParseDateTimeBestEffortUS { static constexpr auto name = "parseDateTimeBestEffortUS"; };
struct NameParseDateTimeBestEffortUSOrZero { static constexpr auto name = "parseDateTimeBestEffortUSOrZero"; };
struct NameParseDateTimeBestEffortUSOrNull { static constexpr auto name = "parseDateTimeBestEffortUSOrNull"; };
struct NameParseDateTime32BestEffort { static constexpr auto name = "parseDateTime32BestEffort"; };
struct NameParseDateTime32BestEffortOrZero { static constexpr auto name = "parseDateTime32BestEffortOrZero"; };
struct NameParseDateTime32BestEffortOrNull { static constexpr auto name = "parseDateTime32BestEffortOrNull"; };
struct NameParseDateTime64BestEffort { static constexpr auto name = "parseDateTime64BestEffort"; };
struct NameParseDateTime64BestEffortOrZero { static constexpr auto name = "parseDateTime64BestEffortOrZero"; };
struct NameParseDateTime64BestEffortOrNull { static constexpr auto name = "parseDateTime64BestEffortOrNull"; };
struct NameParseDateTime64BestEffortUS { static constexpr auto name = "parseDateTime64BestEffortUS"; };
struct NameParseDateTime64BestEffortUSOrZero { static constexpr auto name = "parseDateTime64BestEffortUSOrZero"; };
struct NameParseDateTime64BestEffortUSOrNull { static constexpr auto name = "parseDateTime64BestEffortUSOrNull"; };


using FunctionParseDateTimeBestEffort = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTimeBestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTimeBestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

using FunctionParseDateTimeBestEffortUS = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUS, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTimeBestEffortUSOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUSOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTimeBestEffortUSOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUSOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffortUS>;

using FunctionParseDateTime32BestEffort = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime32BestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime32BestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

using FunctionParseDateTime64BestEffort = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime64BestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime64BestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

using FunctionParseDateTime64BestEffortUS = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortUS, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTime64BestEffortUSOrZero = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortUSOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTime64BestEffortUSOrNull = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortUSOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffortUS>;


class ExecutableFunctionCast : public IExecutableFunction
{
public:
    using WrapperType = std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t)>;

    explicit ExecutableFunctionCast(
            WrapperType && wrapper_function_, const char * name_, std::optional<CastDiagnostic> diagnostic_)
            : wrapper_function(std::move(wrapper_function_)), name(name_), diagnostic(std::move(diagnostic_)) {}

    String getName() const override { return name; }

protected:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// drop second argument, pass others
        ColumnsWithTypeAndName new_arguments{arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2), std::end(arguments));

        try
        {
            return wrapper_function(new_arguments, result_type, nullptr, input_rows_count);
        }
        catch (Exception & e)
        {
            if (diagnostic)
                e.addMessage("while converting source column " + backQuoteIfNeed(diagnostic->column_from) +
                             " to destination column " + backQuoteIfNeed(diagnostic->column_to));
            throw;
        }
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// CAST(Nothing, T) -> T
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char * name;
    std::optional<CastDiagnostic> diagnostic;
};


struct FunctionCastName
{
    static constexpr auto name = "CAST";
};

class FunctionCast final : public IFunctionBase
{
public:
    using MonotonicityForRange = std::function<Monotonicity(const IDataType &, const Field &, const Field &)>;
    using WrapperType = std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t)>;

    FunctionCast(ContextPtr context_
            , const char * cast_name_
            , MonotonicityForRange && monotonicity_for_range_
            , const DataTypes & argument_types_
            , const DataTypePtr & return_type_
            , std::optional<CastDiagnostic> diagnostic_
            , CastType cast_type_)
        : cast_name(cast_name_), monotonicity_for_range(std::move(monotonicity_for_range_))
        , argument_types(argument_types_), return_type(return_type_), diagnostic(std::move(diagnostic_))
        , cast_type(cast_type_)
        , context(context_)
    {
    }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*sample_columns*/) const override
    {
        try
        {
            return std::make_unique<ExecutableFunctionCast>(
                prepareUnpackDictionaries(getArgumentTypes()[0], getResultType()), cast_name, diagnostic);
        }
        catch (Exception & e)
        {
            if (diagnostic)
                e.addMessage("while converting source column " + backQuoteIfNeed(diagnostic->column_from) +
                             " to destination column " + backQuoteIfNeed(diagnostic->column_to));
            throw;
        }
    }

    String getName() const override { return cast_name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool hasInformationAboutMonotonicity() const override
    {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return monotonicity_for_range(type, left, right);
    }

private:
    const char * cast_name;
    MonotonicityForRange monotonicity_for_range;

    DataTypes argument_types;
    DataTypePtr return_type;

    std::optional<CastDiagnostic> diagnostic;
    CastType cast_type;
    ContextPtr context;

    static WrapperType createFunctionAdaptor(FunctionPtr function, const DataTypePtr & from_type)
    {
        auto function_adaptor = std::make_unique<FunctionToOverloadResolverAdaptor>(function)->build({ColumnWithTypeAndName{nullptr, from_type, ""}});

        return [function_adaptor]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            return function_adaptor->execute(arguments, result_type, input_rows_count);
        };
    }

    static WrapperType createToNullableColumnWrapper()
    {
        return [] (ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            ColumnPtr res = result_type->createColumn();
            ColumnUInt8::Ptr col_null_map_to = ColumnUInt8::create(input_rows_count, true);
            return ColumnNullable::create(res->cloneResized(input_rows_count), std::move(col_null_map_to));
        };
    }

    template <typename ToDataType>
    WrapperType createWrapper(const DataTypePtr & from_type, const ToDataType * const to_type, bool requested_result_is_nullable) const
    {
        TypeIndex from_type_index = from_type->getTypeId();
        WhichDataType which(from_type_index);
        TypeIndex to_type_index = to_type->getTypeId();
        WhichDataType to(to_type_index);
        bool can_apply_accurate_cast = (cast_type == CastType::accurate || cast_type == CastType::accurateOrNull)
            && (which.isInt() || which.isUInt() || which.isFloat());
        can_apply_accurate_cast |= cast_type == CastType::accurate && which.isStringOrFixedString() && to.isNativeInteger();

        FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior;
        if (context)
            date_time_overflow_behavior = context->getSettingsRef()[Setting::date_time_overflow_behavior];

        if (requested_result_is_nullable && checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            /// In case when converting to Nullable type, we apply different parsing rule,
            /// that will not throw an exception but return NULL in case of malformed input.
            FunctionPtr function = FunctionConvertFromString<ToDataType, FunctionCastName, ConvertFromStringExceptionMode::Null>::create(context);
            return createFunctionAdaptor(function, from_type);
        }
        else if (!can_apply_accurate_cast)
        {
            FunctionPtr function = FunctionTo<ToDataType>::Type::create(context);
            return createFunctionAdaptor(function, from_type);
        }

        return [wrapper_cast_type = cast_type, from_type_index, to_type, date_time_overflow_behavior]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count)
        {
            ColumnPtr result_column;
            auto res = callOnIndexAndDataType<ToDataType>(from_type_index, [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using LeftDataType = typename Types::LeftType;
                using RightDataType = typename Types::RightType;

                if constexpr (IsDataTypeNumber<LeftDataType>)
                {
                    if constexpr (IsDataTypeDateOrDateTime<RightDataType>)
                    {
#define GENERATE_OVERFLOW_MODE_CASE(OVERFLOW_MODE, ADDITIONS) \
    case FormatSettings::DateTimeOverflowBehavior::OVERFLOW_MODE: \
        result_column \
            = ConvertImpl<LeftDataType, RightDataType, FunctionCastName, FormatSettings::DateTimeOverflowBehavior::OVERFLOW_MODE>:: \
                execute(arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, ADDITIONS()); \
        break;
                        if (wrapper_cast_type == CastType::accurate)
                        {
                            switch (date_time_overflow_behavior)
                            {
                                GENERATE_OVERFLOW_MODE_CASE(Throw, DateTimeAccurateConvertStrategyAdditions)
                                GENERATE_OVERFLOW_MODE_CASE(Ignore, DateTimeAccurateConvertStrategyAdditions)
                                GENERATE_OVERFLOW_MODE_CASE(Saturate, DateTimeAccurateConvertStrategyAdditions)
                            }
                        }
                        else
                        {
                            switch (date_time_overflow_behavior)
                            {
                                GENERATE_OVERFLOW_MODE_CASE(Throw, DateTimeAccurateOrNullConvertStrategyAdditions)
                                GENERATE_OVERFLOW_MODE_CASE(Ignore, DateTimeAccurateOrNullConvertStrategyAdditions)
                                GENERATE_OVERFLOW_MODE_CASE(Saturate, DateTimeAccurateOrNullConvertStrategyAdditions)
                            }
                        }
#undef GENERATE_OVERFLOW_MODE_CASE

                        return true;
                    }
                    else if constexpr (IsDataTypeNumber<RightDataType>)
                    {
                        if (wrapper_cast_type == CastType::accurate)
                        {
                            result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                                arguments,
                                result_type,
                                input_rows_count,
                                BehaviourOnErrorFromString::ConvertDefaultBehaviorTag,
                                AccurateConvertStrategyAdditions());
                        }
                        else
                        {
                            result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                                arguments,
                                result_type,
                                input_rows_count,
                                BehaviourOnErrorFromString::ConvertDefaultBehaviorTag,
                                AccurateOrNullConvertStrategyAdditions());
                        }

                        return true;
                    }
                }
                else if constexpr (IsDataTypeStringOrFixedString<LeftDataType>)
                {
                    if constexpr (IsDataTypeNumber<RightDataType>)
                    {
                        chassert(wrapper_cast_type == CastType::accurate);
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                            arguments,
                            result_type,
                            input_rows_count,
                            BehaviourOnErrorFromString::ConvertDefaultBehaviorTag,
                            AccurateConvertStrategyAdditions());
                    }
                    return true;
                }

                return false;
            });

            /// Additionally check if callOnIndexAndDataType wasn't called at all.
            if (!res)
            {
                if (wrapper_cast_type == CastType::accurateOrNull)
                {
                    auto nullable_column_wrapper = FunctionCast::createToNullableColumnWrapper();
                    return nullable_column_wrapper(arguments, result_type, column_nullable, input_rows_count);
                }
                else
                {
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                        "Conversion from {} to {} is not supported",
                        from_type_index, to_type->getName());
                }
            }

            return result_column;
        };
    }

    template <typename ToDataType>
    WrapperType createBoolWrapper(const DataTypePtr & from_type, const ToDataType * const to_type, bool requested_result_is_nullable) const
    {
        if (checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            if (cast_type == CastType::accurateOrNull)
            {
                return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
                {
                    return ConvertImplGenericFromString<false>::execute(arguments, result_type, column_nullable, input_rows_count, context);
                };
            }

            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
            {
                return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, context);
            };
        }

        return createWrapper<ToDataType>(from_type, to_type, requested_result_is_nullable);
    }

    WrapperType createUInt8ToBoolWrapper(const DataTypePtr from_type, const DataTypePtr to_type) const
    {
        return [from_type, to_type] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/) -> ColumnPtr
        {
            /// Special case when we convert UInt8 column to Bool column.
            /// both columns have type UInt8, but we shouldn't use identity wrapper,
            /// because Bool column can contain only 0 and 1.
            auto res_column = to_type->createColumn();
            const auto & data_from = checkAndGetColumn<ColumnUInt8>(*arguments[0].column).getData();
            auto & data_to = assert_cast<ColumnUInt8 *>(res_column.get())->getData();
            data_to.resize(data_from.size());
            for (size_t i = 0; i != data_from.size(); ++i)
                data_to[i] = static_cast<bool>(data_from[i]);
            return res_column;
        };
    }

    WrapperType createStringWrapper(const DataTypePtr & from_type) const
    {
        FunctionPtr function = FunctionToString::create(context);
        return createFunctionAdaptor(function, from_type);
    }

    WrapperType createFixedStringWrapper(const DataTypePtr & from_type, const size_t N) const
    {
        if (!isStringOrFixedString(from_type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CAST AS FixedString is only implemented for types String and FixedString");

        bool exception_mode_null = cast_type == CastType::accurateOrNull;
        return [exception_mode_null, N] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/)
        {
            if (exception_mode_null)
                return FunctionToFixedString::executeForN<ConvertToFixedStringExceptionMode::Null>(arguments, N);
            else
                return FunctionToFixedString::executeForN<ConvertToFixedStringExceptionMode::Throw>(arguments, N);
        };
    }

#define GENERATE_INTERVAL_CASE(INTERVAL_KIND) \
            case IntervalKind::Kind::INTERVAL_KIND: \
                return createFunctionAdaptor(FunctionConvert<DataTypeInterval, NameToInterval##INTERVAL_KIND, PositiveMonotonicity>::create(context), from_type);

    WrapperType createIntervalWrapper(const DataTypePtr & from_type, IntervalKind kind) const
    {
        switch (kind.kind)
        {
            GENERATE_INTERVAL_CASE(Nanosecond)
            GENERATE_INTERVAL_CASE(Microsecond)
            GENERATE_INTERVAL_CASE(Millisecond)
            GENERATE_INTERVAL_CASE(Second)
            GENERATE_INTERVAL_CASE(Minute)
            GENERATE_INTERVAL_CASE(Hour)
            GENERATE_INTERVAL_CASE(Day)
            GENERATE_INTERVAL_CASE(Week)
            GENERATE_INTERVAL_CASE(Month)
            GENERATE_INTERVAL_CASE(Quarter)
            GENERATE_INTERVAL_CASE(Year)
        }
        throw Exception{ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion to unexpected IntervalKind: {}", kind.toString()};
    }

#undef GENERATE_INTERVAL_CASE

    template <typename ToDataType>
    requires IsDataTypeDecimal<ToDataType>
    WrapperType createDecimalWrapper(const DataTypePtr & from_type, const ToDataType * to_type, bool requested_result_is_nullable) const
    {
        TypeIndex type_index = from_type->getTypeId();
        UInt32 scale = to_type->getScale();

        WhichDataType which(type_index);
        bool ok = which.isNativeInt() || which.isNativeUInt() || which.isDecimal() || which.isFloat() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64()
            || which.isStringOrFixedString();
        if (!ok)
        {
            if (cast_type == CastType::accurateOrNull)
                return createToNullableColumnWrapper();
            else
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
                    from_type->getName(), to_type->getName());
        }

        auto wrapper_cast_type = cast_type;

        return [wrapper_cast_type, type_index, scale, to_type, requested_result_is_nullable]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *column_nullable, size_t input_rows_count)
        {
            ColumnPtr result_column;
            auto res = callOnIndexAndDataType<ToDataType>(type_index, [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using LeftDataType = typename Types::LeftType;
                using RightDataType = typename Types::RightType;

                if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType> && !std::is_same_v<DataTypeDateTime64, RightDataType>)
                {
                    if (wrapper_cast_type == CastType::accurate)
                    {
                        AccurateConvertStrategyAdditions additions;
                        additions.scale = scale;
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                            arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, additions);

                        return true;
                    }
                    else if (wrapper_cast_type == CastType::accurateOrNull)
                    {
                        AccurateOrNullConvertStrategyAdditions additions;
                        additions.scale = scale;
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                            arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, additions);

                        return true;
                    }
                }
                else if constexpr (std::is_same_v<LeftDataType, DataTypeString>)
                {
                    if (requested_result_is_nullable)
                    {
                        /// Consistent with CAST(Nullable(String) AS Nullable(Numbers))
                        /// In case when converting to Nullable type, we apply different parsing rule,
                        /// that will not throw an exception but return NULL in case of malformed input.
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                            arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertReturnNullOnErrorTag, scale);

                        return true;
                    }
                }

                result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, scale);

                return true;
            });

            /// Additionally check if callOnIndexAndDataType wasn't called at all.
            if (!res)
            {
                if (wrapper_cast_type == CastType::accurateOrNull)
                {
                    auto nullable_column_wrapper = FunctionCast::createToNullableColumnWrapper();
                    return nullable_column_wrapper(arguments, result_type, column_nullable, input_rows_count);
                }
                else
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                        "Conversion from {} to {} is not supported",
                        type_index, to_type->getName());
            }

            return result_column;
        };
    }

    WrapperType createAggregateFunctionWrapper(const DataTypePtr & from_type_untyped, const DataTypeAggregateFunction * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
            {
                return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, context);
            };
        }
        else if (const auto * agg_type = checkAndGetDataType<DataTypeAggregateFunction>(from_type_untyped.get()))
        {
            if (agg_type->getFunction()->haveSameStateRepresentation(*to_type->getFunction()))
            {
                return [function = to_type->getFunction()](
                           ColumnsWithTypeAndName & arguments,
                           const DataTypePtr & /* result_type */,
                           const ColumnNullable * /* nullable_source */,
                           size_t /*input_rows_count*/) -> ColumnPtr
                {
                    const auto & argument_column = arguments.front();
                    const auto * col_agg = checkAndGetColumn<ColumnAggregateFunction>(argument_column.column.get());
                    if (col_agg)
                    {
                        auto new_col_agg = ColumnAggregateFunction::create(*col_agg);
                        new_col_agg->set(function);
                        return new_col_agg;
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Illegal column {} for function CAST AS AggregateFunction",
                            argument_column.column->getName());
                    }
                };
            }
        }

        if (cast_type == CastType::accurateOrNull)
            return createToNullableColumnWrapper();
        else
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
                from_type_untyped->getName(), to_type->getName());
    }

    WrapperType createArrayWrapper(const DataTypePtr & from_type_untyped, const DataTypeArray & to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
            {
                return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, context);
            };
        }

        DataTypePtr from_type_holder;
        const auto * from_type = checkAndGetDataType<DataTypeArray>(from_type_untyped.get());
        const auto * from_type_map = checkAndGetDataType<DataTypeMap>(from_type_untyped.get());

        /// Convert from Map
        if (from_type_map)
        {
            /// Recreate array of unnamed tuples because otherwise it may work
            /// unexpectedly while converting to array of named tuples.
            from_type_holder = from_type_map->getNestedTypeWithUnnamedTuple();
            from_type = assert_cast<const DataTypeArray *>(from_type_holder.get());
        }

        if (!from_type)
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "CAST AS Array can only be performed between same-dimensional Array, Map or String types");
        }

        DataTypePtr from_nested_type = from_type->getNestedType();

        /// In query SELECT CAST([] AS Array(Array(String))) from type is Array(Nothing)
        bool from_empty_array = isNothing(from_nested_type);

        if (from_type->getNumberOfDimensions() != to_type.getNumberOfDimensions() && !from_empty_array)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "CAST AS Array can only be performed between same-dimensional array types");

        const DataTypePtr & to_nested_type = to_type.getNestedType();

        /// Prepare nested type conversion
        const auto nested_function = prepareUnpackDictionaries(from_nested_type, to_nested_type);

        return [nested_function, from_nested_type, to_nested_type](
                ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto & argument_column = arguments.front();

            const ColumnArray * col_array = nullptr;

            if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(argument_column.column.get()))
                col_array = &col_map->getNestedColumn();
            else
                col_array = checkAndGetColumn<ColumnArray>(argument_column.column.get());

            if (col_array)
            {
                /// create columns for converting nested column containing original and result columns
                ColumnsWithTypeAndName nested_columns{{ col_array->getDataPtr(), from_nested_type, "" }};

                /// convert nested column
                auto result_column = nested_function(nested_columns, to_nested_type, nullable_source, nested_columns.front().column->size());

                /// set converted nested column to result
                return ColumnArray::create(result_column, col_array->getOffsetsPtr());
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Illegal column {} for function CAST AS Array",
                    argument_column.column->getName());
            }
        };
    }

    using ElementWrappers = std::vector<WrapperType>;

    ElementWrappers getElementWrappers(const DataTypes & from_element_types, const DataTypes & to_element_types) const
    {
        ElementWrappers element_wrappers;
        element_wrappers.reserve(from_element_types.size());

        /// Create conversion wrapper for each element in tuple
        for (size_t i = 0; i < from_element_types.size(); ++i)
        {
            const DataTypePtr & from_element_type = from_element_types[i];
            const DataTypePtr & to_element_type = to_element_types[i];
            element_wrappers.push_back(prepareUnpackDictionaries(from_element_type, to_element_type));
        }

        return element_wrappers;
    }

    WrapperType createTupleWrapper(const DataTypePtr & from_type_untyped, const DataTypeTuple * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
            {
                return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, context);
            };
        }

        const auto * from_type = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get());
        if (!from_type)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "CAST AS Tuple can only be performed between tuple types or from String.\n"
                            "Left type: {}, right type: {}", from_type_untyped->getName(), to_type->getName());

        const auto & from_element_types = from_type->getElements();
        const auto & to_element_types = to_type->getElements();

        std::vector<WrapperType> element_wrappers;
        std::vector<std::optional<size_t>> to_reverse_index;

        /// For named tuples allow conversions for tuples with
        /// different sets of elements. If element exists in @to_type
        /// and doesn't exist in @to_type it will be filled by default values.
        if (from_type->haveExplicitNames() && to_type->haveExplicitNames())
        {
            const auto & from_names = from_type->getElementNames();
            std::unordered_map<String, size_t> from_positions;
            from_positions.reserve(from_names.size());
            for (size_t i = 0; i < from_names.size(); ++i)
                from_positions[from_names[i]] = i;

            const auto & to_names = to_type->getElementNames();
            element_wrappers.reserve(to_names.size());
            to_reverse_index.reserve(from_names.size());

            for (size_t i = 0; i < to_names.size(); ++i)
            {
                auto it = from_positions.find(to_names[i]);
                if (it != from_positions.end())
                {
                    element_wrappers.emplace_back(prepareUnpackDictionaries(from_element_types[it->second], to_element_types[i]));
                    to_reverse_index.emplace_back(it->second);
                }
                else
                {
                    element_wrappers.emplace_back();
                    to_reverse_index.emplace_back();
                }
            }
        }
        else
        {
            if (from_element_types.size() != to_element_types.size())
                throw Exception(ErrorCodes::TYPE_MISMATCH, "CAST AS Tuple can only be performed between tuple types "
                                "with the same number of elements or from String.\nLeft type: {}, right type: {}",
                                from_type->getName(), to_type->getName());

            element_wrappers = getElementWrappers(from_element_types, to_element_types);
            to_reverse_index.reserve(to_element_types.size());
            for (size_t i = 0; i < to_element_types.size(); ++i)
                to_reverse_index.emplace_back(i);
        }

        return [element_wrappers, from_element_types, to_element_types, to_reverse_index]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();

            size_t tuple_size = to_element_types.size();
            const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*col);

            Columns converted_columns(tuple_size);

            /// invoke conversion for each element
            for (size_t i = 0; i < tuple_size; ++i)
            {
                if (to_reverse_index[i])
                {
                    size_t from_idx = *to_reverse_index[i];
                    ColumnsWithTypeAndName element = {{column_tuple.getColumns()[from_idx], from_element_types[from_idx], "" }};
                    converted_columns[i] = element_wrappers[i](element, to_element_types[i], nullable_source, input_rows_count);
                }
                else
                {
                    converted_columns[i] = to_element_types[i]->createColumn()->cloneResized(input_rows_count);
                }
            }

            return ColumnTuple::create(converted_columns);
        };
    }

    /// The case of: tuple([key1, key2, ..., key_n], [value1, value2, ..., value_n])
    WrapperType createTupleToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
    {
        return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*col);

            Columns offsets(2);
            Columns converted_columns(2);
            for (size_t i = 0; i < 2; ++i)
            {
                const auto & column_array = assert_cast<const ColumnArray &>(column_tuple.getColumn(i));
                ColumnsWithTypeAndName element = {{column_array.getDataPtr(), from_kv_types[i], ""}};
                converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullable_source, (element[0].column)->size());
                offsets[i] = column_array.getOffsetsPtr();
            }

            const auto & keys_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets[0]).getData();
            const auto & values_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets[1]).getData();
            if (keys_offsets != values_offsets)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "CAST AS Map can only be performed from tuple of arrays with equal sizes.");

            return ColumnMap::create(converted_columns[0], converted_columns[1], offsets[0]);
        };
    }

    WrapperType createMapToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
    {
        return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();
            const auto & column_map = typeid_cast<const ColumnMap &>(*col);
            const auto & nested_data = column_map.getNestedData();

            Columns converted_columns(2);
            for (size_t i = 0; i < 2; ++i)
            {
                ColumnsWithTypeAndName element = {{nested_data.getColumnPtr(i), from_kv_types[i], ""}};
                converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullable_source, (element[0].column)->size());
            }

            return ColumnMap::create(converted_columns[0], converted_columns[1], column_map.getNestedColumn().getOffsetsPtr());
        };
    }

    /// The case of: [(key1, value1), (key2, value2), ...]
    WrapperType createArrayToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
    {
        return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();
            const auto & column_array = typeid_cast<const ColumnArray &>(*col);
            const auto & nested_data = typeid_cast<const ColumnTuple &>(column_array.getData());

            Columns converted_columns(2);
            for (size_t i = 0; i < 2; ++i)
            {
                ColumnsWithTypeAndName element = {{nested_data.getColumnPtr(i), from_kv_types[i], ""}};
                converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullable_source, (element[0].column)->size());
            }

            return ColumnMap::create(converted_columns[0], converted_columns[1], column_array.getOffsetsPtr());
        };
    }


    WrapperType createMapWrapper(const DataTypePtr & from_type_untyped, const DataTypeMap * to_type) const
    {
        if (const auto * from_tuple = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get()))
        {
            if (from_tuple->getElements().size() != 2)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "CAST AS Map from tuple requires 2 elements. "
                    "Left type: {}, right type: {}",
                    from_tuple->getName(),
                    to_type->getName());

            DataTypes from_kv_types;
            const auto & to_kv_types = to_type->getKeyValueTypes();

            for (const auto & elem : from_tuple->getElements())
            {
                const auto * type_array = checkAndGetDataType<DataTypeArray>(elem.get());
                if (!type_array)
                    throw Exception(ErrorCodes::TYPE_MISMATCH,
                        "CAST AS Map can only be performed from tuples of array. Got: {}", from_tuple->getName());

                from_kv_types.push_back(type_array->getNestedType());
            }

            return createTupleToMapWrapper(from_kv_types, to_kv_types);
        }
        else if (const auto * from_array = typeid_cast<const DataTypeArray *>(from_type_untyped.get()))
        {
            if (typeid_cast<const DataTypeNothing *>(from_array->getNestedType().get()))
                return [nested = to_type->getNestedType()](ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t size)
                {
                    return ColumnMap::create(nested->createColumnConstWithDefaultValue(size)->convertToFullColumnIfConst());
                };

            const auto * nested_tuple = typeid_cast<const DataTypeTuple *>(from_array->getNestedType().get());
            if (!nested_tuple || nested_tuple->getElements().size() != 2)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "CAST AS Map from array requires nested tuple of 2 elements. "
                    "Left type: {}, right type: {}",
                    from_array->getName(),
                    to_type->getName());

            return createArrayToMapWrapper(nested_tuple->getElements(), to_type->getKeyValueTypes());
        }
        else if (const auto * from_type = checkAndGetDataType<DataTypeMap>(from_type_untyped.get()))
        {
            return createMapToMapWrapper(from_type->getKeyValueTypes(), to_type->getKeyValueTypes());
        }
        else
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Unsupported types to CAST AS Map. "
                "Left type: {}, right type: {}", from_type_untyped->getName(), to_type->getName());
        }
    }

    WrapperType createTupleToObjectWrapper(const DataTypeTuple & from_tuple, bool has_nullable_subcolumns) const
    {
        if (!from_tuple.haveExplicitNames())
            throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Cast to Object can be performed only from flatten Named Tuple. Got: {}", from_tuple.getName());

        PathsInData paths;
        DataTypes from_types;

        std::tie(paths, from_types) = flattenTuple(from_tuple.getPtr());
        auto to_types = from_types;

        for (auto & type : to_types)
        {
            if (isTuple(type) || isNested(type))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Cast to Object can be performed only from flatten Named Tuple. Got: {}",
                    from_tuple.getName());

            type = recursiveRemoveLowCardinality(type);
        }

        return [element_wrappers = getElementWrappers(from_types, to_types),
            has_nullable_subcolumns, from_types, to_types, paths]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t input_rows_count)
        {
            size_t tuple_size = to_types.size();
            auto flattened_column = flattenTuple(arguments.front().column);
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*flattened_column);

            if (tuple_size != column_tuple.getColumns().size())
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Expected tuple with {} subcolumn, but got {} subcolumns",
                    tuple_size, column_tuple.getColumns().size());

            auto res = ColumnObjectDeprecated::create(has_nullable_subcolumns);
            for (size_t i = 0; i < tuple_size; ++i)
            {
                ColumnsWithTypeAndName element = {{column_tuple.getColumns()[i], from_types[i], "" }};
                auto converted_column = element_wrappers[i](element, to_types[i], nullable_source, input_rows_count);
                res->addSubcolumn(paths[i], converted_column->assumeMutable());
            }

            return res;
        };
    }

    WrapperType createMapToObjectWrapper(const DataTypeMap & from_map, bool has_nullable_subcolumns) const
    {
        auto key_value_types = from_map.getKeyValueTypes();

        if (!isStringOrFixedString(key_value_types[0]))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Cast to Object from Map can be performed only from Map "
                "with String or FixedString key. Got: {}", from_map.getName());

        const auto & value_type = key_value_types[1];
        auto to_value_type = value_type;

        if (!has_nullable_subcolumns && value_type->isNullable())
            to_value_type = removeNullable(value_type);

        if (has_nullable_subcolumns && !value_type->isNullable())
            to_value_type = makeNullable(value_type);

        DataTypes to_key_value_types{std::make_shared<DataTypeString>(), std::move(to_value_type)};
        auto element_wrappers = getElementWrappers(key_value_types, to_key_value_types);

        return [has_nullable_subcolumns, element_wrappers, key_value_types, to_key_value_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t) -> ColumnPtr
        {
            const auto & column_map = assert_cast<const ColumnMap &>(*arguments.front().column);
            const auto & offsets = column_map.getNestedColumn().getOffsets();
            auto key_value_columns = column_map.getNestedData().getColumnsCopy();

            for (size_t i = 0; i < 2; ++i)
            {
                ColumnsWithTypeAndName element{{key_value_columns[i], key_value_types[i], ""}};
                key_value_columns[i] = element_wrappers[i](element, to_key_value_types[i], nullable_source, key_value_columns[i]->size());
            }

            const auto & key_column_str = assert_cast<const ColumnString &>(*key_value_columns[0]);
            const auto & value_column = *key_value_columns[1];

            using SubcolumnsMap = HashMap<StringRef, MutableColumnPtr, StringRefHash>;
            SubcolumnsMap subcolumns;

            for (size_t row = 0; row < offsets.size(); ++row)
            {
                for (size_t i = offsets[static_cast<ssize_t>(row) - 1]; i < offsets[row]; ++i)
                {
                    auto ref = key_column_str.getDataAt(i);

                    bool inserted;
                    SubcolumnsMap::LookupResult it;
                    subcolumns.emplace(ref, it, inserted);
                    auto & subcolumn = it->getMapped();

                    if (inserted)
                        subcolumn = value_column.cloneEmpty()->cloneResized(row);

                    /// Map can have duplicated keys. We insert only first one.
                    if (subcolumn->size() == row)
                        subcolumn->insertFrom(value_column, i);
                }

                /// Insert default values for keys missed in current row.
                for (const auto & [_, subcolumn] : subcolumns)
                    if (subcolumn->size() == row)
                        subcolumn->insertDefault();
            }

            auto column_object = ColumnObjectDeprecated::create(has_nullable_subcolumns);
            for (auto && [key, subcolumn] : subcolumns)
            {
                PathInData path(key.toView());
                column_object->addSubcolumn(path, std::move(subcolumn));
            }

            return column_object;
        };
    }

    WrapperType createObjectDeprecatedWrapper(const DataTypePtr & from_type, const DataTypeObjectDeprecated * to_type) const
    {
        if (const auto * from_tuple = checkAndGetDataType<DataTypeTuple>(from_type.get()))
        {
            return createTupleToObjectWrapper(*from_tuple, to_type->hasNullableSubcolumns());
        }
        else if (const auto * from_map = checkAndGetDataType<DataTypeMap>(from_type.get()))
        {
            return createMapToObjectWrapper(*from_map, to_type->hasNullableSubcolumns());
        }
        else if (checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count)
            {
                auto res = ConvertImplGenericFromString<true>::execute(arguments, result_type, nullable_source, input_rows_count, context)->assumeMutable();
                res->finalize();
                return res;
            };
        }
        else if (checkAndGetDataType<DataTypeObjectDeprecated>(from_type.get()))
        {
            return [is_nullable = to_type->hasNullableSubcolumns()] (ColumnsWithTypeAndName & arguments, const DataTypePtr & , const ColumnNullable * , size_t) -> ColumnPtr
            {
                const auto & column_object = assert_cast<const ColumnObjectDeprecated &>(*arguments.front().column);
                auto res = ColumnObjectDeprecated::create(is_nullable);
                for (size_t i = 0; i < column_object.size(); i++)
                    res->insert(column_object[i]);

                res->finalize();
                return res;
            };
        }

        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Cast to Object can be performed only from flatten named Tuple, Map or String. Got: {}", from_type->getName());
    }

    WrapperType createObjectWrapper(const DataTypePtr & from_type, const DataTypeObject * to_object) const
    {
        if (checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count)
            {
                auto res = ConvertImplGenericFromString<true>::execute(arguments, result_type, nullable_source, input_rows_count, context)->assumeMutable();
                res->finalize();
                return res;
            };
        }

        /// TODO: support CAST between JSON types with different parameters
        ///       support CAST from Map to JSON
        ///       support CAST from Tuple to JSON
        ///       support CAST from Object('json') to JSON
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cast to {} can be performed only from String. Got: {}", magic_enum::enum_name(to_object->getSchemaFormat()), from_type->getName());
    }

    WrapperType createVariantToVariantWrapper(const DataTypeVariant & from_variant, const DataTypeVariant & to_variant) const
    {
        /// We support only extension of variant type, so, only new types can be added.
        /// For example: Variant(T1, T2) -> Variant(T1, T2, T3) is supported, but Variant(T1, T2) -> Variant(T1, T3) is not supported.
        /// We want to extend Variant type for free without rewriting the data, but we sort data types inside Variant during type creation
        /// (we do it because we want Variant(T1, T2) to be the same as Variant(T2, T1)), but after extension the order of variant types
        /// (and so their discriminators) can be different. For example: Variant(T1, T3) -> Variant(T1, T2, T3).
        /// To avoid full rewrite of discriminators column, ColumnVariant supports it's local order of variant columns (and so local
        /// discriminators) and stores mapping global order -> local order.
        /// So, to extend Variant with new types for free, we should keep old local order for old variants, append new variants and change
        /// mapping global order -> local order according to the new global order.

        /// Create map (new variant type) -> (it's global discriminator in new order).
        const auto & new_variants = to_variant.getVariants();
        std::unordered_map<String, ColumnVariant::Discriminator> new_variant_types_to_new_global_discriminator;
        new_variant_types_to_new_global_discriminator.reserve(new_variants.size());
        for (size_t i = 0; i != new_variants.size(); ++i)
            new_variant_types_to_new_global_discriminator[new_variants[i]->getName()] = i;

        /// Create set of old variant types.
        const auto & old_variants = from_variant.getVariants();
        std::unordered_map<String, ColumnVariant::Discriminator> old_variant_types_to_old_global_discriminator;
        old_variant_types_to_old_global_discriminator.reserve(old_variants.size());
        for (size_t i = 0; i != old_variants.size(); ++i)
            old_variant_types_to_old_global_discriminator[old_variants[i]->getName()] = i;

        /// Check that the set of old variants types is a subset of new variant types and collect new global discriminator for each old global discriminator.
        std::unordered_map<ColumnVariant::Discriminator, ColumnVariant::Discriminator> old_global_discriminator_to_new;
        old_global_discriminator_to_new.reserve(old_variants.size());
        for (const auto & [old_variant_type, old_discriminator] : old_variant_types_to_old_global_discriminator)
        {
            auto it = new_variant_types_to_new_global_discriminator.find(old_variant_type);
            if (it == new_variant_types_to_new_global_discriminator.end())
                throw Exception(
                    ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Cannot convert type {} to {}. Conversion between Variant types is allowed only when new Variant type is an extension "
                    "of an initial one", from_variant.getName(), to_variant.getName());
            old_global_discriminator_to_new[old_discriminator] = it->second;
        }

        /// Collect variant types and their global discriminators that should be added to the old Variant to get the new Variant.
        std::vector<std::pair<DataTypePtr, ColumnVariant::Discriminator>> variant_types_and_discriminators_to_add;
        variant_types_and_discriminators_to_add.reserve(new_variants.size() - old_variants.size());
        for (size_t i = 0; i != new_variants.size(); ++i)
        {
            if (!old_variant_types_to_old_global_discriminator.contains(new_variants[i]->getName()))
                variant_types_and_discriminators_to_add.emplace_back(new_variants[i], i);
        }

        return [old_global_discriminator_to_new, variant_types_and_discriminators_to_add]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t) -> ColumnPtr
        {
            const auto & column_variant = assert_cast<const ColumnVariant &>(*arguments.front().column.get());
            size_t num_old_variants = column_variant.getNumVariants();
            Columns new_variant_columns;
            new_variant_columns.reserve(num_old_variants + variant_types_and_discriminators_to_add.size());
            std::vector<ColumnVariant::Discriminator> new_local_to_global_discriminators;
            new_local_to_global_discriminators.reserve(num_old_variants + variant_types_and_discriminators_to_add.size());
            for (size_t i = 0; i != num_old_variants; ++i)
            {
                new_variant_columns.push_back(column_variant.getVariantPtrByLocalDiscriminator(i));
                new_local_to_global_discriminators.push_back(old_global_discriminator_to_new.at(column_variant.globalDiscriminatorByLocal(i)));
            }

            for (const auto & [new_variant_type, new_global_discriminator] : variant_types_and_discriminators_to_add)
            {
                new_variant_columns.push_back(new_variant_type->createColumn());
                new_local_to_global_discriminators.push_back(new_global_discriminator);
            }

            return ColumnVariant::create(column_variant.getLocalDiscriminatorsPtr(), column_variant.getOffsetsPtr(), new_variant_columns, new_local_to_global_discriminators);
        };
    }

    /// Create wrapper only if we support this conversion.
    WrapperType createWrapperIfCanConvert(const DataTypePtr & from, const DataTypePtr & to) const
    {
        try
        {
            /// We can avoid try/catch here if we will implement check that 2 types can be casted, but it
            /// requires quite a lot of work. By now let's simply use try/catch.
            /// First, check that we can create a wrapper.
            WrapperType wrapper = prepareUnpackDictionaries(from, to);
            /// Second, check if we can perform a conversion on column with default value.
            /// (we cannot just check empty column as we do some checks only during iteration over rows).
            auto test_col = from->createColumn();
            test_col->insertDefault();
            ColumnsWithTypeAndName column_from = {{test_col->getPtr(), from, "" }};
            wrapper(column_from, to, nullptr, 1);
            return wrapper;
        }
        catch (const Exception &)
        {
            return {};
        }
    }

    WrapperType createVariantToColumnWrapper(const DataTypeVariant & from_variant, const DataTypePtr & to_type) const
    {
        const auto & variant_types = from_variant.getVariants();
        std::vector<WrapperType> variant_wrappers;
        variant_wrappers.reserve(variant_types.size());

        /// Create conversion wrapper for each variant.
        for (const auto & variant_type : variant_types)
        {
            WrapperType wrapper;
            if (cast_type == CastType::accurateOrNull)
            {
                /// Create wrapper only if we support conversion from variant to the resulting type.
                wrapper = createWrapperIfCanConvert(variant_type, to_type);
            }
            else
            {
                wrapper = prepareUnpackDictionaries(variant_type, to_type);
            }
            variant_wrappers.push_back(wrapper);
        }

        return [variant_wrappers, variant_types, to_type]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            const auto & column_variant = assert_cast<const ColumnVariant &>(*arguments.front().column.get());

            /// First, cast each variant to the result type.
            std::vector<ColumnPtr> casted_variant_columns;
            casted_variant_columns.reserve(variant_types.size());
            for (size_t i = 0; i != variant_types.size(); ++i)
            {
                auto variant_col = column_variant.getVariantPtrByGlobalDiscriminator(i);
                ColumnsWithTypeAndName variant = {{variant_col, variant_types[i], "" }};
                const auto & variant_wrapper = variant_wrappers[i];
                ColumnPtr casted_variant;
                /// Check if we have wrapper for this variant.
                if (variant_wrapper)
                    casted_variant = variant_wrapper(variant, result_type, nullptr, variant_col->size());
                casted_variant_columns.push_back(std::move(casted_variant));
            }

            /// Second, construct resulting column from casted variant columns according to discriminators.
            const auto & local_discriminators = column_variant.getLocalDiscriminators();
            auto res = result_type->createColumn();
            res->reserve(input_rows_count);
            for (size_t i = 0; i != input_rows_count; ++i)
            {
                auto global_discr = column_variant.globalDiscriminatorByLocal(local_discriminators[i]);
                if (global_discr == ColumnVariant::NULL_DISCRIMINATOR || !casted_variant_columns[global_discr])
                    res->insertDefault();
                else
                    res->insertFrom(*casted_variant_columns[global_discr], column_variant.offsetAt(i));
            }

            return res;
        };
    }

    static ColumnPtr createVariantFromDescriptorsAndOneNonEmptyVariant(const DataTypes & variant_types, const ColumnPtr & discriminators, const ColumnPtr & variant, ColumnVariant::Discriminator variant_discr)
    {
        Columns variants;
        variants.reserve(variant_types.size());
        for (size_t i = 0; i != variant_types.size(); ++i)
        {
            if (i == variant_discr)
                variants.emplace_back(variant);
            else
                variants.push_back(variant_types[i]->createColumn());
        }

        return ColumnVariant::create(discriminators, variants);
    }

    WrapperType createStringToVariantWrapper() const
    {
        return [&](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            auto column = arguments[0].column->convertToFullColumnIfLowCardinality();
            auto args = arguments;
            args[0].column = column;

            const ColumnNullable * column_nullable = nullptr;
            if (isColumnNullable(*args[0].column))
            {
                column_nullable = assert_cast<const ColumnNullable *>(args[0].column.get());
                args[0].column = column_nullable->getNestedColumnPtr();
            }

            args[0].type = removeNullable(removeLowCardinality(args[0].type));

            if (cast_type == CastType::accurateOrNull)
                return ConvertImplGenericFromString<false>::execute(args, result_type, column_nullable, input_rows_count, context);
            return ConvertImplGenericFromString<true>::execute(args, result_type, column_nullable, input_rows_count, context);
        };
    }

    WrapperType createColumnToVariantWrapper(const DataTypePtr & from_type, const DataTypeVariant & to_variant) const
    {
        /// We allow converting NULL to Variant(...) as Variant can store NULLs.
        if (from_type->onlyNull())
        {
            return [](ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
            {
                auto result_column = result_type->createColumn();
                result_column->insertManyDefaults(input_rows_count);
                return result_column;
            };
        }

        auto variant_discr_opt = to_variant.tryGetVariantDiscriminator(removeNullableOrLowCardinalityNullable(from_type)->getName());
        /// Cast String to Variant through parsing if it's not Variant(String).
        if (isStringOrFixedString(removeNullable(removeLowCardinality(from_type))) && (!variant_discr_opt || to_variant.getVariants().size() > 1))
            return createStringToVariantWrapper();

        if (!variant_discr_opt)
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert type {} to {}. Conversion to Variant allowed only for types from this Variant", from_type->getName(), to_variant.getName());

        return [variant_discr = *variant_discr_opt]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t) -> ColumnPtr
        {
            const auto & result_variant_type = assert_cast<const DataTypeVariant &>(*result_type);
            const auto & variant_types = result_variant_type.getVariants();
            if (const ColumnNullable * col_nullable = typeid_cast<const ColumnNullable *>(arguments.front().column.get()))
            {
                const auto & column = col_nullable->getNestedColumnPtr();
                const auto & null_map = col_nullable->getNullMapData();
                IColumn::Filter filter;
                filter.reserve(column->size());
                auto discriminators = ColumnVariant::ColumnDiscriminators::create();
                auto & discriminators_data = discriminators->getData();
                discriminators_data.reserve(column->size());
                size_t variant_size_hint = 0;
                for (size_t i = 0; i != column->size(); ++i)
                {
                    if (null_map[i])
                    {
                        discriminators_data.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                        filter.push_back(0);
                    }
                    else
                    {
                        discriminators_data.push_back(variant_discr);
                        filter.push_back(1);
                        ++variant_size_hint;
                    }
                }

                ColumnPtr variant_column;
                /// If there were no NULLs, just use the column.
                if (variant_size_hint == column->size())
                    variant_column = column;
                /// Otherwise we should use filtered column.
                else
                    variant_column = column->filter(filter, variant_size_hint);
                return createVariantFromDescriptorsAndOneNonEmptyVariant(variant_types, std::move(discriminators), variant_column, variant_discr);
            }
            else if (isColumnLowCardinalityNullable(*arguments.front().column))
            {
                const auto & column = arguments.front().column;

                /// Variant column cannot have LowCardinality(Nullable(...)) variant, as Variant column stores NULLs itself.
                /// We should create a null-map, insert NULL_DISCRIMINATOR on NULL values and filter initial column.
                const auto & col_lc = assert_cast<const ColumnLowCardinality &>(*column);
                const auto & indexes = col_lc.getIndexes();
                auto null_index = col_lc.getDictionary().getNullValueIndex();
                IColumn::Filter filter;
                filter.reserve(col_lc.size());
                auto discriminators = ColumnVariant::ColumnDiscriminators::create();
                auto & discriminators_data = discriminators->getData();
                discriminators_data.reserve(col_lc.size());
                size_t variant_size_hint = 0;
                for (size_t i = 0; i != col_lc.size(); ++i)
                {
                    if (indexes.getUInt(i) == null_index)
                    {
                        discriminators_data.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                        filter.push_back(0);
                    }
                    else
                    {
                        discriminators_data.push_back(variant_discr);
                        filter.push_back(1);
                        ++variant_size_hint;
                    }
                }

                MutableColumnPtr variant_column;
                /// If there were no NULLs, we can just clone the column.
                if (variant_size_hint == col_lc.size())
                    variant_column = IColumn::mutate(column);
                /// Otherwise we should filter column.
                else
                    variant_column = column->filter(filter, variant_size_hint)->assumeMutable();

                assert_cast<ColumnLowCardinality &>(*variant_column).nestedRemoveNullable();
                return createVariantFromDescriptorsAndOneNonEmptyVariant(variant_types, std::move(discriminators), std::move(variant_column), variant_discr);
            }
            else
            {
                const auto & column = arguments.front().column;
                auto discriminators = ColumnVariant::ColumnDiscriminators::create();
                discriminators->getData().resize_fill(column->size(), variant_discr);
                return createVariantFromDescriptorsAndOneNonEmptyVariant(variant_types, std::move(discriminators), column, variant_discr);
            }
        };
    }

    /// Wrapper for conversion to/from Variant type
    WrapperType createVariantWrapper(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        if (const auto * from_variant = checkAndGetDataType<DataTypeVariant>(from_type.get()))
        {
            if (const auto * to_variant = checkAndGetDataType<DataTypeVariant>(to_type.get()))
                return createVariantToVariantWrapper(*from_variant, *to_variant);

            return createVariantToColumnWrapper(*from_variant, to_type);
        }

        return createColumnToVariantWrapper(from_type, assert_cast<const DataTypeVariant &>(*to_type));
    }

    WrapperType createDynamicToColumnWrapper(const DataTypePtr &) const
    {
        return [this]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            /// When casting Dynamic to regular column we should cast all variants from current Dynamic column
            /// and construct the result based on discriminators.
            const auto & column_dynamic = assert_cast<const ColumnDynamic &>(*arguments.front().column.get());
            const auto & variant_column = column_dynamic.getVariantColumn();
            const auto & variant_info = column_dynamic.getVariantInfo();

            /// First, cast usual variants to result type.
            const auto & variant_types = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
            std::vector<ColumnPtr> casted_variant_columns;
            casted_variant_columns.reserve(variant_types.size());
            for (size_t i = 0; i != variant_types.size(); ++i)
            {
                /// Skip shared variant, it will be processed later.
                if (i == column_dynamic.getSharedVariantDiscriminator())
                {
                    casted_variant_columns.push_back(nullptr);
                    continue;
                }

                const auto & variant_col = variant_column.getVariantPtrByGlobalDiscriminator(i);
                ColumnsWithTypeAndName variant = {{variant_col, variant_types[i], ""}};
                WrapperType variant_wrapper;
                if (cast_type == CastType::accurateOrNull)
                    /// Create wrapper only if we support conversion from variant to the resulting type.
                    variant_wrapper = createWrapperIfCanConvert(variant_types[i], result_type);
                else
                    variant_wrapper = prepareUnpackDictionaries(variant_types[i], result_type);

                ColumnPtr casted_variant;
                /// Check if we have wrapper for this variant.
                if (variant_wrapper)
                    casted_variant = variant_wrapper(variant, result_type, nullptr, variant_col->size());
                casted_variant_columns.push_back(casted_variant);
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
                        type->getDefaultSerialization()->deserializeBinary(*variant_columns_from_shared_variant[it->second], buf, format_settings);
                    }
                    else
                    {
                        shared_variant_indexes.emplace_back();
                        shared_variant_offsets.emplace_back();
                    }
                }
            }

            /// Cast all extracted variants into result type.
            std::vector<ColumnPtr> casted_shared_variant_columns;
            casted_shared_variant_columns.reserve(variant_types_from_shared_variant.size());
            for (size_t i = 0; i != variant_types_from_shared_variant.size(); ++i)
            {
                ColumnsWithTypeAndName variant = {{variant_columns_from_shared_variant[i]->getPtr(), variant_types_from_shared_variant[i], ""}};
                WrapperType variant_wrapper;
                if (cast_type == CastType::accurateOrNull)
                    /// Create wrapper only if we support conversion from variant to the resulting type.
                    variant_wrapper = createWrapperIfCanConvert(variant_types_from_shared_variant[i], result_type);
                else
                    variant_wrapper = prepareUnpackDictionaries(variant_types_from_shared_variant[i], result_type);

                ColumnPtr casted_variant;
                /// Check if we have wrapper for this variant.
                if (variant_wrapper)
                    casted_variant = variant_wrapper(variant, result_type, nullptr, variant_columns_from_shared_variant[i]->size());
                casted_shared_variant_columns.push_back(casted_variant);
            }

            /// Construct result column from all casted variants.
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
                    if (casted_shared_variant_columns[shared_variant_indexes[i]])
                        res->insertFrom(*casted_shared_variant_columns[shared_variant_indexes[i]], shared_variant_offsets[i]);
                    else
                        res->insertDefault();
                }
                else
                {
                    if (casted_variant_columns[global_discr])
                        res->insertFrom(*casted_variant_columns[global_discr], offsets[i]);
                    else
                        res->insertDefault();
                }
            }

            return res;
        };
    }

    WrapperType createStringToDynamicThroughParsingWrapper() const
    {
        return [&](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            auto column = arguments[0].column->convertToFullColumnIfLowCardinality();
            auto args = arguments;
            args[0].column = column;

            const ColumnNullable * column_nullable = nullptr;
            if (isColumnNullable(*args[0].column))
            {
                column_nullable = assert_cast<const ColumnNullable *>(args[0].column.get());
                args[0].column = column_nullable->getNestedColumnPtr();
            }

            args[0].type = removeNullable(removeLowCardinality(args[0].type));

            if (cast_type == CastType::accurateOrNull)
                return ConvertImplGenericFromString<false>::execute(args, result_type, column_nullable, input_rows_count, context);
            return ConvertImplGenericFromString<true>::execute(args, result_type, column_nullable, input_rows_count, context);
        };
    }

    WrapperType createVariantToDynamicWrapper(const DataTypeVariant & from_variant_type, const DataTypeDynamic & dynamic_type) const
    {
        /// First create extended Variant with shared variant type and cast this Variant to it.
        auto variants_for_dynamic = from_variant_type.getVariants();
        size_t number_of_variants = variants_for_dynamic.size();
        variants_for_dynamic.push_back(ColumnDynamic::getSharedVariantDataType());
        const auto & variant_type_for_dynamic = std::make_shared<DataTypeVariant>(variants_for_dynamic);
        auto old_to_new_variant_wrapper = createVariantToVariantWrapper(from_variant_type, *variant_type_for_dynamic);
        auto max_dynamic_types = dynamic_type.getMaxDynamicTypes();
        return [old_to_new_variant_wrapper, variant_type_for_dynamic, number_of_variants, max_dynamic_types]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * col_nullable, size_t input_rows_count) -> ColumnPtr
        {
            auto variant_column_for_dynamic = old_to_new_variant_wrapper(arguments, result_type, col_nullable, input_rows_count);
            /// If resulting Dynamic column can contain all variants from this Variant column, just create Dynamic column from it.
            if (max_dynamic_types >= number_of_variants)
                return ColumnDynamic::create(variant_column_for_dynamic, variant_type_for_dynamic, max_dynamic_types, max_dynamic_types);

            /// Otherwise some variants should go to the shared variant. Create temporary Dynamic column from this Variant and insert
            /// all data to the resulting Dynamic column, this insertion will do all the logic with shared variant.
            auto tmp_dynamic_column = ColumnDynamic::create(variant_column_for_dynamic, variant_type_for_dynamic, number_of_variants, number_of_variants);
            auto result_dynamic_column = ColumnDynamic::create(max_dynamic_types);
            result_dynamic_column->insertRangeFrom(*tmp_dynamic_column, 0, tmp_dynamic_column->size());
            return result_dynamic_column;
        };
    }

    WrapperType createColumnToDynamicWrapper(const DataTypePtr & from_type, const DataTypeDynamic & dynamic_type) const
    {
        if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(from_type.get()))
            return createVariantToDynamicWrapper(*variant_type, dynamic_type);

        if (context && context->getSettingsRef()[Setting::cast_string_to_dynamic_use_inference] && isStringOrFixedString(removeNullable(removeLowCardinality(from_type))))
            return createStringToDynamicThroughParsingWrapper();

        /// First, cast column to Variant with 2 variants - the type of the column we cast and shared variant type.
        auto variant_type = std::make_shared<DataTypeVariant>(DataTypes{removeNullableOrLowCardinalityNullable(from_type)});
        auto column_to_variant_wrapper = createColumnToVariantWrapper(from_type, *variant_type);
        /// Second, cast this Variant to Dynamic.
        auto variant_to_dynamic_wrapper = createVariantToDynamicWrapper(*variant_type, dynamic_type);
        return [column_to_variant_wrapper, variant_to_dynamic_wrapper, variant_type]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * col_nullable, size_t input_rows_count) -> ColumnPtr
        {
            auto variant_res = column_to_variant_wrapper(arguments, variant_type, col_nullable, input_rows_count);
            ColumnsWithTypeAndName args = {{variant_res, variant_type, ""}};
            return variant_to_dynamic_wrapper(args, result_type, nullptr, input_rows_count);
        };
    }

    WrapperType createDynamicToDynamicWrapper(const DataTypeDynamic & from_dynamic, const DataTypeDynamic & to_dynamic) const
    {
        size_t from_max_types = from_dynamic.getMaxDynamicTypes();
        size_t to_max_types = to_dynamic.getMaxDynamicTypes();
        if (from_max_types == to_max_types)
            return createIdentityWrapper(from_dynamic.getPtr());

        if (to_max_types > from_max_types)
        {
            return [to_max_types]
                   (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t) -> ColumnPtr
            {
                const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*arguments[0].column);
                /// We should use the same limit as already used in column and change only global limit.
                /// It's needed because shared variant should contain values only when limit is exceeded,
                /// so if there are already some data, we cannot increase the limit.
                return ColumnDynamic::create(dynamic_column.getVariantColumnPtr(), dynamic_column.getVariantInfo(), dynamic_column.getMaxDynamicTypes(), to_max_types);
            };
        }

        return [to_max_types]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t) -> ColumnPtr
        {
            const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*arguments[0].column);
            /// If real limit in the column is not greater than desired, just use the same variant column.
            if (dynamic_column.getMaxDynamicTypes() <= to_max_types)
                return ColumnDynamic::create(dynamic_column.getVariantColumnPtr(), dynamic_column.getVariantInfo(), dynamic_column.getMaxDynamicTypes(), to_max_types);

            /// Otherwise some variants should go to the shared variant. We try to keep the most frequent variants.
            const auto & variant_info = dynamic_column.getVariantInfo();
            const auto & variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
            const auto & statistics = dynamic_column.getStatistics();
            const auto & variant_column = dynamic_column.getVariantColumn();
            auto shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();
            std::vector<std::tuple<size_t, String, DataTypePtr>> variants_with_sizes;
            variants_with_sizes.reserve(variant_info.variant_names.size());
            for (const auto & [name, discr] : variant_info.variant_name_to_discriminator)
            {
                /// Don't include shared variant.
                if (discr == shared_variant_discr)
                    continue;

                size_t size = variant_column.getVariantByGlobalDiscriminator(discr).size();
                /// If column has statistics from the data part, use size from it for consistency.
                /// It's important to keep the same dynamic structure of the result column during ALTER.
                if (statistics)
                {
                    auto statistics_it = statistics->variants_statistics.find(name);
                    if (statistics_it != statistics->variants_statistics.end())
                        size = statistics_it->second;
                }
                variants_with_sizes.emplace_back(size, name, variants[discr]);
            }

            std::sort(variants_with_sizes.begin(), variants_with_sizes.end(), std::greater());
            DataTypes result_variants;
            result_variants.reserve(to_max_types + 1); /// +1 for shared variant.
            /// Add new variants from sorted list until we reach to_max_types.
            for (const auto & [size, name, type] : variants_with_sizes)
            {
                if (result_variants.size() < to_max_types)
                    result_variants.push_back(type);
                else
                    break;
            }

            /// Add shared variant.
            result_variants.push_back(ColumnDynamic::getSharedVariantDataType());
            /// Create resulting Variant type and Dynamic column.
            auto result_variant_type = std::make_shared<DataTypeVariant>(result_variants);
            auto result_dynamic_column = ColumnDynamic::create(result_variant_type->createColumn(), result_variant_type, to_max_types, to_max_types);
            const auto & result_variant_info = result_dynamic_column->getVariantInfo();
            auto & result_variant_column = result_dynamic_column->getVariantColumn();
            auto result_shared_variant_discr = result_dynamic_column->getSharedVariantDiscriminator();
            /// Create mapping from old discriminators to the new ones.
            std::vector<ColumnVariant::Discriminator> old_to_new_discriminators;
            old_to_new_discriminators.resize(variant_info.variant_name_to_discriminator.size(), result_shared_variant_discr);
            for (const auto & [name, discr] : result_variant_info.variant_name_to_discriminator)
            {
                auto old_discr = variant_info.variant_name_to_discriminator.at(name);
                old_to_new_discriminators[old_discr] = discr;
                /// Reuse old variant column if it's not shared variant.
                if (discr != result_shared_variant_discr)
                    result_variant_column.getVariantPtrByGlobalDiscriminator(discr) = variant_column.getVariantPtrByGlobalDiscriminator(old_discr);
            }

            const auto & local_discriminators = variant_column.getLocalDiscriminators();
            const auto & offsets = variant_column.getOffsets();
            const auto & shared_variant = dynamic_column.getSharedVariant();
            auto & result_local_discriminators = result_variant_column.getLocalDiscriminators();
            result_local_discriminators.reserve(local_discriminators.size());
            auto & result_offsets = result_variant_column.getOffsets();
            result_offsets.reserve(offsets.size());
            auto & result_shared_variant = result_dynamic_column->getSharedVariant();
            for (size_t i = 0; i != local_discriminators.size(); ++i)
            {
                auto global_discr = variant_column.globalDiscriminatorByLocal(local_discriminators[i]);
                if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
                {
                    result_local_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                    result_offsets.emplace_back();
                }
                else if (global_discr == shared_variant_discr)
                {
                    result_local_discriminators.push_back(result_variant_column.localDiscriminatorByGlobal(result_shared_variant_discr));
                    result_offsets.push_back(result_shared_variant.size());
                    result_shared_variant.insertFrom(shared_variant, offsets[i]);
                }
                else
                {
                    auto result_global_discr = old_to_new_discriminators[global_discr];
                    if (result_global_discr == result_shared_variant_discr)
                    {
                        result_local_discriminators.push_back(result_variant_column.localDiscriminatorByGlobal(result_shared_variant_discr));
                        result_offsets.push_back(result_shared_variant.size());
                        ColumnDynamic::serializeValueIntoSharedVariant(
                            result_shared_variant,
                            variant_column.getVariantByGlobalDiscriminator(global_discr),
                            variants[global_discr],
                            variants[global_discr]->getDefaultSerialization(),
                            offsets[i]);
                    }
                    else
                    {
                        result_local_discriminators.push_back(result_variant_column.localDiscriminatorByGlobal(result_global_discr));
                        result_offsets.push_back(offsets[i]);
                    }
                }
            }

            return result_dynamic_column;
        };
    }

    /// Wrapper for conversion to/from Dynamic type
    WrapperType createDynamicWrapper(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        if (const auto * from_dynamic = checkAndGetDataType<DataTypeDynamic>(from_type.get()))
        {
            if (const auto * to_dynamic = checkAndGetDataType<DataTypeDynamic>(to_type.get()))
                return createDynamicToDynamicWrapper(*from_dynamic, *to_dynamic);

            return createDynamicToColumnWrapper(to_type);
        }

        return createColumnToDynamicWrapper(from_type, *checkAndGetDataType<DataTypeDynamic>(to_type.get()));
    }

    template <typename FieldType>
    WrapperType createEnumWrapper(const DataTypePtr & from_type, const DataTypeEnum<FieldType> * to_type) const
    {
        using EnumType = DataTypeEnum<FieldType>;
        using Function = typename FunctionTo<EnumType>::Type;

        if (const auto * from_enum8 = checkAndGetDataType<DataTypeEnum8>(from_type.get()))
            checkEnumToEnumConversion(from_enum8, to_type);
        else if (const auto * from_enum16 = checkAndGetDataType<DataTypeEnum16>(from_type.get()))
            checkEnumToEnumConversion(from_enum16, to_type);

        if (checkAndGetDataType<DataTypeString>(from_type.get()))
            return createStringToEnumWrapper<ColumnString, EnumType>();
        else if (checkAndGetDataType<DataTypeFixedString>(from_type.get()))
            return createStringToEnumWrapper<ColumnFixedString, EnumType>();
        else if (isNativeNumber(from_type) || isEnum(from_type))
        {
            auto function = Function::create(context);
            return createFunctionAdaptor(function, from_type);
        }
        else
        {
            if (cast_type == CastType::accurateOrNull)
                return createToNullableColumnWrapper();
            else
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
                    from_type->getName(), to_type->getName());
        }
    }

    template <typename EnumTypeFrom, typename EnumTypeTo>
    void checkEnumToEnumConversion(const EnumTypeFrom * from_type, const EnumTypeTo * to_type) const
    {
        const auto & from_values = from_type->getValues();
        const auto & to_values = to_type->getValues();

        using ValueType = std::common_type_t<typename EnumTypeFrom::FieldType, typename EnumTypeTo::FieldType>;
        using NameValuePair = std::pair<std::string, ValueType>;
        using EnumValues = std::vector<NameValuePair>;

        EnumValues name_intersection;
        std::set_intersection(std::begin(from_values), std::end(from_values),
            std::begin(to_values), std::end(to_values), std::back_inserter(name_intersection),
            [] (auto && from, auto && to) { return from.first < to.first; });

        for (const auto & name_value : name_intersection)
        {
            const auto & old_value = name_value.second;
            const auto & new_value = to_type->getValue(name_value.first);
            if (old_value != new_value)
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Enum conversion changes value for element '{}' from {} to {}",
                    name_value.first, toString(old_value), toString(new_value));
        }
    }

    template <typename ColumnStringType, typename EnumType>
    WrapperType createStringToEnumWrapper() const
    {
        const char * function_name = cast_name;
        return [function_name] (
            ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
        {
            const auto & first_col = arguments.front().column.get();
            const auto & result_type = typeid_cast<const EnumType &>(*res_type);

            const ColumnStringType * col = typeid_cast<const ColumnStringType *>(first_col);

            if (col && nullable_col && nullable_col->size() != col->size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnNullable is not compatible with original");

            if (col)
            {
                const auto size = col->size();

                auto res = result_type.createColumn();
                auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
                out_data.resize(size);

                auto default_enum_value = result_type.getValues().front().second;

                if (nullable_col)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (!nullable_col->isNullAt(i))
                            out_data[i] = result_type.getValue(col->getDataAt(i));
                        else
                            out_data[i] = default_enum_value;
                    }
                }
                else
                {
                    for (size_t i = 0; i < size; ++i)
                        out_data[i] = result_type.getValue(col->getDataAt(i));
                }

                return res;
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} as first argument of function {}",
                    first_col->getName(), function_name);
        };
    }

    template <typename EnumType>
    WrapperType createEnumToStringWrapper() const
    {
        const char * function_name = cast_name;
        return [function_name] (
            ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
        {
            using ColumnEnumType = typename EnumType::ColumnType;

            const auto & first_col = arguments.front().column.get();
            const auto & first_type = arguments.front().type.get();

            const ColumnEnumType * enum_col = typeid_cast<const ColumnEnumType *>(first_col);
            const EnumType * enum_type = typeid_cast<const EnumType *>(first_type);

            if (enum_col && nullable_col && nullable_col->size() != enum_col->size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnNullable is not compatible with original");

            if (enum_col && enum_type)
            {
                const auto size = enum_col->size();
                const auto & enum_data = enum_col->getData();

                auto res = res_type->createColumn();

                if (nullable_col)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (!nullable_col->isNullAt(i))
                        {
                            const auto & value = enum_type->getNameForValue(enum_data[i]);
                            res->insertData(value.data, value.size);
                        }
                        else
                            res->insertDefault();
                    }
                }
                else
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        const auto & value = enum_type->getNameForValue(enum_data[i]);
                        res->insertData(value.data, value.size);
                    }
                }

                return res;
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} as first argument of function {}",
                    first_col->getName(), function_name);
        };
    }

    static WrapperType createIdentityWrapper(const DataTypePtr &)
    {
        return [] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/)
        {
            return arguments.front().column;
        };
    }

    static WrapperType createNothingWrapper(const IDataType * to_type)
    {
        ColumnPtr res = to_type->createColumnConstWithDefaultValue(1);
        return [res] (ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t input_rows_count)
        {
            /// Column of Nothing type is trivially convertible to any other column
            return res->cloneResized(input_rows_count)->convertToFullColumnIfConst();
        };
    }

    WrapperType prepareUnpackDictionaries(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        /// Conversion from/to Variant/Dynamic data type is processed in a special way.
        /// We don't need to remove LowCardinality/Nullable.
        if (isDynamic(to_type) || isDynamic(from_type))
            return createDynamicWrapper(from_type, to_type);

        if (isVariant(to_type) || isVariant(from_type))
            return createVariantWrapper(from_type, to_type);

        const auto * from_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(from_type.get());
        const auto * to_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(to_type.get());
        const auto & from_nested = from_low_cardinality ? from_low_cardinality->getDictionaryType() : from_type;
        const auto & to_nested = to_low_cardinality ? to_low_cardinality->getDictionaryType() : to_type;

        if (from_type->onlyNull())
        {
            if (!to_nested->isNullable() && !isVariant(to_type))
            {
                if (cast_type == CastType::accurateOrNull)
                {
                    return createToNullableColumnWrapper();
                }
                else
                {
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert NULL to a non-nullable type");
                }
            }

            return [](ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
            {
                return result_type->createColumnConstWithDefaultValue(input_rows_count)->convertToFullColumnIfConst();
            };
        }

        bool skip_not_null_check = false;

        if (from_low_cardinality && from_nested->isNullable() && !to_nested->isNullable())
            /// Disable check for dictionary. Will check that column doesn't contain NULL in wrapper below.
            skip_not_null_check = true;

        auto wrapper = prepareRemoveNullable(from_nested, to_nested, skip_not_null_check);
        if (!from_low_cardinality && !to_low_cardinality)
            return wrapper;

        return [wrapper, from_low_cardinality, to_low_cardinality, skip_not_null_check]
                (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
        {
            ColumnsWithTypeAndName args = {arguments[0]};
            auto & arg = args.front();
            auto res_type = result_type;

            ColumnPtr converted_column;

            ColumnPtr res_indexes;
            /// For some types default can't be casted (for example, String to Int). In that case convert column to full.
            bool src_converted_to_full_column = false;

            {
                auto tmp_rows_count = input_rows_count;

                if (to_low_cardinality)
                    res_type = to_low_cardinality->getDictionaryType();

                if (from_low_cardinality)
                {
                    const auto & col_low_cardinality = typeid_cast<const ColumnLowCardinality &>(*arguments[0].column);

                    if (skip_not_null_check && col_low_cardinality.containsNull())
                        throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type");

                    arg.column = col_low_cardinality.getDictionary().getNestedColumn();
                    arg.type = from_low_cardinality->getDictionaryType();

                    /// TODO: Make map with defaults conversion.
                    src_converted_to_full_column = !removeNullable(arg.type)->equals(*removeNullable(res_type));
                    if (src_converted_to_full_column)
                        arg.column = arg.column->index(col_low_cardinality.getIndexes(), 0);
                    else
                        res_indexes = col_low_cardinality.getIndexesPtr();

                    tmp_rows_count = arg.column->size();
                }

                /// Perform the requested conversion.
                converted_column = wrapper(args, res_type, nullable_source, tmp_rows_count);
            }

            if (to_low_cardinality)
            {
                auto res_column = to_low_cardinality->createColumn();
                auto & col_low_cardinality = typeid_cast<ColumnLowCardinality &>(*res_column);

                if (from_low_cardinality && !src_converted_to_full_column)
                    col_low_cardinality.insertRangeFromDictionaryEncodedColumn(*converted_column, *res_indexes);
                else
                    col_low_cardinality.insertRangeFromFullColumn(*converted_column, 0, converted_column->size());

                return res_column;
            }
            else if (!src_converted_to_full_column)
                return converted_column->index(*res_indexes, 0);
            else
                return converted_column;
        };
    }

    WrapperType prepareRemoveNullable(const DataTypePtr & from_type, const DataTypePtr & to_type, bool skip_not_null_check) const
    {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.

        bool source_is_nullable = from_type->isNullable();
        bool result_is_nullable = to_type->isNullable();

        auto wrapper = prepareImpl(removeNullable(from_type), removeNullable(to_type), result_is_nullable);

        if (result_is_nullable)
        {
            return [wrapper, source_is_nullable]
                (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
            {
                /// Create a temporary columns on which to perform the operation.
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*result_type);
                const auto & nested_type = nullable_type.getNestedType();

                ColumnsWithTypeAndName tmp_args;
                if (source_is_nullable)
                    tmp_args = createBlockWithNestedColumns(arguments);
                else
                    tmp_args = arguments;

                const ColumnNullable * nullable_source = nullptr;

                /// Add original ColumnNullable for createStringToEnumWrapper()
                if (source_is_nullable)
                {
                    if (arguments.size() != 1)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of arguments");
                    nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
                }

                /// Perform the requested conversion.
                auto tmp_res = wrapper(tmp_args, nested_type, nullable_source, input_rows_count);

                /// May happen in fuzzy tests. For debug purpose.
                if (!tmp_res)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Couldn't convert {} to {} in prepareRemoveNullable wrapper.",
                                    arguments[0].type->getName(), nested_type->getName());

                return wrapInNullable(tmp_res, arguments, nested_type, input_rows_count);
            };
        }
        else if (source_is_nullable)
        {
            /// Conversion from Nullable to non-Nullable.

            return [wrapper, skip_not_null_check]
                (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
            {
                auto tmp_args = createBlockWithNestedColumns(arguments);
                auto nested_type = removeNullable(result_type);

                /// Check that all values are not-NULL.
                /// Check can be skipped in case if LowCardinality dictionary is transformed.
                /// In that case, correctness will be checked beforehand.
                if (!skip_not_null_check)
                {
                    const auto & col = arguments[0].column;
                    const auto & nullable_col = assert_cast<const ColumnNullable &>(*col);
                    const auto & null_map = nullable_col.getNullMapData();

                    if (!memoryIsZero(null_map.data(), 0, null_map.size()))
                        throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type");
                }
                const ColumnNullable * nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
                return wrapper(tmp_args, nested_type, nullable_source, input_rows_count);
            };
        }
        else
            return wrapper;
    }

    /// 'from_type' and 'to_type' are nested types in case of Nullable.
    /// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
    WrapperType prepareImpl(const DataTypePtr & from_type, const DataTypePtr & to_type, bool requested_result_is_nullable) const
    {
        if (isUInt8(from_type) && isBool(to_type))
            return createUInt8ToBoolWrapper(from_type, to_type);

        /// We can cast IPv6 into IPv6, IPv4 into IPv4, but we should not allow to cast FixedString(16) into IPv6 as part of identity cast
        bool safe_convert_custom_types = true;

        if (const auto * to_type_custom_name = to_type->getCustomName())
            safe_convert_custom_types = from_type->getCustomName() && from_type->getCustomName()->getName() == to_type_custom_name->getName();
        else if (const auto * from_type_custom_name = from_type->getCustomName())
            safe_convert_custom_types = to_type->getCustomName() && from_type_custom_name->getName() == to_type->getCustomName()->getName();

        if (from_type->equals(*to_type) && safe_convert_custom_types)
        {
            /// We can only use identity conversion for DataTypeAggregateFunction when they are strictly equivalent.
            if (typeid_cast<const DataTypeAggregateFunction *>(from_type.get()))
            {
                if (DataTypeAggregateFunction::strictEquals(from_type, to_type))
                    return createIdentityWrapper(from_type);
            }
            else
                return createIdentityWrapper(from_type);
        }
        else if (WhichDataType(from_type).isNothing())
            return createNothingWrapper(to_type.get());

        WrapperType ret;

        auto make_default_wrapper = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (is_any_of<ToDataType,
                DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
                DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
                DataTypeFloat32, DataTypeFloat64,
                DataTypeDate, DataTypeDate32, DataTypeDateTime,
                DataTypeUUID, DataTypeIPv4, DataTypeIPv6>)
            {
                ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                return true;
            }
            if constexpr (std::is_same_v<ToDataType, DataTypeUInt8>)
            {
                if (isBool(to_type))
                    ret = createBoolWrapper<ToDataType>(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                else
                    ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                return true;
            }
            if constexpr (
                std::is_same_v<ToDataType, DataTypeEnum8> ||
                std::is_same_v<ToDataType, DataTypeEnum16>)
            {
                ret = createEnumWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()));
                return true;
            }
            if constexpr (is_any_of<ToDataType,
                DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
                DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal256>,
                DataTypeDateTime64>)
            {
                ret = createDecimalWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                return true;
            }

            return false;
        };

        bool cast_ipv4_ipv6_default_on_conversion_error_value = context && context->getSettingsRef()[Setting::cast_ipv4_ipv6_default_on_conversion_error];
        bool input_format_ipv4_default_on_conversion_error_value = context && context->getSettingsRef()[Setting::input_format_ipv4_default_on_conversion_error];
        bool input_format_ipv6_default_on_conversion_error_value = context && context->getSettingsRef()[Setting::input_format_ipv6_default_on_conversion_error];

        auto make_custom_serialization_wrapper = [&, cast_ipv4_ipv6_default_on_conversion_error_value, input_format_ipv4_default_on_conversion_error_value, input_format_ipv6_default_on_conversion_error_value](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::RightType;
            using FromDataType = typename Types::LeftType;

            if constexpr (WhichDataType(FromDataType::type_id).isStringOrFixedString())
            {
                if constexpr (std::is_same_v<ToDataType, DataTypeIPv4>)
                {
                    ret = [cast_ipv4_ipv6_default_on_conversion_error_value,
                           input_format_ipv4_default_on_conversion_error_value,
                           requested_result_is_nullable](
                              ColumnsWithTypeAndName & arguments,
                              const DataTypePtr & result_type,
                              const ColumnNullable * column_nullable,
                              size_t) -> ColumnPtr
                    {
                        if (!WhichDataType(result_type).isIPv4())
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected IPv4", result_type->getName());

                        const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                        if (requested_result_is_nullable)
                            return convertToIPv4<IPStringToNumExceptionMode::Null>(arguments[0].column, null_map);
                        else if (cast_ipv4_ipv6_default_on_conversion_error_value || input_format_ipv4_default_on_conversion_error_value)
                            return convertToIPv4<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                        else
                            return convertToIPv4<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                    };

                    return true;
                }

                if constexpr (std::is_same_v<ToDataType, DataTypeIPv6>)
                {
                    ret = [cast_ipv4_ipv6_default_on_conversion_error_value,
                           input_format_ipv6_default_on_conversion_error_value,
                           requested_result_is_nullable](
                              ColumnsWithTypeAndName & arguments,
                              const DataTypePtr & result_type,
                              const ColumnNullable * column_nullable,
                              size_t) -> ColumnPtr
                    {
                        if (!WhichDataType(result_type).isIPv6())
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected IPv6", result_type->getName());

                        const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                        if (requested_result_is_nullable)
                            return convertToIPv6<IPStringToNumExceptionMode::Null>(arguments[0].column, null_map);
                        else if (cast_ipv4_ipv6_default_on_conversion_error_value || input_format_ipv6_default_on_conversion_error_value)
                            return convertToIPv6<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                        else
                            return convertToIPv6<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                    };

                    return true;
                }

                if (to_type->getCustomSerialization() && to_type->getCustomName())
                {
                    ret = [requested_result_is_nullable, this](
                              ColumnsWithTypeAndName & arguments,
                              const DataTypePtr & result_type,
                              const ColumnNullable * column_nullable,
                              size_t input_rows_count) -> ColumnPtr
                    {
                        auto wrapped_result_type = result_type;
                        if (requested_result_is_nullable)
                            wrapped_result_type = makeNullable(result_type);
                        if (this->cast_type == CastType::accurateOrNull)
                            return ConvertImplGenericFromString<false>::execute(
                                arguments, wrapped_result_type, column_nullable, input_rows_count, context);
                        return ConvertImplGenericFromString<true>::execute(
                            arguments, wrapped_result_type, column_nullable, input_rows_count, context);
                    };
                    return true;
                }
            }
            else if constexpr (WhichDataType(FromDataType::type_id).isIPv6() && WhichDataType(ToDataType::type_id).isIPv4())
            {
                ret = [cast_ipv4_ipv6_default_on_conversion_error_value, requested_result_is_nullable](
                                ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t)
                        -> ColumnPtr
                {
                    if (!WhichDataType(result_type).isIPv4())
                        throw Exception(
                            ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected IPv4", result_type->getName());

                    const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                    if (requested_result_is_nullable)
                        return convertIPv6ToIPv4<IPStringToNumExceptionMode::Null>(arguments[0].column, null_map);
                    else if (cast_ipv4_ipv6_default_on_conversion_error_value)
                        return convertIPv6ToIPv4<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                    else
                        return convertIPv6ToIPv4<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                };

                return true;
            }

            if constexpr (WhichDataType(ToDataType::type_id).isStringOrFixedString())
            {
                if constexpr (WhichDataType(FromDataType::type_id).isEnum())
                {
                    ret = createEnumToStringWrapper<FromDataType>();
                    return true;
                }
                else if (from_type->getCustomSerialization())
                {
                    ret = [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
                    {
                        return ConvertImplGenericToString<typename ToDataType::ColumnType>::execute(arguments, result_type, input_rows_count, context);
                    };
                    return true;
                }
            }

            return false;
        };

        if (callOnTwoTypeIndexes(from_type->getTypeId(), to_type->getTypeId(), make_custom_serialization_wrapper))
            return ret;

        if (callOnIndexAndDataType<void>(to_type->getTypeId(), make_default_wrapper))
            return ret;

        switch (to_type->getTypeId())
        {
            case TypeIndex::String:
                return createStringWrapper(from_type);
            case TypeIndex::FixedString:
                return createFixedStringWrapper(from_type, checkAndGetDataType<DataTypeFixedString>(to_type.get())->getN());
            case TypeIndex::Array:
                return createArrayWrapper(from_type, static_cast<const DataTypeArray &>(*to_type));
            case TypeIndex::Tuple:
                return createTupleWrapper(from_type, checkAndGetDataType<DataTypeTuple>(to_type.get()));
            case TypeIndex::Map:
                return createMapWrapper(from_type, checkAndGetDataType<DataTypeMap>(to_type.get()));
            case TypeIndex::ObjectDeprecated:
                return createObjectDeprecatedWrapper(from_type, checkAndGetDataType<DataTypeObjectDeprecated>(to_type.get()));
            case TypeIndex::Object:
                return createObjectWrapper(from_type, checkAndGetDataType<DataTypeObject>(to_type.get()));
            case TypeIndex::AggregateFunction:
                return createAggregateFunctionWrapper(from_type, checkAndGetDataType<DataTypeAggregateFunction>(to_type.get()));
            case TypeIndex::Interval:
                return createIntervalWrapper(from_type, checkAndGetDataType<DataTypeInterval>(to_type.get())->getKind());
            default:
                break;
        }

        if (cast_type == CastType::accurateOrNull)
            return createToNullableColumnWrapper();
        else
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
                from_type->getName(), to_type->getName());
    }
};

}


FunctionBasePtr createFunctionBaseCast(
    ContextPtr context,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type)
{
    DataTypes data_types(arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    FunctionCast::MonotonicityForRange monotonicity;

    if (isEnum(arguments.front().type)
        && castTypeToEither<DataTypeEnum8, DataTypeEnum16>(return_type.get(), [&](auto & type)
        {
            monotonicity = FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }
    else if (castTypeToEither<
        DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
        DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
        DataTypeFloat32, DataTypeFloat64,
        DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeDateTime64,
        DataTypeString>(return_type.get(), [&](auto & type)
        {
            monotonicity = FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }

    return std::make_unique<FunctionCast>(context, name, std::move(monotonicity), data_types, return_type, diagnostic, cast_type);
}

REGISTER_FUNCTION(Conversion)
{
    factory.registerFunction<FunctionToUInt8>();
    factory.registerFunction<FunctionToUInt16>();
    factory.registerFunction<FunctionToUInt32>();
    factory.registerFunction<FunctionToUInt64>();
    factory.registerFunction<FunctionToUInt128>();
    factory.registerFunction<FunctionToUInt256>();
    factory.registerFunction<FunctionToInt8>();
    factory.registerFunction<FunctionToInt16>();
    factory.registerFunction<FunctionToInt32>();
    factory.registerFunction<FunctionToInt64>();
    factory.registerFunction<FunctionToInt128>();
    factory.registerFunction<FunctionToInt256>();
    factory.registerFunction<FunctionToFloat32>();
    factory.registerFunction<FunctionToFloat64>();

    factory.registerFunction<FunctionToDecimal32>();
    factory.registerFunction<FunctionToDecimal64>();
    factory.registerFunction<FunctionToDecimal128>();
    factory.registerFunction<FunctionToDecimal256>();

    factory.registerFunction<FunctionToDate>();

    /// MySQL compatibility alias. Cannot be registered as alias,
    /// because we don't want it to be normalized to toDate in queries,
    /// otherwise CREATE DICTIONARY query breaks.
    factory.registerFunction("DATE", &FunctionToDate::create, {}, FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionToDate32>();
    factory.registerFunction<FunctionToDateTime>();
    factory.registerFunction<FunctionToDateTime32>();
    factory.registerFunction<FunctionToDateTime64>();
    factory.registerFunction<FunctionToUUID>();
    factory.registerFunction<FunctionToIPv4>();
    factory.registerFunction<FunctionToIPv6>();
    factory.registerFunction<FunctionToString>();

    factory.registerFunction<FunctionToUnixTimestamp>();

    factory.registerFunction<FunctionToUInt8OrZero>();
    factory.registerFunction<FunctionToUInt16OrZero>();
    factory.registerFunction<FunctionToUInt32OrZero>();
    factory.registerFunction<FunctionToUInt64OrZero>();
    factory.registerFunction<FunctionToUInt128OrZero>();
    factory.registerFunction<FunctionToUInt256OrZero>();
    factory.registerFunction<FunctionToInt8OrZero>();
    factory.registerFunction<FunctionToInt16OrZero>();
    factory.registerFunction<FunctionToInt32OrZero>();
    factory.registerFunction<FunctionToInt64OrZero>();
    factory.registerFunction<FunctionToInt128OrZero>();
    factory.registerFunction<FunctionToInt256OrZero>();
    factory.registerFunction<FunctionToFloat32OrZero>();
    factory.registerFunction<FunctionToFloat64OrZero>();
    factory.registerFunction<FunctionToDateOrZero>();
    factory.registerFunction<FunctionToDate32OrZero>();
    factory.registerFunction<FunctionToDateTimeOrZero>();
    factory.registerFunction<FunctionToDateTime64OrZero>();

    factory.registerFunction<FunctionToDecimal32OrZero>();
    factory.registerFunction<FunctionToDecimal64OrZero>();
    factory.registerFunction<FunctionToDecimal128OrZero>();
    factory.registerFunction<FunctionToDecimal256OrZero>();

    factory.registerFunction<FunctionToUUIDOrZero>();
    factory.registerFunction<FunctionToIPv4OrZero>();
    factory.registerFunction<FunctionToIPv6OrZero>();

    factory.registerFunction<FunctionToUInt8OrNull>();
    factory.registerFunction<FunctionToUInt16OrNull>();
    factory.registerFunction<FunctionToUInt32OrNull>();
    factory.registerFunction<FunctionToUInt64OrNull>();
    factory.registerFunction<FunctionToUInt128OrNull>();
    factory.registerFunction<FunctionToUInt256OrNull>();
    factory.registerFunction<FunctionToInt8OrNull>();
    factory.registerFunction<FunctionToInt16OrNull>();
    factory.registerFunction<FunctionToInt32OrNull>();
    factory.registerFunction<FunctionToInt64OrNull>();
    factory.registerFunction<FunctionToInt128OrNull>();
    factory.registerFunction<FunctionToInt256OrNull>();
    factory.registerFunction<FunctionToFloat32OrNull>();
    factory.registerFunction<FunctionToFloat64OrNull>();
    factory.registerFunction<FunctionToDateOrNull>();
    factory.registerFunction<FunctionToDate32OrNull>();
    factory.registerFunction<FunctionToDateTimeOrNull>();
    factory.registerFunction<FunctionToDateTime64OrNull>();

    factory.registerFunction<FunctionToDecimal32OrNull>();
    factory.registerFunction<FunctionToDecimal64OrNull>();
    factory.registerFunction<FunctionToDecimal128OrNull>();
    factory.registerFunction<FunctionToDecimal256OrNull>();

    factory.registerFunction<FunctionToUUIDOrNull>();
    factory.registerFunction<FunctionToIPv4OrNull>();
    factory.registerFunction<FunctionToIPv6OrNull>();

    factory.registerFunction<FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUS>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUSOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUSOrNull>();
    factory.registerFunction<FunctionParseDateTime32BestEffort>();
    factory.registerFunction<FunctionParseDateTime32BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime32BestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTime64BestEffort>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTime64BestEffortUS>();
    factory.registerFunction<FunctionParseDateTime64BestEffortUSOrZero>();
    factory.registerFunction<FunctionParseDateTime64BestEffortUSOrNull>();

    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalNanosecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMicrosecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMillisecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalSecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMinute, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalHour, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalDay, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalWeek, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMonth, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalQuarter, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalYear, PositiveMonotonicity>>();
}

}
