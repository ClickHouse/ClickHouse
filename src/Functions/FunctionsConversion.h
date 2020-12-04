#pragma once

#include <ext/enumerate.h>
#include <ext/collection_cast.h>
#include <ext/range.h>
#include <type_traits>

#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/Operators.h>
#include <IO/parseDateTimeBestEffort.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Formats/FormatSettings.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Common/FieldVisitors.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/DateTimeTransforms.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Functions/toFixedString.h>


namespace DB
{

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
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
}


/** Type conversion functions.
  * toType - conversion in "natural way";
  */

inline UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column)
{
    const auto * arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type)
        || checkAndGetDataType<DataTypeUInt32>(arg_type)
        || checkAndGetDataType<DataTypeUInt16>(arg_type)
        || checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception("Illegal type of toDecimal() scale " + named_column.type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    Field field;
    named_column.column->get(0, field);
    return field.get<UInt32>();
}

/// Function toUnixTimestamp has exactly the same implementation as toDateTime of String type.
struct NameToUnixTimestamp { static constexpr auto name = "toUnixTimestamp"; };


/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImpl
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    template <typename Additions = void *>
    static ColumnPtr NO_SANITIZE_UNDEFINED execute(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/,
        Additions additions [[maybe_unused]] = Additions())
    {
        const ColumnWithTypeAndName & named_from = arguments[0];

        using ColVecFrom = typename FromDataType::ColumnType;
        using ColVecTo = typename ToDataType::ColumnType;

        if (std::is_same_v<Name, NameToUnixTimestamp>)
        {
            if (isDate(named_from.type))
                throw Exception("Illegal type " + named_from.type->getName() + " of first argument of function " + Name::name,
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        if constexpr ((IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
            && !(std::is_same_v<DataTypeDateTime64, FromDataType> || std::is_same_v<DataTypeDateTime64, ToDataType>))
        {
            if constexpr (!IsDataTypeDecimalOrNumber<FromDataType> || !IsDataTypeDecimalOrNumber<ToDataType>)
            {
                throw Exception("Illegal column " + named_from.column->getName() + " of first argument of function " + Name::name,
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        if (const ColVecFrom * col_from = checkAndGetColumn<ColVecFrom>(named_from.column.get()))
        {
            typename ColVecTo::MutablePtr col_to = nullptr;
            if constexpr (IsDataTypeDecimal<ToDataType>)
            {
                UInt32 scale = additions;
                col_to = ColVecTo::create(0, scale);
            }
            else
                col_to = ColVecTo::create();

            const auto & vec_from = col_from->getData();
            auto & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
            {
                if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
                {
                    if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                        vec_to[i] = convertDecimals<FromDataType, ToDataType>(vec_from[i], vec_from.getScale(), vec_to.getScale());
                    else if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>)
                        vec_to[i] = convertFromDecimal<FromDataType, ToDataType>(vec_from[i], vec_from.getScale());
                    else if constexpr (IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>)
                        vec_to[i] = convertToDecimal<FromDataType, ToDataType>(vec_from[i], vec_to.getScale());
                    else
                        throw Exception("Unsupported data type in conversion function", ErrorCodes::CANNOT_CONVERT_TYPE);
                }
                else if constexpr (is_big_int_v<FromFieldType> || is_big_int_v<ToFieldType>)
                {
                    if constexpr (std::is_same_v<FromFieldType, UInt128> || std::is_same_v<ToFieldType, UInt128>)
                        throw Exception("Unexpected UInt128 to big int conversion", ErrorCodes::NOT_IMPLEMENTED);
                    /// If From Data is Nan or Inf, throw exception
                    else if (!isFinite(vec_from[i]))
                        throw Exception("Unexpected inf or nan to big int conversion", ErrorCodes::NOT_IMPLEMENTED);
                    else
                        vec_to[i] = bigint_cast<ToFieldType>(vec_from[i]);
                }
                else if constexpr (std::is_same_v<ToFieldType, UInt128> && sizeof(FromFieldType) <= sizeof(UInt64))
                    vec_to[i] = static_cast<ToFieldType>(static_cast<UInt64>(vec_from[i]));
                else
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }

            return col_to;
        }
        else
            throw Exception("Illegal column " + named_from.column->getName() + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

/** Conversion of DateTime to Date: throw off time component.
  */
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeDateTime, DataTypeDate, ToDateImpl> {};


/** Conversion of Date to DateTime: adding 00:00:00 time component.
  */
struct ToDateTimeImpl
{
    static constexpr auto name = "toDateTime";

    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d));
    }

    // no-op conversion from DateTime to DateTime, used in DateTime64 to DateTime conversion.
    static inline UInt32 execute(UInt32 d, const DateLUTImpl & /*time_zone*/)
    {
        return d;
    }
};

template <typename Name> struct ConvertImpl<DataTypeDate, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeDate, DataTypeDateTime, ToDateTimeImpl> {};

/// Implementation of toDate function.

template <typename FromType, typename ToType>
struct ToDateTransform32Or64
{
    static constexpr auto name = "toDate";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        return (from < 0xFFFF)
            ? from
            : time_zone.toDayNum(std::min(time_t(from), time_t(0xFFFFFFFF)));
    }
};

template <typename FromType, typename ToType>
struct ToDateTransform32Or64Signed
{
    static constexpr auto name = "toDate";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        /// The function should be monotonic (better for query optimizations), so we saturate instead of overflow.
        if (from < 0)
            return 0;
        return (from < 0xFFFF)
            ? from
            : time_zone.toDayNum(std::min(time_t(from), time_t(0xFFFFFFFF)));
    }
};

template <typename FromType, typename ToType>
struct ToDateTransform8Or16Signed
{
    static constexpr auto name = "toDate";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
            return 0;
        return from;
    }
};

/** Special case of converting Int8, Int16, (U)Int32 or (U)Int64 (and also, for convenience,
  * Float32, Float64) to Date. If the number is negative, saturate it to unix epoch time. If the
  * number is less than 65536, then it is treated as DayNum, and if it's greater or equals to 65536,
  * then treated as unix timestamp. If the number exceeds UInt32, saturate to MAX_UINT32 then as DayNum.
  * It's a bit illogical, as we actually have two functions in one.
  * But allows to support frequent case,
  *  when user write toDate(UInt32), expecting conversion of unix timestamp to Date.
  *  (otherwise such usage would be frequent mistake).
  */
template <typename Name> struct ConvertImpl<DataTypeUInt32, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeUInt32, DataTypeDate, ToDateTransform32Or64<UInt32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeUInt64, DataTypeDate, ToDateTransform32Or64<UInt64, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt8, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeInt8, DataTypeDate, ToDateTransform8Or16Signed<Int8, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt16, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeInt16, DataTypeDate, ToDateTransform8Or16Signed<Int16, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeInt32, DataTypeDate, ToDateTransform32Or64Signed<Int32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeInt64, DataTypeDate, ToDateTransform32Or64Signed<Int64, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeFloat32, DataTypeDate, ToDateTransform32Or64Signed<Float32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeFloat64, DataTypeDate, ToDateTransform32Or64Signed<Float64, UInt16>> {};


template <typename FromType, typename ToType>
struct ToDateTimeTransform64
{
    static constexpr auto name = "toDateTime";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        return std::min(time_t(from), time_t(0xFFFFFFFF));
    }
};

template <typename FromType, typename ToType>
struct ToDateTimeTransformSigned
{
    static constexpr auto name = "toDateTime";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
            return 0;
        return from;
    }
};

template <typename FromType, typename ToType>
struct ToDateTimeTransform64Signed
{
    static constexpr auto name = "toDateTime";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
            return 0;
        return std::min(time_t(from), time_t(0xFFFFFFFF));
    }
};

/** Special case of converting Int8, Int16, Int32 or (U)Int64 (and also, for convenience, Float32,
  * Float64) to DateTime. If the number is negative, saturate it to unix epoch time. If the number
  * exceeds UInt32, saturate to MAX_UINT32.
  */
template <typename Name> struct ConvertImpl<DataTypeInt8, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt8, DataTypeDateTime, ToDateTimeTransformSigned<Int8, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt16, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt16, DataTypeDateTime, ToDateTimeTransformSigned<Int16, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt32, DataTypeDateTime, ToDateTimeTransformSigned<Int32, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt64, DataTypeDateTime, ToDateTimeTransform64Signed<Int64, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeUInt64, DataTypeDateTime, ToDateTimeTransform64<UInt64, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeFloat32, DataTypeDateTime, ToDateTimeTransform64Signed<Float32, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeFloat64, DataTypeDateTime, ToDateTimeTransform64Signed<Float64, UInt32>> {};


/** Conversion of Date or DateTime to DateTime64: add zero sub-second part.
  */
struct ToDateTime64Transform
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier = 1;

    ToDateTime64Transform(UInt32 scale = 0)
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    inline DateTime64::NativeType execute(UInt16 d, const DateLUTImpl & time_zone) const
    {
        const auto dt = ToDateTimeImpl::execute(d, time_zone);
        return execute(dt, time_zone);
    }

    inline DateTime64::NativeType execute(UInt32 dt, const DateLUTImpl & /*time_zone*/) const
    {
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(dt, 0, scale_multiplier);
    }
};

template <typename Name> struct ConvertImpl<DataTypeDate, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeDate, DataTypeDateTime64, ToDateTime64Transform> {};
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeDateTime, DataTypeDateTime64, ToDateTime64Transform> {};

/** Conversion of DateTime64 to Date or DateTime: discards fractional part.
 */
template <typename Transform>
struct FromDateTime64Transform
{
    static constexpr auto name = Transform::name;

    const DateTime64::NativeType scale_multiplier = 1;

    FromDateTime64Transform(UInt32 scale)
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    inline auto execute(DateTime64::NativeType dt, const DateLUTImpl & time_zone) const
    {
        const auto c = DecimalUtils::splitWithScaleMultiplier(DateTime64(dt), scale_multiplier);
        return Transform::execute(static_cast<UInt32>(c.whole), time_zone);
    }
};

template <typename Name> struct ConvertImpl<DataTypeDateTime64, DataTypeDate, Name>
    : DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate, FromDateTime64Transform<ToDateImpl>> {};
template <typename Name> struct ConvertImpl<DataTypeDateTime64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeDateTime64, DataTypeDateTime, FromDateTime64Transform<ToDateTimeImpl>> {};


/** Transformation of numbers, dates, datetimes to strings: through formatting.
  */
template <typename DataType>
struct FormatImpl
{
    static void execute(const typename DataType::FieldType x, WriteBuffer & wb, const DataType *, const DateLUTImpl *)
    {
        writeText(x, wb);
    }
};

template <>
struct FormatImpl<DataTypeDate>
{
    static void execute(const DataTypeDate::FieldType x, WriteBuffer & wb, const DataTypeDate *, const DateLUTImpl *)
    {
        writeDateText(DayNum(x), wb);
    }
};

template <>
struct FormatImpl<DataTypeDateTime>
{
    static void execute(const DataTypeDateTime::FieldType x, WriteBuffer & wb, const DataTypeDateTime *, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(x, wb, *time_zone);
    }
};

template <>
struct FormatImpl<DataTypeDateTime64>
{
    static void execute(const DataTypeDateTime64::FieldType x, WriteBuffer & wb, const DataTypeDateTime64 * type, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(DateTime64(x), type->getScale(), wb, *time_zone);
    }
};


template <typename FieldType>
struct FormatImpl<DataTypeEnum<FieldType>>
{
    static void execute(const FieldType x, WriteBuffer & wb, const DataTypeEnum<FieldType> * type, const DateLUTImpl *)
    {
        writeString(type->getNameForValue(x), wb);
    }
};

template <typename FieldType>
struct FormatImpl<DataTypeDecimal<FieldType>>
{
    static void execute(const FieldType x, WriteBuffer & wb, const DataTypeDecimal<FieldType> * type, const DateLUTImpl *)
    {
        writeText(x, type->getScale(), wb);
    }
};


/// DataTypeEnum<T> to DataType<T> free conversion
template <typename FieldType, typename Name>
struct ConvertImpl<DataTypeEnum<FieldType>, DataTypeNumber<FieldType>, Name>
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        return arguments[0].column;
    }
};


template <typename FromDataType, typename Name>
struct ConvertImpl<FromDataType, std::enable_if_t<!std::is_same_v<FromDataType, DataTypeString>, DataTypeString>, Name>
{
    using FromFieldType = typename FromDataType::FieldType;
    using ColVecType = std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>, ColumnVector<FromFieldType>>;

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        const auto & col_with_type_and_name = arguments[0];
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        const DateLUTImpl * time_zone = nullptr;

        /// For argument of DateTime type, second argument with time zone could be specified.
        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime> || std::is_same_v<FromDataType, DataTypeDateTime64>)
            time_zone = &extractTimeZoneFromFunctionArguments(arguments, 1, 0);

        if (const auto col_from = checkAndGetColumn<ColVecType>(col_with_type_and_name.column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColVecType::Container & vec_from = col_from->getData();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();

            if constexpr (std::is_same_v<FromDataType, DataTypeDate>)
                data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
            else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime>)
                data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss") + 1));
            else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
                data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss.") + vec_from.getScale() + 1));
            else
                data_to.resize(size * 3);   /// Arbitrary

            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                FormatImpl<FromDataType>::execute(vec_from[i], write_buffer, &type, time_zone);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            write_buffer.finalize();
            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Generic conversion of any type to String.
struct ConvertImplGenericToString
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments)
    {
        const auto & col_with_type_and_name = arguments[0];
        const IDataType & type = *col_with_type_and_name.type;
        const IColumn & col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        data_to.resize(size * 2); /// Using coefficient 2 for initial size is arbitrary.
        offsets_to.resize(size);

        WriteBufferFromVector<ColumnString::Chars> write_buffer(data_to);

        FormatSettings format_settings;
        for (size_t i = 0; i < size; ++i)
        {
            type.serializeAsText(col_from, i, write_buffer, format_settings);
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
        }

        write_buffer.finalize();
        return col_to;
    }
};


/** Conversion of strings to numbers, dates, datetimes: through parsing.
  */
template <typename DataType>
void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    readText(x, rb);
}

template <>
inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    DayNum tmp(0);
    readDateText(tmp, rb);
    x = tmp;
}

template <>
inline void parseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
{
    time_t tmp = 0;
    readDateTimeText(tmp, rb, *time_zone);
    x = tmp;
}

template <>
inline void parseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    UUID tmp;
    readUUIDText(tmp, rb);
    x = tmp;
}


template <typename DataType>
bool tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    if constexpr (std::is_floating_point_v<typename DataType::FieldType>)
        return tryReadFloatText(x, rb);
    else /*if constexpr (is_integer_v<typename DataType::FieldType>)*/
        return tryReadIntText(x, rb);
}

template <>
inline bool tryParseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    DayNum tmp(0);
    if (!tryReadDateText(tmp, rb))
        return false;
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
{
    time_t tmp = 0;
    if (!tryReadDateTimeText(tmp, rb, *time_zone))
        return false;
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    UUID tmp;
    if (!tryReadUUIDText(tmp, rb))
        return false;

    x = tmp;
    return true;
}


/** Throw exception with verbose message when string value is not parsed completely.
  */
[[noreturn]] inline void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, const DataTypePtr result_type)
{
    const IDataType & to_type = *result_type;

    WriteBufferFromOwnString message_buf;
    message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
                << " as " << to_type.getName()
                << ": syntax error";

    if (read_buffer.offset())
        message_buf << " at position " << read_buffer.offset()
                    << " (parsed just " << quote << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
    else
        message_buf << " at begin of string";

    if (isNativeNumber(to_type))
        message_buf << ". Note: there are to" << to_type.getName() << "OrZero and to" << to_type.getName() << "OrNull functions, which returns zero/NULL instead of throwing exception.";

    throw Exception(message_buf.str(), ErrorCodes::CANNOT_PARSE_TEXT);
}


enum class ConvertFromStringExceptionMode
{
    Throw,  /// Throw exception if value cannot be parsed.
    Zero,   /// Fill with zero or default if value cannot be parsed.
    Null    /// Return ColumnNullable with NULLs when value cannot be parsed.
};

enum class ConvertFromStringParsingMode
{
    Normal,
    BestEffort,  /// Only applicable for DateTime. Will use sophisticated method, that is slower.
    BestEffortUS
};

template <typename FromDataType, typename ToDataType, typename Name,
    ConvertFromStringExceptionMode exception_mode, ConvertFromStringParsingMode parsing_mode>
struct ConvertThroughParsing
{
    static_assert(std::is_same_v<FromDataType, DataTypeString> || std::is_same_v<FromDataType, DataTypeFixedString>,
        "ConvertThroughParsing is only applicable for String or FixedString data types");

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    // using ToFieldType = typename ToDataType::FieldType;

    static bool isAllRead(ReadBuffer & in)
    {
        /// In case of FixedString, skip zero bytes at end.
        if constexpr (std::is_same_v<FromDataType, DataTypeFixedString>)
            while (!in.eof() && *in.position() == 0)
                ++in.position();

        if (in.eof())
            return true;

        /// Special case, that allows to parse string with DateTime as Date.
        if (std::is_same_v<ToDataType, DataTypeDate> && (in.buffer().size()) == strlen("YYYY-MM-DD hh:mm:ss"))
            return true;

        return false;
    }

    template <typename Additions = void *>
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t input_rows_count,
                        Additions additions [[maybe_unused]] = Additions())
    {
        using ColVecTo = typename ToDataType::ColumnType;

        const DateLUTImpl * local_time_zone [[maybe_unused]] = nullptr;
        const DateLUTImpl * utc_time_zone [[maybe_unused]] = nullptr;

        /// For conversion to DateTime type, second argument with time zone could be specified.
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime> || to_datetime64)
        {
            const auto result_type = removeNullable(res_type);
            // Time zone is already figured out during result type resolution, no need to do it here.
            if (const auto dt_col = checkAndGetDataType<ToDataType>(result_type.get()))
                local_time_zone = &dt_col->getTimeZone();
            else
            {
                local_time_zone = &extractTimeZoneFromFunctionArguments(arguments, 1, 0);
            }

            if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort || parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                utc_time_zone = &DateLUT::instance("UTC");
        }

        const IColumn * col_from = arguments[0].column.get();
        const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
        const ColumnFixedString * col_from_fixed_string = checkAndGetColumn<ColumnFixedString>(col_from);

        if (std::is_same_v<FromDataType, DataTypeString> && !col_from_string)
            throw Exception("Illegal column " + col_from->getName()
                + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);

        if (std::is_same_v<FromDataType, DataTypeFixedString> && !col_from_fixed_string)
            throw Exception("Illegal column " + col_from->getName()
                + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);

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

        for (size_t i = 0; i < size; ++i)
        {
            size_t next_offset = std::is_same_v<FromDataType, DataTypeString> ? (*offsets)[i] : (current_offset + fixed_string_size);
            size_t string_size = std::is_same_v<FromDataType, DataTypeString> ? next_offset - current_offset - 1 : fixed_string_size;

            ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);

            if constexpr (exception_mode == ConvertFromStringExceptionMode::Throw)
            {
                if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parseDateTime64BestEffort(res, vec_to.getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parseDateTimeBestEffort(res, read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                }
                else if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                {
                    time_t res;
                    parseDateTimeBestEffortUS(res, read_buffer, *local_time_zone, *utc_time_zone);
                    vec_to[i] = res;
                }
                else
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 value = 0;
                        readDateTime64Text(value, vec_to.getScale(), read_buffer, *local_time_zone);
                        vec_to[i] = value;
                    }
                    else if constexpr (IsDataTypeDecimal<ToDataType>)
                        ToDataType::readText(vec_to[i], read_buffer, ToDataType::maxPrecision(), vec_to.getScale());
                    else
                        parseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone);
                }

                if (!isAllRead(read_buffer))
                    throwExceptionForIncompletelyParsedValue(read_buffer, res_type);
            }
            else
            {
                bool parsed;

                if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parsed = tryParseDateTime64BestEffort(res, vec_to.getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parsed = tryParseDateTimeBestEffort(res, read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                }
                else
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 value = 0;
                        parsed = tryReadDateTime64Text(value, vec_to.getScale(), read_buffer, *local_time_zone);
                        vec_to[i] = value;
                    }
                    else if constexpr (IsDataTypeDecimal<ToDataType>)
                        parsed = ToDataType::tryReadText(vec_to[i], read_buffer, ToDataType::maxPrecision(), vec_to.getScale());
                    else
                        parsed = tryParseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone);
                }

                parsed = parsed && isAllRead(read_buffer);

                if (!parsed)
                    vec_to[i] = static_cast<typename ToDataType::FieldType>(0);

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


template <typename ToDataType, typename Name>
struct ConvertImpl<std::enable_if_t<!std::is_same_v<ToDataType, DataTypeString>, DataTypeString>, ToDataType, Name>
    : ConvertThroughParsing<DataTypeString, ToDataType, Name, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::Normal> {};

template <typename ToDataType, typename Name>
struct ConvertImpl<std::enable_if_t<!std::is_same_v<ToDataType, DataTypeFixedString>, DataTypeFixedString>, ToDataType, Name>
    : ConvertThroughParsing<DataTypeFixedString, ToDataType, Name, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::Normal> {};

/// Generic conversion of any type from String. Used for complex types: Array and Tuple.
struct ConvertImplGenericFromString
{
    static ColumnPtr execute(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const IColumn & col_from = *arguments[0].column;
        size_t size = col_from.size();

        const IDataType & data_type_to = *result_type;

        if (const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(&col_from))
        {
            auto res = data_type_to.createColumn();

            IColumn & column_to = *res;
            column_to.reserve(size);

            const ColumnString::Chars & chars = col_from_string->getChars();
            const IColumn::Offsets & offsets = col_from_string->getOffsets();

            size_t current_offset = 0;

            FormatSettings format_settings;
            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory read_buffer(&chars[current_offset], offsets[i] - current_offset - 1);

                data_type_to.deserializeAsWholeText(column_to, read_buffer, format_settings);

                if (!read_buffer.eof())
                    throwExceptionForIncompletelyParsedValue(read_buffer, result_type);

                current_offset = offsets[i];
            }

            return res;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of conversion function from string",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <>
struct ConvertImpl<DataTypeString, DataTypeUInt32, NameToUnixTimestamp>
    : ConvertImpl<DataTypeString, DataTypeDateTime, NameToUnixTimestamp> {};


/** If types are identical, just take reference to column.
  */
template <typename T, typename Name>
struct ConvertImpl<std::enable_if_t<!T::is_parametric, T>, T, Name>
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        return arguments[0].column;
    }
};


/** Conversion from FixedString to String.
  * Cutting sequences of zero bytes from end of strings.
  */
template <typename Name>
struct ConvertImpl<DataTypeFixedString, DataTypeString, Name>
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        if (const ColumnFixedString * col_from = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
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
                size_t bytes_to_copy = n;
                while (bytes_to_copy > 0 && data_from[offset_from + bytes_to_copy - 1] == 0)
                    --bytes_to_copy;

                memcpy(&data_to[offset_to], &data_from[offset_from], bytes_to_copy);
                offset_from += n;
                offset_to += bytes_to_copy;
                data_to[offset_to] = 0;
                ++offset_to;
                offsets_to[i] = offset_to;
            }

            data_to.resize(offset_to);
            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Declared early because used below.
struct NameToDate { static constexpr auto name = "toDate"; };
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
        static constexpr auto kind = IntervalKind::INTERVAL_KIND; \
    };

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

template<typename Name, typename ToDataType>
static inline bool isDateTime64(const ColumnsWithTypeAndName & arguments)
{
    if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        return true;
    else if constexpr (std::is_same_v<Name, NameToDateTime> || std::is_same_v<Name, NameParseDateTimeBestEffort>
        || std::is_same_v<Name, NameParseDateTimeBestEffortOrZero> || std::is_same_v<Name, NameParseDateTimeBestEffortOrNull>)
    {
        return (arguments.size() == 2 && isUnsignedInteger(arguments[1].type)) || arguments.size() == 3;
    }

    return false;
}

template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction
{
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;
    static constexpr bool to_decimal =
        std::is_same_v<Name, NameToDecimal32> || std::is_same_v<Name, NameToDecimal64>
         || std::is_same_v<Name, NameToDecimal128> || std::is_same_v<Name, NameToDecimal256>;

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvert>(); }
    static FunctionPtr create() { return std::make_shared<FunctionConvert>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return std::is_same_v<Name, NameToString>; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args = {{"Value", nullptr, nullptr, nullptr}};
        FunctionArgumentDescriptors optional_args;

        if constexpr (to_decimal)
        {
            mandatory_args.push_back({"scale", &isNativeInteger, &isColumnConst, "const Integer"});
        }

        if (!to_decimal && isDateTime64<Name, ToDataType>(arguments))
        {
            mandatory_args.push_back({"scale", &isNativeInteger, &isColumnConst, "const Integer"});
        }

        // toString(DateTime or DateTime64, [timezone: String])
        if ((std::is_same_v<Name, NameToString> && !arguments.empty() && (isDateTime64(arguments[0].type) || isDateTime(arguments[0].type)))
            // toUnixTimestamp(value[, timezone : String])
            || std::is_same_v<Name, NameToUnixTimestamp>
            // toDate(value[, timezone : String])
            || std::is_same_v<ToDataType, DataTypeDate> // TODO: shall we allow timestamp argument for toDate? DateTime knows nothing about timezones and this argument is ignored below.
            // toDateTime(value[, timezone: String])
            || std::is_same_v<ToDataType, DataTypeDateTime>
            // toDateTime64(value, scale : Integer[, timezone: String])
            || std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            optional_args.push_back({"timezone", &isString, &isColumnConst, "const String"});
        }

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

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

            throw Exception("Something wrong with toDecimalNN()", ErrorCodes::LOGICAL_ERROR);
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
                        extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0));

                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0));
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0));
            else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);
            else
                return std::make_shared<ToDataType>();
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
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
                || e.code() == ErrorCodes::CANNOT_PARSE_UUID)
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
    ColumnPtr executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        if (arguments.empty())
            throw Exception{"Function " + getName() + " expects at least 1 arguments",
               ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

        const IDataType * from_type = arguments[0].type.get();
        ColumnPtr result_column;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if constexpr (IsDataTypeDecimal<RightDataType>)
            {
                if constexpr (std::is_same_v<RightDataType, DataTypeDateTime64>)
                {
                    // account for optional timezone argument
                    if (arguments.size() != 2 && arguments.size() != 3)
                        throw Exception{"Function " + getName() + " expects 2 or 3 arguments for DataTypeDateTime64.",
                            ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
                }
                else if (arguments.size() != 2)
                {
                    throw Exception{"Function " + getName() + " expects 2 arguments for Decimal.",
                        ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
                }

                const ColumnWithTypeAndName & scale_column = arguments[1];
                UInt32 scale = extractToDecimalScale(scale_column);

                result_column = ConvertImpl<LeftDataType, RightDataType, Name>::execute(arguments, result_type, input_rows_count, scale);
            }
            else if constexpr (IsDataTypeDateOrDateTime<RightDataType> && std::is_same_v<LeftDataType, DataTypeDateTime64>)
            {
                const auto * dt64 = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
                result_column = ConvertImpl<LeftDataType, RightDataType, Name>::execute(arguments, result_type, input_rows_count, dt64->getScale());
            }
            else if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftT = typename LeftDataType::FieldType;
                using RightT = typename RightDataType::FieldType;

                static constexpr bool bad_left =
                    IsDecimalNumber<LeftT> || std::is_floating_point_v<LeftT> || is_big_int_v<LeftT> || is_signed_v<LeftT>;
                static constexpr bool bad_right =
                    IsDecimalNumber<RightT> || std::is_floating_point_v<RightT> || is_big_int_v<RightT> || is_signed_v<RightT>;

                /// Disallow int vs UUID conversion (but support int vs UInt128 conversion)
                if constexpr ((bad_left && std::is_same_v<RightDataType, DataTypeUUID>) ||
                              (bad_right && std::is_same_v<LeftDataType, DataTypeUUID>))
                {
                    throw Exception("Wrong UUID conversion", ErrorCodes::CANNOT_CONVERT_TYPE);
                }
                else
                    result_column = ConvertImpl<LeftDataType, RightDataType, Name>::execute(arguments, result_type, input_rows_count);
            }
            else
                result_column = ConvertImpl<LeftDataType, RightDataType, Name>::execute(arguments, result_type, input_rows_count);

            return true;
        };

        if (isDateTime64<Name, ToDataType>(arguments))
        {
            /// For toDateTime('xxxx-xx-xx xx:xx:xx.00', 2[, 'timezone']) we need to it convert to DateTime64
            const ColumnWithTypeAndName & scale_column = arguments[1];
            UInt32 scale = extractToDecimalScale(scale_column);

            if (to_datetime64 || scale != 0) /// When scale = 0, the data type is DateTime otherwise the data type is DateTime64
            {
                if (!callOnIndexAndDataType<DataTypeDateTime64>(from_type->getTypeId(), call))
                    throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                return result_column;
            }
        }

        bool done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call);
        if (!done)
        {
            /// Generic conversion of any type to String.
            if (std::is_same_v<ToDataType, DataTypeString>)
            {
                return ConvertImplGenericToString::execute(arguments);
            }
            else
                throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
    static constexpr bool to_decimal =
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>> ||
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>>;

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvertFromString>(); }
    static FunctionPtr create() { return std::make_shared<FunctionConvertFromString>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypePtr res;

        if (isDateTime64<Name, ToDataType>(arguments))
        {
            validateFunctionArgumentTypes(*this, arguments,
                FunctionArgumentDescriptors{{"string", isStringOrFixedString, nullptr, "String or FixedString"}},
                // optional
                FunctionArgumentDescriptors{
                    {"precision", isUInt8, isColumnConst, "const UInt8"},
                    {"timezone", isStringOrFixedString, isColumnConst, "const String or FixedString"},
                });

            UInt64 scale = to_datetime64 ? DataTypeDateTime64::default_scale : 0;
            if (arguments.size() > 1)
                scale = extractToDecimalScale(arguments[1]);
            const auto timezone = extractTimeZoneNameFromFunctionArguments(arguments, 2, 0);

            res = scale == 0 ? res = std::make_shared<DataTypeDateTime>(timezone) : std::make_shared<DataTypeDateTime64>(scale, timezone);
        }
        else
        {
            if ((arguments.size() != 1 && arguments.size() != 2) || (to_decimal && arguments.size() != 2))
                throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size()) +
                    ", should be 1 or 2. Second argument only make sense for DateTime (time zone, optional) and Decimal (scale).",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!isStringOrFixedString(arguments[0].type))
            {
                if (this->getName().find("OrZero") != std::string::npos ||
                    this->getName().find("OrNull") != std::string::npos)
                    throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() +
                            ". Conversion functions with postfix 'OrZero' or 'OrNull'  should take String argument",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                else
                    throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            if (arguments.size() == 2)
            {
                if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                {
                    if (!isString(arguments[1].type))
                        throw Exception("Illegal type " + arguments[1].type->getName() + " of 2nd argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
                else if constexpr (to_decimal)
                {
                    if (!isInteger(arguments[1].type))
                        throw Exception("Illegal type " + arguments[1].type->getName() + " of 2nd argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    if (!arguments[1].column)
                        throw Exception("Second argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
                }
                else
                {
                    throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                        + toString(arguments.size()) + ", should be 1. Second argument makes sense only for DateTime and Decimal.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                res = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
            else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);
            else if constexpr (to_decimal)
            {
                UInt64 scale = extractToDecimalScale(arguments[1]);
                res = createDecimalMaxPrecision<typename ToDataType::FieldType>(scale);
                if (!res)
                    throw Exception("Something wrong with toDecimalNNOrZero() or toDecimalNNOrNull()", ErrorCodes::LOGICAL_ERROR);
            }
            else
                res = std::make_shared<ToDataType>();
        }

        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
            res = std::make_shared<DataTypeNullable>(res);

        return res;
    }

    template <typename ConvertToDataType>
    ColumnPtr executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, UInt32 scale = 0) const
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
            result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count,
                assert_cast<const ToDataType &>(*removeNullable(result_type)).getScale());
        else
        {
            if (isDateTime64<Name, ToDataType>(arguments))
            {
                UInt64 scale = to_datetime64 ? DataTypeDateTime64::default_scale : 0;
                if (arguments.size() > 1)
                    scale = extractToDecimalScale(arguments[1]);

                if (scale == 0)
                    result_column = executeInternal<DataTypeDateTime>(arguments, result_type, input_rows_count);
                else
                {
                    result_column = executeInternal<DataTypeDateTime64>(arguments, result_type, input_rows_count, static_cast<UInt32>(scale));
                }
            }
            else
            {
                result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count);
            }
        }

        if (!result_column)
            throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                + ". Only String or FixedString argument is accepted for try-conversion function."
                + " For other arguments, use function without 'orZero' or 'orNull'.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return result_column;
    }
};


/// Monotonicity.

struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { true };
    }
};

struct UnknownMonotonicity
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { false };
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
            return { true, true, true };

        /// Float cases.

        /// When converting to Float, the conversion is always monotonic.
        if (std::is_floating_point_v<T>)
            return {true, true, true};

        /// If converting from Float, for monotonicity, arguments must fit in range of result type.
        if (WhichDataType(type).isFloat())
        {
            if (left.isNull() || right.isNull())
                return {};

            Float64 left_float = left.get<Float64>();
            Float64 right_float = right.get<Float64>();

            if (left_float >= static_cast<Float64>(std::numeric_limits<T>::min())
                && left_float <= static_cast<Float64>(std::numeric_limits<T>::max())
                && right_float >= static_cast<Float64>(std::numeric_limits<T>::min())
                && right_float <= static_cast<Float64>(std::numeric_limits<T>::max()))
                return { true };

            return {};
        }

        /// Integer cases.

        const bool from_is_unsigned = type.isValueRepresentedByUnsignedInteger();
        const bool to_is_unsigned = is_unsigned_v<T>;

        const size_t size_of_from = type.getSizeOfValueInMemory();
        const size_t size_of_to = sizeof(T);

        const bool left_in_first_half = left.isNull()
            ? from_is_unsigned
            : (left.get<Int64>() >= 0);

        const bool right_in_first_half = right.isNull()
            ? !from_is_unsigned
            : (right.get<Int64>() >= 0);

        /// Size of type is the same.
        if (size_of_from == size_of_to)
        {
            if (from_is_unsigned == to_is_unsigned)
                return {true, true, true};

            if (left_in_first_half == right_in_first_half)
                return {true};

            return {};
        }

        /// Size of type is expanded.
        if (size_of_from < size_of_to)
        {
            if (from_is_unsigned == to_is_unsigned)
                return {true, true, true};

            if (!to_is_unsigned)
                return {true, true, true};

            /// signed -> unsigned. If arguments from the same half, then function is monotonic.
            if (left_in_first_half == right_in_first_half)
                return {true};

            return {};
        }

        /// Size of type is shrunk.
        if (size_of_from > size_of_to)
        {
            /// Function cannot be monotonic on unbounded ranges.
            if (left.isNull() || right.isNull())
                return {};

            /// Function cannot be monotonic when left and right are not on the same ranges.
            if (divideByRangeOfType(left.get<UInt64>()) != divideByRangeOfType(right.get<UInt64>()))
                return {};

            if (to_is_unsigned)
                return {true};
            else
                // If To is signed, it's possible that the signedness is different after conversion. So we check it explicitly.
                return {(T(left.get<UInt64>()) >= 0) == (T(right.get<UInt64>()) >= 0)};
        }

        __builtin_unreachable();
    }
};

struct ToDateMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        auto which = WhichDataType(type);
        if (which.isDateOrDateTime() || which.isInt8() || which.isInt16() || which.isUInt8() || which.isUInt16())
            return {true, true, true};
        else if (
            (which.isUInt() && ((left.isNull() || left.get<UInt64>() < 0xFFFF) && (right.isNull() || right.get<UInt64>() >= 0xFFFF)))
            || (which.isInt() && ((left.isNull() || left.get<Int64>() < 0xFFFF) && (right.isNull() || right.get<Int64>() >= 0xFFFF)))
            || (which.isFloat() && ((left.isNull() || left.get<Float64>() < 0xFFFF) && (right.isNull() || right.get<Float64>() >= 0xFFFF)))
            || !type.isValueRepresentedByNumber())
            return {};
        else
            return {true, true, true};
    }
};

struct ToDateTimeMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field &, const Field &)
    {
        if (type.isValueRepresentedByNumber())
            return {true, true, true};
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
        IFunction::Monotonicity positive(true, true);
        IFunction::Monotonicity not_monotonic;

        const auto * type_ptr = &type;
        if (const auto * low_cardinality_type = checkAndGetDataType<DataTypeLowCardinality>(type_ptr))
            type_ptr = low_cardinality_type->getDictionaryType().get();

        /// `toString` function is monotonous if the argument is Date or DateTime or String, or non-negative numbers with the same number of symbols.
        if (checkDataTypes<DataTypeDate, DataTypeDateTime, DataTypeString>(type_ptr))
            return positive;

        if (left.isNull() || right.isNull())
            return {};

        if (left.getType() == Field::Types::UInt64
            && right.getType() == Field::Types::UInt64)
        {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0)
                || (floor(log10(left.get<UInt64>())) == floor(log10(right.get<UInt64>())))
                ? positive : not_monotonic;
        }

        if (left.getType() == Field::Types::Int64
            && right.getType() == Field::Types::Int64)
        {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0)
                || (left.get<Int64>() > 0 && right.get<Int64>() > 0 && floor(log10(left.get<Int64>())) == floor(log10(right.get<Int64>())))
                ? positive : not_monotonic;
        }

        return not_monotonic;
    }
};


struct NameToUInt8 { static constexpr auto name = "toUInt8"; };
struct NameToUInt16 { static constexpr auto name = "toUInt16"; };
struct NameToUInt32 { static constexpr auto name = "toUInt32"; };
struct NameToUInt64 { static constexpr auto name = "toUInt64"; };
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

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8, ToNumberMonotonicity<UInt8>>;
using FunctionToUInt16 = FunctionConvert<DataTypeUInt16, NameToUInt16, ToNumberMonotonicity<UInt16>>;
using FunctionToUInt32 = FunctionConvert<DataTypeUInt32, NameToUInt32, ToNumberMonotonicity<UInt32>>;
using FunctionToUInt64 = FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;
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
using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime, ToDateTimeMonotonicity>;
using FunctionToDateTime32 = FunctionConvert<DataTypeDateTime, NameToDateTime32, ToDateTimeMonotonicity>;
using FunctionToDateTime64 = FunctionConvert<DataTypeDateTime64, NameToDateTime64, UnknownMonotonicity>;
using FunctionToUUID = FunctionConvert<DataTypeUUID, NameToUUID, ToNumberMonotonicity<UInt128>>;
using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToUnixTimestamp = FunctionConvert<DataTypeUInt32, NameToUnixTimestamp, ToNumberMonotonicity<UInt32>>;
using FunctionToDecimal32 = FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
using FunctionToDecimal64 = FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
using FunctionToDecimal128 = FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;
using FunctionToDecimal256 = FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256, UnknownMonotonicity>;


template <typename DataType> struct FunctionTo;

template <> struct FunctionTo<DataTypeUInt8> { using Type = FunctionToUInt8; };
template <> struct FunctionTo<DataTypeUInt16> { using Type = FunctionToUInt16; };
template <> struct FunctionTo<DataTypeUInt32> { using Type = FunctionToUInt32; };
template <> struct FunctionTo<DataTypeUInt64> { using Type = FunctionToUInt64; };
template <> struct FunctionTo<DataTypeUInt256> { using Type = FunctionToUInt256; };
template <> struct FunctionTo<DataTypeInt8> { using Type = FunctionToInt8; };
template <> struct FunctionTo<DataTypeInt16> { using Type = FunctionToInt16; };
template <> struct FunctionTo<DataTypeInt32> { using Type = FunctionToInt32; };
template <> struct FunctionTo<DataTypeInt64> { using Type = FunctionToInt64; };
template <> struct FunctionTo<DataTypeInt128> { using Type = FunctionToInt128; };
template <> struct FunctionTo<DataTypeInt256> { using Type = FunctionToInt256; };
template <> struct FunctionTo<DataTypeFloat32> { using Type = FunctionToFloat32; };
template <> struct FunctionTo<DataTypeFloat64> { using Type = FunctionToFloat64; };
template <> struct FunctionTo<DataTypeDate> { using Type = FunctionToDate; };
template <> struct FunctionTo<DataTypeDateTime> { using Type = FunctionToDateTime; };
template <> struct FunctionTo<DataTypeDateTime64> { using Type = FunctionToDateTime64; };
template <> struct FunctionTo<DataTypeUUID> { using Type = FunctionToUUID; };
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
struct NameToDateTimeOrZero { static constexpr auto name = "toDateTimeOrZero"; };
struct NameToDateTime64OrZero { static constexpr auto name = "toDateTime64OrZero"; };
struct NameToDecimal32OrZero { static constexpr auto name = "toDecimal32OrZero"; };
struct NameToDecimal64OrZero { static constexpr auto name = "toDecimal64OrZero"; };
struct NameToDecimal128OrZero { static constexpr auto name = "toDecimal128OrZero"; };
struct NameToDecimal256OrZero { static constexpr auto name = "toDecimal256OrZero"; };
struct NameToUUIDOrZero { static constexpr auto name = "toUUIDOrZero"; };

using FunctionToUInt8OrZero = FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt16OrZero = FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt32OrZero = FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt64OrZero = FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrZero, ConvertFromStringExceptionMode::Zero>;
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
using FunctionToDateTimeOrZero = FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateTime64OrZero = FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal32OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal64OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal128OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal256OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUUIDOrZero = FunctionConvertFromString<DataTypeUUID, NameToUUIDOrZero, ConvertFromStringExceptionMode::Zero>;

struct NameToUInt8OrNull { static constexpr auto name = "toUInt8OrNull"; };
struct NameToUInt16OrNull { static constexpr auto name = "toUInt16OrNull"; };
struct NameToUInt32OrNull { static constexpr auto name = "toUInt32OrNull"; };
struct NameToUInt64OrNull { static constexpr auto name = "toUInt64OrNull"; };
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
struct NameToDateTimeOrNull { static constexpr auto name = "toDateTimeOrNull"; };
struct NameToDateTime64OrNull { static constexpr auto name = "toDateTime64OrNull"; };
struct NameToDecimal32OrNull { static constexpr auto name = "toDecimal32OrNull"; };
struct NameToDecimal64OrNull { static constexpr auto name = "toDecimal64OrNull"; };
struct NameToDecimal128OrNull { static constexpr auto name = "toDecimal128OrNull"; };
struct NameToDecimal256OrNull { static constexpr auto name = "toDecimal256OrNull"; };
struct NameToUUIDOrNull { static constexpr auto name = "toUUIDOrNull"; };

using FunctionToUInt8OrNull = FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt16OrNull = FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt32OrNull = FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt64OrNull = FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrNull, ConvertFromStringExceptionMode::Null>;
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
using FunctionToDateTimeOrNull = FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateTime64OrNull = FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal32OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal64OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal128OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal256OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUUIDOrNull = FunctionConvertFromString<DataTypeUUID, NameToUUIDOrNull, ConvertFromStringExceptionMode::Null>;

struct NameParseDateTimeBestEffort { static constexpr auto name = "parseDateTimeBestEffort"; };
struct NameParseDateTimeBestEffortUS { static constexpr auto name = "parseDateTimeBestEffortUS"; };
struct NameParseDateTimeBestEffortOrZero { static constexpr auto name = "parseDateTimeBestEffortOrZero"; };
struct NameParseDateTimeBestEffortOrNull { static constexpr auto name = "parseDateTimeBestEffortOrNull"; };
struct NameParseDateTime32BestEffort { static constexpr auto name = "parseDateTime32BestEffort"; };
struct NameParseDateTime32BestEffortOrZero { static constexpr auto name = "parseDateTime32BestEffortOrZero"; };
struct NameParseDateTime32BestEffortOrNull { static constexpr auto name = "parseDateTime32BestEffortOrNull"; };
struct NameParseDateTime64BestEffort { static constexpr auto name = "parseDateTime64BestEffort"; };
struct NameParseDateTime64BestEffortOrZero { static constexpr auto name = "parseDateTime64BestEffortOrZero"; };
struct NameParseDateTime64BestEffortOrNull { static constexpr auto name = "parseDateTime64BestEffortOrNull"; };


using FunctionParseDateTimeBestEffort = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTimeBestEffortUS = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUS, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTimeBestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTimeBestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

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

class ExecutableFunctionCast : public IExecutableFunctionImpl
{
public:
    using WrapperType = std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t)>;

    struct Diagnostic
    {
        std::string column_from;
        std::string column_to;
    };

    explicit ExecutableFunctionCast(
            WrapperType && wrapper_function_, const char * name_, std::optional<Diagnostic> diagnostic_)
            : wrapper_function(std::move(wrapper_function_)), name(name_), diagnostic(std::move(diagnostic_)) {}

    String getName() const override { return name; }

protected:
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
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
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char * name;
    std::optional<Diagnostic> diagnostic;
};


struct NameCast { static constexpr auto name = "CAST"; };

class FunctionCast final : public IFunctionBaseImpl
{
public:
    using WrapperType = std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t)>;
    using MonotonicityForRange = std::function<Monotonicity(const IDataType &, const Field &, const Field &)>;
    using Diagnostic = ExecutableFunctionCast::Diagnostic;

    FunctionCast(const char * name_, MonotonicityForRange && monotonicity_for_range_
        , const DataTypes & argument_types_, const DataTypePtr & return_type_, std::optional<Diagnostic> diagnostic_)
        : name(name_), monotonicity_for_range(monotonicity_for_range_)
        , argument_types(argument_types_), return_type(return_type_), diagnostic(std::move(diagnostic_))
    {
    }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName & /*sample_columns*/) const override
    {
        try
        {
            return std::make_unique<ExecutableFunctionCast>(
                    prepareUnpackDictionaries(getArgumentTypes()[0], getResultType()), name, diagnostic);
        }
        catch (Exception & e)
        {
            if (diagnostic)
                e.addMessage("while converting source column " + backQuoteIfNeed(diagnostic->column_from) +
                             " to destination column " + backQuoteIfNeed(diagnostic->column_to));
            throw;
        }
    }

    String getName() const override { return name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool hasInformationAboutMonotonicity() const override
    {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return monotonicity_for_range(type, left, right);
    }

private:

    const char * name;
    MonotonicityForRange monotonicity_for_range;

    DataTypes argument_types;
    DataTypePtr return_type;

    std::optional<Diagnostic> diagnostic;

    template <typename DataType>
    WrapperType createWrapper(const DataTypePtr & from_type, const DataType * const, bool requested_result_is_nullable) const
    {
        FunctionPtr function;

        if (requested_result_is_nullable && checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            /// In case when converting to Nullable type, we apply different parsing rule,
            /// that will not throw an exception but return NULL in case of malformed input.
            function = FunctionConvertFromString<DataType, NameCast, ConvertFromStringExceptionMode::Null>::create();
        }
        else
            function = FunctionTo<DataType>::Type::create();

        auto function_adaptor =
                FunctionOverloadResolverAdaptor(std::make_unique<DefaultOverloadResolver>(function))
                .build({ColumnWithTypeAndName{nullptr, from_type, ""}});

        return [function_adaptor] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            return function_adaptor->execute(arguments, result_type, input_rows_count);
        };
    }

    static WrapperType createStringWrapper(const DataTypePtr & from_type)
    {
        FunctionPtr function = FunctionToString::create();

        auto function_adaptor =
                FunctionOverloadResolverAdaptor(std::make_unique<DefaultOverloadResolver>(function))
                .build({ColumnWithTypeAndName{nullptr, from_type, ""}});

        return [function_adaptor] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            return function_adaptor->execute(arguments, result_type, input_rows_count);
        };
    }

    static WrapperType createFixedStringWrapper(const DataTypePtr & from_type, const size_t N)
    {
        if (!isStringOrFixedString(from_type))
            throw Exception{"CAST AS FixedString is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED};

        return [N] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/)
        {
            return FunctionToFixedString::executeForN(arguments, N);
        };
    }

    static WrapperType createUUIDWrapper(const DataTypePtr & from_type, const DataTypeUUID * const, bool requested_result_is_nullable)
    {
        if (requested_result_is_nullable)
            throw Exception{"CAST AS Nullable(UUID) is not implemented", ErrorCodes::NOT_IMPLEMENTED};

        FunctionPtr function = FunctionTo<DataTypeUUID>::Type::create();

        auto function_adaptor =
                FunctionOverloadResolverAdaptor(std::make_unique<DefaultOverloadResolver>(function))
                .build({ColumnWithTypeAndName{nullptr, from_type, ""}});

        return [function_adaptor] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            return function_adaptor->execute(arguments, result_type, input_rows_count);
        };
    }

    template <typename ToDataType>
    std::enable_if_t<IsDataTypeDecimal<ToDataType>, WrapperType>
    createDecimalWrapper(const DataTypePtr & from_type, const ToDataType * to_type) const
    {
        TypeIndex type_index = from_type->getTypeId();
        UInt32 scale = to_type->getScale();

        WhichDataType which(type_index);
        bool ok = which.isNativeInt() ||
            which.isNativeUInt() ||
            which.isDecimal() ||
            which.isFloat() ||
            which.isDateOrDateTime() ||
            which.isStringOrFixedString();
        if (!ok)
            throw Exception{"Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported",
                ErrorCodes::CANNOT_CONVERT_TYPE};

        return [type_index, scale, to_type] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            ColumnPtr result_column;
            auto res = callOnIndexAndDataType<ToDataType>(type_index, [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using LeftDataType = typename Types::LeftType;
                using RightDataType = typename Types::RightType;

                result_column = ConvertImpl<LeftDataType, RightDataType, NameCast>::execute(arguments, result_type, input_rows_count, scale);
                return true;
            });

            /// Additionally check if callOnIndexAndDataType wasn't called at all.
            if (!res)
            {
                throw Exception{"Conversion from " + std::string(getTypeName(type_index)) + " to " + to_type->getName() +
                                " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
            }

            return result_column;
        };
    }

    static WrapperType createAggregateFunctionWrapper(const DataTypePtr & from_type_untyped, const DataTypeAggregateFunction * to_type)
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t /*input_rows_count*/)
            {
                return ConvertImplGenericFromString::execute(arguments, result_type);
            };
        }
        else
            throw Exception{"Conversion from " + from_type_untyped->getName() + " to " + to_type->getName() +
                " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
    }

    WrapperType createArrayWrapper(const DataTypePtr & from_type_untyped, const DataTypeArray * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t /*input_rows_count*/)
            {
                return ConvertImplGenericFromString::execute(arguments, result_type);
            };
        }

        DataTypePtr from_nested_type;
        DataTypePtr to_nested_type;
        const auto * from_type = checkAndGetDataType<DataTypeArray>(from_type_untyped.get());

        /// get the most nested type
        if (from_type && to_type)
        {
            from_nested_type = from_type->getNestedType();
            to_nested_type = to_type->getNestedType();

            from_type = checkAndGetDataType<DataTypeArray>(from_nested_type.get());
            to_type = checkAndGetDataType<DataTypeArray>(to_nested_type.get());
        }

        /// both from_type and to_type should be nullptr now is array types had same dimensions
        if ((from_type == nullptr) != (to_type == nullptr))
            throw Exception{"CAST AS Array can only be performed between same-dimensional array types or from String",
                ErrorCodes::TYPE_MISMATCH};

        /// Prepare nested type conversion
        const auto nested_function = prepareUnpackDictionaries(from_nested_type, to_nested_type);

        return [nested_function, from_nested_type, to_nested_type](
                ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto & array_arg = arguments.front();

            if (const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(array_arg.column.get()))
            {
                /// create columns for converting nested column containing original and result columns
                ColumnsWithTypeAndName nested_columns{{ col_array->getDataPtr(), from_nested_type, "" }};

                /// convert nested column
                auto result_column = nested_function(nested_columns, to_nested_type, nullable_source, nested_columns.front().column->size());

                /// set converted nested column to result
                return ColumnArray::create(result_column, col_array->getOffsetsPtr());
            }
            else
                throw Exception{"Illegal column " + array_arg.column->getName() + " for function CAST AS Array", ErrorCodes::LOGICAL_ERROR};
        };
    }

    WrapperType createTupleWrapper(const DataTypePtr & from_type_untyped, const DataTypeTuple * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t /*input_rows_count*/)
            {
                return ConvertImplGenericFromString::execute(arguments, result_type);
            };
        }

        const auto * from_type = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get());
        if (!from_type)
            throw Exception{"CAST AS Tuple can only be performed between tuple types or from String.\nLeft type: "
                + from_type_untyped->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};

        if (from_type->getElements().size() != to_type->getElements().size())
            throw Exception{"CAST AS Tuple can only be performed between tuple types with the same number of elements or from String.\n"
                "Left type: " + from_type->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};

        const auto & from_element_types = from_type->getElements();
        const auto & to_element_types = to_type->getElements();
        std::vector<WrapperType> element_wrappers;
        element_wrappers.reserve(from_element_types.size());

        /// Create conversion wrapper for each element in tuple
        for (const auto idx_type : ext::enumerate(from_type->getElements()))
            element_wrappers.push_back(prepareUnpackDictionaries(idx_type.second, to_element_types[idx_type.first]));

        return [element_wrappers, from_element_types, to_element_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();

            size_t tuple_size = from_element_types.size();
            const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*col);

            Columns converted_columns(tuple_size);

            /// invoke conversion for each element
            for (size_t i = 0; i < tuple_size; ++i)
            {
                ColumnsWithTypeAndName element = {{column_tuple.getColumns()[i], from_element_types[i], "" }};
                converted_columns[i] = element_wrappers[i](element, to_element_types[i], nullable_source, input_rows_count);
            }

            return ColumnTuple::create(converted_columns);
        };
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
            auto function = Function::create();
            auto func_or_adaptor = FunctionOverloadResolverAdaptor(std::make_unique<DefaultOverloadResolver>(function))
                    .build(ColumnsWithTypeAndName{{nullptr, from_type, "" }});

            return [func_or_adaptor] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
            {
                return func_or_adaptor->execute(arguments, result_type, input_rows_count);
            };
        }
        else
            throw Exception{"Conversion from " + from_type->getName() + " to " + to_type->getName() +
                " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
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
                throw Exception{"Enum conversion changes value for element '" + name_value.first +
                    "' from " + toString(old_value) + " to " + toString(new_value), ErrorCodes::CANNOT_CONVERT_TYPE};
        }
    }

    template <typename ColumnStringType, typename EnumType>
    WrapperType createStringToEnumWrapper() const
    {
        const char * function_name = name;
        return [function_name] (
            ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
        {
            const auto & first_col = arguments.front().column.get();
            const auto & result_type = typeid_cast<const EnumType &>(*res_type);

            const ColumnStringType * col = typeid_cast<const ColumnStringType *>(first_col);

            if (col && nullable_col && nullable_col->size() != col->size())
                throw Exception("ColumnNullable is not compatible with original", ErrorCodes::LOGICAL_ERROR);

            if (col)
            {
                const auto size = col->size();

                auto res = result_type.createColumn();
                auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
                out_data.resize(size);

                if (nullable_col)
                {
                    for (const auto i : ext::range(0, size))
                    {
                        if (!nullable_col->isNullAt(i))
                            out_data[i] = result_type.getValue(col->getDataAt(i));
                    }
                }
                else
                {
                    for (const auto i : ext::range(0, size))
                        out_data[i] = result_type.getValue(col->getDataAt(i));
                }

                return res;
            }
            else
                throw Exception{"Unexpected column " + first_col->getName() + " as first argument of function " + function_name,
                    ErrorCodes::LOGICAL_ERROR};
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
        const auto * from_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(from_type.get());
        const auto * to_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(to_type.get());
        const auto & from_nested = from_low_cardinality ? from_low_cardinality->getDictionaryType() : from_type;
        const auto & to_nested = to_low_cardinality ? to_low_cardinality->getDictionaryType() : to_type;

        if (from_type->onlyNull())
        {
            if (!to_nested->isNullable())
                throw Exception{"Cannot convert NULL to a non-nullable type", ErrorCodes::CANNOT_CONVERT_TYPE};

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
                    const auto * col_low_cardinality = typeid_cast<const ColumnLowCardinality *>(arguments[0].column.get());

                    if (skip_not_null_check && col_low_cardinality->containsNull())
                        throw Exception{"Cannot convert NULL value to non-Nullable type",
                                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};

                    arg.column = col_low_cardinality->getDictionary().getNestedColumn();
                    arg.type = from_low_cardinality->getDictionaryType();

                    /// TODO: Make map with defaults conversion.
                    src_converted_to_full_column = !removeNullable(arg.type)->equals(*removeNullable(res_type));
                    if (src_converted_to_full_column)
                        arg.column = arg.column->index(col_low_cardinality->getIndexes(), 0);
                    else
                        res_indexes = col_low_cardinality->getIndexesPtr();

                    tmp_rows_count = arg.column->size();
                }

                /// Perform the requested conversion.
                converted_column = wrapper(args, res_type, nullable_source, tmp_rows_count);
            }

            if (to_low_cardinality)
            {
                auto res_column = to_low_cardinality->createColumn();
                auto * col_low_cardinality = typeid_cast<ColumnLowCardinality *>(res_column.get());

                if (from_low_cardinality && !src_converted_to_full_column)
                {
                    col_low_cardinality->insertRangeFromDictionaryEncodedColumn(*converted_column, *res_indexes);
                }
                else
                    col_low_cardinality->insertRangeFromFullColumn(*converted_column, 0, converted_column->size());

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
                        throw Exception("Invalid number of arguments", ErrorCodes::LOGICAL_ERROR);
                    nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
                }

                /// Perform the requested conversion.
                auto tmp_res = wrapper(tmp_args, nested_type, nullable_source, input_rows_count);

                /// May happen in fuzzy tests. For debug purpose.
                if (!tmp_res)
                    throw Exception("Couldn't convert " + arguments[0].type->getName() + " to "
                                    + nested_type->getName() + " in " + " prepareRemoveNullable wrapper.", ErrorCodes::LOGICAL_ERROR);

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

                    if (!memoryIsZero(null_map.data(), null_map.size()))
                        throw Exception{"Cannot convert NULL value to non-Nullable type",
                                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
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
        if (from_type->equals(*to_type))
            return createIdentityWrapper(from_type);
        else if (WhichDataType(from_type).isNothing())
            return createNothingWrapper(to_type.get());

        WrapperType ret;

        auto make_default_wrapper = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (
                std::is_same_v<ToDataType, DataTypeUInt8> ||
                std::is_same_v<ToDataType, DataTypeUInt16> ||
                std::is_same_v<ToDataType, DataTypeUInt32> ||
                std::is_same_v<ToDataType, DataTypeUInt64> ||
                std::is_same_v<ToDataType, DataTypeUInt256> ||
                std::is_same_v<ToDataType, DataTypeInt8> ||
                std::is_same_v<ToDataType, DataTypeInt16> ||
                std::is_same_v<ToDataType, DataTypeInt32> ||
                std::is_same_v<ToDataType, DataTypeInt64> ||
                std::is_same_v<ToDataType, DataTypeInt128> ||
                std::is_same_v<ToDataType, DataTypeInt256> ||
                std::is_same_v<ToDataType, DataTypeFloat32> ||
                std::is_same_v<ToDataType, DataTypeFloat64> ||
                std::is_same_v<ToDataType, DataTypeDate> ||
                std::is_same_v<ToDataType, DataTypeDateTime>)
            {
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
            if constexpr (
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>> ||
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>> ||
                std::is_same_v<ToDataType, DataTypeDateTime64>)
            {
                ret = createDecimalWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()));
                return true;
            }
            if constexpr (std::is_same_v<ToDataType, DataTypeUUID>)
            {
                if (isStringOrFixedString(from_type))
                {
                    ret = createUUIDWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                    return true;
                }
            }

            return false;
        };

        if (callOnIndexAndDataType<void>(to_type->getTypeId(), make_default_wrapper))
            return ret;

        switch (to_type->getTypeId())
        {
            case TypeIndex::String:
                return createStringWrapper(from_type);
            case TypeIndex::FixedString:
                return createFixedStringWrapper(from_type, checkAndGetDataType<DataTypeFixedString>(to_type.get())->getN());

            case TypeIndex::Array:
                return createArrayWrapper(from_type, checkAndGetDataType<DataTypeArray>(to_type.get()));
            case TypeIndex::Tuple:
                return createTupleWrapper(from_type, checkAndGetDataType<DataTypeTuple>(to_type.get()));

            case TypeIndex::AggregateFunction:
                return createAggregateFunctionWrapper(from_type, checkAndGetDataType<DataTypeAggregateFunction>(to_type.get()));
            default:
                break;
        }

        throw Exception{"Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported",
            ErrorCodes::CANNOT_CONVERT_TYPE};
    }
};

class CastOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    using MonotonicityForRange = FunctionCast::MonotonicityForRange;
    using Diagnostic = FunctionCast::Diagnostic;

    static constexpr auto name = "CAST";

    static FunctionOverloadResolverImplPtr create(const Context & context);
    static FunctionOverloadResolverImplPtr createImpl(bool keep_nullable, std::optional<Diagnostic> diagnostic = {})
    {
        return std::make_unique<CastOverloadResolver>(keep_nullable, std::move(diagnostic));
    }


    explicit CastOverloadResolver(bool keep_nullable_, std::optional<Diagnostic> diagnostic_ = {})
        : keep_nullable(keep_nullable_), diagnostic(std::move(diagnostic_))
    {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

protected:

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        auto monotonicity = getMonotonicityInformation(arguments.front().type, return_type.get());
        return std::make_unique<FunctionCast>(name, std::move(monotonicity), data_types, return_type, diagnostic);
    }

    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & column = arguments.back().column;
        if (!column)
            throw Exception("Second argument to " + getName() + " must be a constant string describing type."
                " Instead there is non-constant column of type " + arguments.back().type->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * type_col = checkAndGetColumnConst<ColumnString>(column.get());
        if (!type_col)
            throw Exception("Second argument to " + getName() + " must be a constant string describing type."
                " Instead there is a column with the following structure: " + column->dumpStructure(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr type = DataTypeFactory::instance().get(type_col->getValue<String>());
        if (keep_nullable && arguments.front().type->isNullable())
            return makeNullable(type);
        return type;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    bool keep_nullable;
    std::optional<Diagnostic> diagnostic;

    template <typename DataType>
    static auto monotonicityForType(const DataType * const)
    {
        return FunctionTo<DataType>::Type::Monotonic::get;
    }

    static MonotonicityForRange getMonotonicityInformation(const DataTypePtr & from_type, const IDataType * to_type)
    {
        if (const auto * type = checkAndGetDataType<DataTypeUInt8>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt16>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt64>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt256>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt8>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt16>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt64>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt128>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt256>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeFloat32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeFloat64>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeDate>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeDateTime>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeString>(to_type))
            return monotonicityForType(type);
        if (isEnum(from_type))
        {
            if (const auto * type = checkAndGetDataType<DataTypeEnum8>(to_type))
                return monotonicityForType(type);
            if (const auto * type = checkAndGetDataType<DataTypeEnum16>(to_type))
                return monotonicityForType(type);
        }
        /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
        return {};
    }
};

}
