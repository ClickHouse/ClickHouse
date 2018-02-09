#pragma once

#include <ext/enumerate.h>
#include <ext/collection_cast.h>
#include <ext/range.h>
#include <type_traits>

#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeInterval.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Common/FieldVisitors.h>
#include <Interpreters/ExpressionActions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionsDateTime.h>
#include <Functions/FunctionHelpers.h>


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
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
}


/** Type conversion functions.
  * toType - conversion in "natural way";
  */


/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImpl
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<FromFieldType> * col_from
            = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnVector<ToFieldType>::create();

            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = static_cast<ToFieldType>(vec_from[i]);

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** Conversion of Date to DateTime: adding 00:00:00 time component.
  */
struct ToDateTimeImpl
{
    static constexpr auto name = "toDateTime";

    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum_t(d));
    }
};

template <typename Name> struct ConvertImpl<DataTypeDate, DataTypeDateTime, Name>
    : DateTimeTransformImpl<UInt16, UInt32, ToDateTimeImpl> {};


/// Implementation of toDate function.

template <typename FromType, typename ToType>
struct ToDateTransform32Or64
{
    static constexpr auto name = "toDate";

    static inline ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        return (from < 0xFFFF) ? from : time_zone.toDayNum(from);
    }
};

/** Conversion of DateTime to Date: throw off time component.
  */
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDate, Name>
    : DateTimeTransformImpl<UInt32, UInt16, ToDateImpl> {};

/** Special case of converting (U)Int32 or (U)Int64 (and also, for convenience, Float32, Float64) to Date.
  * If number is less than 65536, then it is treated as DayNum, and if greater or equals, then as unix timestamp.
  * It's a bit illogical, as we actually have two functions in one.
  * But allows to support frequent case,
  *  when user write toDate(UInt32), expecting conversion of unix timestamp to Date.
  *  (otherwise such usage would be frequent mistake).
  */
template <typename Name> struct ConvertImpl<DataTypeUInt32, DataTypeDate, Name>
    : DateTimeTransformImpl<UInt32, UInt16, ToDateTransform32Or64<UInt32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDate, Name>
    : DateTimeTransformImpl<UInt64, UInt16, ToDateTransform32Or64<UInt64, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDate, Name>
    : DateTimeTransformImpl<Int32, UInt16, ToDateTransform32Or64<Int32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDate, Name>
    : DateTimeTransformImpl<Int64, UInt16, ToDateTransform32Or64<Int64, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDate, Name>
    : DateTimeTransformImpl<Float32, UInt16, ToDateTransform32Or64<Float32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDate, Name>
    : DateTimeTransformImpl<Float64, UInt16, ToDateTransform32Or64<Float64, UInt16>> {};


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
        writeDateText(DayNum_t(x), wb);
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

template <typename FieldType>
struct FormatImpl<DataTypeEnum<FieldType>>
{
    static void execute(const FieldType x, WriteBuffer & wb, const DataTypeEnum<FieldType> * type, const DateLUTImpl *)
    {
        writeString(type->getNameForValue(x), wb);
    }
};


/// DataTypeEnum<T> to DataType<T> free conversion
template <typename FieldType, typename Name>
struct ConvertImpl<DataTypeEnum<FieldType>, DataTypeNumber<FieldType>, Name>
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};


template <typename FromDataType, typename Name>
struct ConvertImpl<FromDataType, std::enable_if_t<!std::is_same_v<FromDataType, DataTypeString>, DataTypeString>, Name>
{
    using FromFieldType = typename FromDataType::FieldType;

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        const DateLUTImpl * time_zone = nullptr;

        /// For argument of DateTime type, second argument with time zone could be specified.
        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime>)
            time_zone = &extractTimeZoneFromFunctionArguments(block, arguments, 1, 0);

        if (const auto col_from = checkAndGetColumn<ColumnVector<FromFieldType>>(col_with_type_and_name.column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();

            if constexpr (std::is_same_v<FromDataType, DataTypeDate>)
                data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
            else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime>)
                data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss") + 1));
            else
                data_to.resize(size * 3);   /// Arbitary

            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                FormatImpl<FromDataType>::execute(vec_from[i], write_buffer, &type, time_zone);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            data_to.resize(write_buffer.count());

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Generic conversion of any type to String.
struct ConvertImplGenericToString
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto & col_with_type_and_name = block.getByPosition(arguments[0]);
        const IDataType & type = *col_with_type_and_name.type;
        const IColumn & col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();

        auto col_to = ColumnString::create();

        ColumnString::Chars_t & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        data_to.resize(size * 2); /// Using coefficient 2 for initial size is arbitary.
        offsets_to.resize(size);

        WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

        for (size_t i = 0; i < size; ++i)
        {
            type.serializeText(col_from, i, write_buffer);
            writeChar(0, write_buffer);
            offsets_to[i] = write_buffer.count();
        }

        data_to.resize(write_buffer.count());
        block.getByPosition(result).column = std::move(col_to);
    }
};


/** Conversion of strings to numbers, dates, datetimes: through parsing.
  */
template <typename DataType> void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    readText(x, rb);
}

template <> inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    DayNum_t tmp(0);
    readDateText(tmp, rb);
    x = tmp;
}

template <> inline void parseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
{
    time_t tmp = 0;
    readDateTimeText(tmp, rb, *time_zone);
    x = tmp;
}

template <> inline void parseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    UUID tmp;
    readText(tmp, rb);
    x = tmp;
}

template <typename DataType>
bool tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb)
{
    if constexpr (std::is_integral_v<typename DataType::FieldType>)
        return tryReadIntText(x, rb);
    else if constexpr (std::is_floating_point_v<typename DataType::FieldType>)
        return tryReadFloatText(x, rb);
    /// NOTE Need to implement for Date and DateTime too.
}


/** Throw exception with verbose message when string value is not parsed completely.
  */
void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, Block & block, size_t result);


enum class ConvertFromStringExceptionMode
{
    Throw,  /// Throw exception if value cannot be parsed.
    Zero,   /// Fill with zero or default if value cannot be parsed.
    Null    /// Return ColumnNullable with NULLs when value cannot be parsed.
};

template <typename FromDataType, typename ToDataType, typename Name, ConvertFromStringExceptionMode mode>
struct ConvertThroughParsing
{
    static_assert(std::is_same_v<FromDataType, DataTypeString> || std::is_same_v<FromDataType, DataTypeFixedString>,
        "ConvertThroughParsing is only applicable for String or FixedString data types");

    using ToFieldType = typename ToDataType::FieldType;

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

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const DateLUTImpl * time_zone [[maybe_unused]] = nullptr;

        /// For conversion to DateTime type, second argument with time zone could be specified.
        if (std::is_same_v<ToDataType, DataTypeDateTime>)
            time_zone = &extractTimeZoneFromFunctionArguments(block, arguments, 1, 0);

        const IColumn * col_from = block.getByPosition(arguments[0]).column.get();
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

        size_t size = block.rows();
        auto col_to = ColumnVector<ToFieldType>::create(size);
        typename ColumnVector<ToFieldType>::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (mode == ConvertFromStringExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        const ColumnString::Chars_t * chars = nullptr;
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

            if constexpr (mode == ConvertFromStringExceptionMode::Throw)
            {
                parseImpl<ToDataType>(vec_to[i], read_buffer, time_zone);

                if (!isAllRead(read_buffer))
                    throwExceptionForIncompletelyParsedValue(read_buffer, block, result);
            }
            else
            {
                bool parsed = tryParseImpl<ToDataType>(vec_to[i], read_buffer) && isAllRead(read_buffer);

                if (!parsed)
                    vec_to[i] = 0;

                if constexpr (mode == ConvertFromStringExceptionMode::Null)
                    (*vec_null_map_to)[i] = !parsed;
            }

            current_offset = next_offset;
        }

        if constexpr (mode == ConvertFromStringExceptionMode::Null)
            block.getByPosition(result).column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            block.getByPosition(result).column = std::move(col_to);
    }
};


template <typename ToDataType, typename Name>
struct ConvertImpl<std::enable_if_t<!std::is_same_v<ToDataType, DataTypeString>, DataTypeString>, ToDataType, Name>
    : ConvertThroughParsing<DataTypeString, ToDataType, Name, ConvertFromStringExceptionMode::Throw> {};

template <typename ToDataType, typename Name>
struct ConvertImpl<std::enable_if_t<!std::is_same_v<ToDataType, DataTypeFixedString>, DataTypeFixedString>, ToDataType, Name>
    : ConvertThroughParsing<DataTypeFixedString, ToDataType, Name, ConvertFromStringExceptionMode::Throw> {};


/** Conversion from String through parsing, which returns default value instead of throwing an exception.
  */
template <typename ToDataType, typename Name>
struct ConvertOrZeroImpl : ConvertThroughParsing<DataTypeString, ToDataType, Name, ConvertFromStringExceptionMode::Zero> {};

template <typename ToDataType, typename Name>
struct ConvertOrNullImpl : ConvertThroughParsing<DataTypeString, ToDataType, Name, ConvertFromStringExceptionMode::Null> {};


/// Generic conversion of any type from String. Used for complex types: Array and Tuple.
struct ConvertImplGenericFromString
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const IColumn & col_from = *block.getByPosition(arguments[0]).column;
        size_t size = col_from.size();

        const IDataType & data_type_to = *block.getByPosition(result).type;

        if (const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(&col_from))
        {
            auto res = data_type_to.createColumn();

            if (!size)
                return;

            IColumn & column_to = *res;
            column_to.reserve(size);

            const ColumnString::Chars_t & chars = col_from_string->getChars();
            const IColumn::Offsets & offsets = col_from_string->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory read_buffer(&chars[current_offset], offsets[i] - current_offset - 1);

                data_type_to.deserializeTextEscaped(column_to, read_buffer);

                if (!read_buffer.eof())
                    throwExceptionForIncompletelyParsedValue(read_buffer, block, result);

                current_offset = offsets[i];
            }

            block.getByPosition(result).column = std::move(res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of conversion function from string",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Function toUnixTimestamp has exactly the same implementation as toDateTime of String type.
struct NameToUnixTimestamp { static constexpr auto name = "toUnixTimestamp"; };

template <>
struct ConvertImpl<DataTypeString, DataTypeUInt32, NameToUnixTimestamp>
    : ConvertImpl<DataTypeString, DataTypeDateTime, NameToUnixTimestamp> {};


/** If types are identical, just take reference to column.
  */
template <typename T, typename Name>
struct ConvertImpl<std::enable_if_t<!T::is_parametric, T>, T, Name>
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};


/** Conversion from FixedString to String.
  * Cutting sequences of zero bytes from end of strings.
  */
template <typename Name>
struct ConvertImpl<DataTypeFixedString, DataTypeString, Name>
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnFixedString * col_from = checkAndGetColumn<ColumnFixedString>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnString::create();

            const ColumnFixedString::Chars_t & data_from = col_from->getChars();
            ColumnString::Chars_t & data_to = col_to->getChars();
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
            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Declared early because used below.
struct NameToDate { static constexpr auto name = "toDate"; };
struct NameToDateTime { static constexpr auto name = "toDateTime"; };
struct NameToString { static constexpr auto name = "toString"; };


#define DEFINE_NAME_TO_INTERVAL(INTERVAL_KIND) \
    struct NameToInterval ## INTERVAL_KIND \
    { \
        static constexpr auto name = "toInterval" #INTERVAL_KIND; \
        static constexpr int kind = DataTypeInterval::INTERVAL_KIND; \
    };

DEFINE_NAME_TO_INTERVAL(Second)
DEFINE_NAME_TO_INTERVAL(Minute)
DEFINE_NAME_TO_INTERVAL(Hour)
DEFINE_NAME_TO_INTERVAL(Day)
DEFINE_NAME_TO_INTERVAL(Week)
DEFINE_NAME_TO_INTERVAL(Month)
DEFINE_NAME_TO_INTERVAL(Year)

#undef DEFINE_NAME_TO_INTERVAL


template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction
{
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvert>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const Block &) override { return std::is_same_v<Name, NameToString>; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (std::is_same_v<ToDataType, DataTypeInterval>)
        {
            return std::make_shared<DataTypeInterval>(DataTypeInterval::Kind(Name::kind));
        }
        else
        {
            /** Optional second argument with time zone is supported:
              * - for functions toDateTime, toUnixTimestamp, toDate;
              * - for function toString of DateTime argument.
              */

            if (arguments.size() == 2)
            {
                if (!checkAndGetDataType<DataTypeString>(arguments[1].type.get()))
                    throw Exception("Illegal type " + arguments[1].type->getName() + " of 2nd argument of function " + getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                if (!(std::is_same_v<Name, NameToDateTime>
                    || std::is_same_v<Name, NameToDate>
                    || std::is_same_v<Name, NameToUnixTimestamp>
                    || (std::is_same_v<Name, NameToString>
                        && checkDataType<DataTypeDateTime>(arguments[0].type.get()))))
                {
                    throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                        + toString(arguments.size()) + ", should be 1.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }
            }

            if (std::is_same_v<ToDataType, DataTypeDateTime>)
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
            else
                return std::make_shared<ToDataType>();
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        try
        {
            executeInternal(block, arguments, result);
        }
        catch (Exception & e)
        {
            /// More convenient error message.
            if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            {
                e.addMessage("Cannot parse "
                    + block.getByPosition(result).type->getName() + " from "
                    + block.getByPosition(arguments[0]).type->getName()
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
                    + block.getByPosition(result).type->getName() + " from "
                    + block.getByPosition(arguments[0]).type->getName());
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
    void executeInternal(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (!arguments.size())
            throw Exception{"Function " + getName() + " expects at least 1 arguments",
               ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION};

        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if      (checkDataType<DataTypeUInt8>(from_type)) ConvertImpl<DataTypeUInt8, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeUInt16>(from_type)) ConvertImpl<DataTypeUInt16, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeUInt32>(from_type)) ConvertImpl<DataTypeUInt32, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeUInt64>(from_type)) ConvertImpl<DataTypeUInt64, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeInt8>(from_type)) ConvertImpl<DataTypeInt8, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeInt16>(from_type)) ConvertImpl<DataTypeInt16, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeInt32>(from_type)) ConvertImpl<DataTypeInt32, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeInt64>(from_type)) ConvertImpl<DataTypeInt64, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeFloat32>(from_type)) ConvertImpl<DataTypeFloat32, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeFloat64>(from_type)) ConvertImpl<DataTypeFloat64, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeDate>(from_type)) ConvertImpl<DataTypeDate, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeDateTime>(from_type)) ConvertImpl<DataTypeDateTime, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeUUID>(from_type)) ConvertImpl<DataTypeUUID, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeString>(from_type)) ConvertImpl<DataTypeString, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeFixedString>(from_type)) ConvertImpl<DataTypeFixedString, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeEnum8>(from_type)) ConvertImpl<DataTypeEnum8, ToDataType, Name>::execute(block, arguments, result);
        else if (checkDataType<DataTypeEnum16>(from_type)) ConvertImpl<DataTypeEnum16, ToDataType, Name>::execute(block, arguments, result);
        else
        {
            /// Generic conversion of any type to String.
            if (std::is_same_v<ToDataType, DataTypeString>)
            {
                ConvertImplGenericToString::execute(block, arguments, result);
            }
            else
                throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
};


/** Functions toTOrZero (where T is number of date or datetime type):
  *  try to convert from String to type T through parsing,
  *  if cannot parse, return default value instead of throwing exception.
  * NOTE Also need implement tryToUnixTimestamp with timezone.
  */
template <typename ToDataType, typename Name>
class FunctionConvertOrZero : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvertOrZero>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if (checkAndGetDataType<DataTypeString>(from_type))
            ConvertOrZeroImpl<ToDataType, Name>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName()
                + ". Only String argument is accepted for try-conversion function. For other arguments, use function without 'orZero'.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


template <typename ToDataType, typename Name>
class FunctionConvertOrNull : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvertOrNull>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeNullable>(std::make_shared<ToDataType>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if (checkAndGetDataType<DataTypeString>(from_type))
            ConvertOrNullImpl<ToDataType, Name>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName()
                + ". Only String argument is accepted for try-conversion function. For other arguments, use function without 'orNull'.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


/** Conversion to fixed string is implemented only for strings.
  */
class FunctionToFixedString : public IFunction
{
public:
    static constexpr auto name = "toFixedString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToFixedString>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!arguments[1].type->isUnsignedInteger())
            throw Exception("Second argument for function " + getName() + " must be unsigned integer", ErrorCodes::ILLEGAL_COLUMN);
        if (!arguments[1].column)
            throw Exception("Second argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
        if (!arguments[0].type->isStringOrFixedString())
            throw Exception(getName() + " is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED);

        const size_t n = arguments[1].column->getUInt(0);
        return std::make_shared<DataTypeFixedString>(n);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto n = block.getByPosition(arguments[1]).column->getUInt(0);
        return execute(block, arguments, result, n);
    }

    static void execute(Block & block, const ColumnNumbers & arguments, const size_t result, const size_t n)
    {
        const auto & column = block.getByPosition(arguments[0]).column;

        if (const auto column_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto column_fixed = ColumnFixedString::create(n);

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_string->getChars();
            const auto & in_offsets = column_string->getOffsets();

            out_chars.resize_fill(in_offsets.size() * n);

            for (size_t i = 0; i < in_offsets.size(); ++i)
            {
                const size_t off = i ? in_offsets[i - 1] : 0;
                const size_t len = in_offsets[i] - off - 1;
                if (len > n)
                    throw Exception("String too long for type FixedString(" + toString(n) + ")",
                        ErrorCodes::TOO_LARGE_STRING_SIZE);
                memcpy(&out_chars[i * n], &in_chars[off], len);
            }

            block.getByPosition(result).column = std::move(column_fixed);
        }
        else if (const auto column_fixed_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const auto src_n = column_fixed_string->getN();
            if (src_n > n)
                throw Exception{
                    "String too long for type FixedString(" + toString(n) + ")",
                    ErrorCodes::TOO_LARGE_STRING_SIZE};

            auto column_fixed = ColumnFixedString::create(n);

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_fixed_string->getChars();
            const auto size = column_fixed_string->size();
            out_chars.resize_fill(size * n);

            for (const auto i : ext::range(0, size))
                memcpy(&out_chars[i * n], &in_chars[i * src_n], src_n);

            block.getByPosition(result).column = std::move(column_fixed);
        }
        else
            throw Exception("Unexpected column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
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

template <typename T>
struct ToIntMonotonicity
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
        size_t size_of_type = type.getSizeOfValueInMemory();

        /// If type is expanding, then function is monotonic.
        if (sizeof(T) > size_of_type)
            return { true, true, true };

        /// If type is same, too. (Enum has separate case, because it is different data type)
        if (checkDataType<DataTypeNumber<T>>(&type) ||
            checkDataType<DataTypeEnum<T>>(&type))
            return { true, true, true };

        /// In other cases, if range is unbounded, we don't know, whether function is monotonic or not.
        if (left.isNull() || right.isNull())
            return {};

        /// If converting from float, for monotonicity, arguments must fit in range of result type.
        if (checkDataType<DataTypeFloat32>(&type)
            || checkDataType<DataTypeFloat64>(&type))
        {
            Float64 left_float = left.get<Float64>();
            Float64 right_float = right.get<Float64>();

            if (left_float >= std::numeric_limits<T>::min() && left_float <= std::numeric_limits<T>::max()
                && right_float >= std::numeric_limits<T>::min() && right_float <= std::numeric_limits<T>::max())
                return { true };

            return {};
        }

        /// If signedness of type is changing, or converting from Date, DateTime, then arguments must be from same half,
        ///  and after conversion, resulting values must be from same half.
        /// Just in case, it is required in rest of cases too.
        if ((left.get<Int64>() >= 0) != (right.get<Int64>() >= 0)
            || (T(left.get<Int64>()) >= 0) != (T(right.get<Int64>()) >= 0))
            return {};

        /// If type is shrinked, then for monotonicity, all bits other than that fits, must be same.
        if (divideByRangeOfType(left.get<UInt64>()) != divideByRangeOfType(right.get<UInt64>()))
            return {};

        return { true };
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

        /// `toString` function is monotonous if the argument is Date or DateTime, or non-negative numbers with the same number of symbols.

        if (checkAndGetDataType<DataTypeDate>(&type)
            || typeid_cast<const DataTypeDateTime *>(&type))
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
struct NameToInt8 { static constexpr auto name = "toInt8"; };
struct NameToInt16 { static constexpr auto name = "toInt16"; };
struct NameToInt32 { static constexpr auto name = "toInt32"; };
struct NameToInt64 { static constexpr auto name = "toInt64"; };
struct NameToFloat32 { static constexpr auto name = "toFloat32"; };
struct NameToFloat64 { static constexpr auto name = "toFloat64"; };
struct NameToUUID { static constexpr auto name = "toUUID"; };

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8, ToIntMonotonicity<UInt8>>;
using FunctionToUInt16 = FunctionConvert<DataTypeUInt16, NameToUInt16, ToIntMonotonicity<UInt16>>;
using FunctionToUInt32 = FunctionConvert<DataTypeUInt32, NameToUInt32, ToIntMonotonicity<UInt32>>;
using FunctionToUInt64 = FunctionConvert<DataTypeUInt64, NameToUInt64, ToIntMonotonicity<UInt64>>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8, NameToInt8, ToIntMonotonicity<Int8>>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16, NameToInt16, ToIntMonotonicity<Int16>>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32, NameToInt32, ToIntMonotonicity<Int32>>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64, NameToInt64, ToIntMonotonicity<Int64>>;
using FunctionToFloat32 = FunctionConvert<DataTypeFloat32, NameToFloat32, PositiveMonotonicity>;
using FunctionToFloat64 = FunctionConvert<DataTypeFloat64, NameToFloat64, PositiveMonotonicity>;
using FunctionToDate = FunctionConvert<DataTypeDate, NameToDate, ToIntMonotonicity<UInt16>>;
using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime, ToIntMonotonicity<UInt32>>;
using FunctionToUUID = FunctionConvert<DataTypeUUID, NameToUUID, ToIntMonotonicity<UInt128>>;
using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToUnixTimestamp = FunctionConvert<DataTypeUInt32, NameToUnixTimestamp, ToIntMonotonicity<UInt32>>;


template <typename DataType> struct FunctionTo;

template <> struct FunctionTo<DataTypeUInt8> { using Type = FunctionToUInt8; };
template <> struct FunctionTo<DataTypeUInt16> { using Type = FunctionToUInt16; };
template <> struct FunctionTo<DataTypeUInt32> { using Type = FunctionToUInt32; };
template <> struct FunctionTo<DataTypeUInt64> { using Type = FunctionToUInt64; };
template <> struct FunctionTo<DataTypeInt8> { using Type = FunctionToInt8; };
template <> struct FunctionTo<DataTypeInt16> { using Type = FunctionToInt16; };
template <> struct FunctionTo<DataTypeInt32> { using Type = FunctionToInt32; };
template <> struct FunctionTo<DataTypeInt64> { using Type = FunctionToInt64; };
template <> struct FunctionTo<DataTypeFloat32> { using Type = FunctionToFloat32; };
template <> struct FunctionTo<DataTypeFloat64> { using Type = FunctionToFloat64; };
template <> struct FunctionTo<DataTypeDate> { using Type = FunctionToDate; };
template <> struct FunctionTo<DataTypeDateTime> { using Type = FunctionToDateTime; };
template <> struct FunctionTo<DataTypeUUID> { using Type = FunctionToUUID; };
template <> struct FunctionTo<DataTypeString> { using Type = FunctionToString; };
template <> struct FunctionTo<DataTypeFixedString> { using Type = FunctionToFixedString; };

template <typename FieldType> struct FunctionTo<DataTypeEnum<FieldType>>
    : FunctionTo<DataTypeNumber<FieldType>>
{
};

struct NameToUInt8OrZero { static constexpr auto name = "toUInt8OrZero"; };
struct NameToUInt16OrZero { static constexpr auto name = "toUInt16OrZero"; };
struct NameToUInt32OrZero { static constexpr auto name = "toUInt32OrZero"; };
struct NameToUInt64OrZero { static constexpr auto name = "toUInt64OrZero"; };
struct NameToInt8OrZero { static constexpr auto name = "toInt8OrZero"; };
struct NameToInt16OrZero { static constexpr auto name = "toInt16OrZero"; };
struct NameToInt32OrZero { static constexpr auto name = "toInt32OrZero"; };
struct NameToInt64OrZero { static constexpr auto name = "toInt64OrZero"; };
struct NameToFloat32OrZero { static constexpr auto name = "toFloat32OrZero"; };
struct NameToFloat64OrZero { static constexpr auto name = "toFloat64OrZero"; };

using FunctionToUInt8OrZero = FunctionConvertOrZero<DataTypeUInt8, NameToUInt8OrZero>;
using FunctionToUInt16OrZero = FunctionConvertOrZero<DataTypeUInt16, NameToUInt16OrZero>;
using FunctionToUInt32OrZero = FunctionConvertOrZero<DataTypeUInt32, NameToUInt32OrZero>;
using FunctionToUInt64OrZero = FunctionConvertOrZero<DataTypeUInt64, NameToUInt64OrZero>;
using FunctionToInt8OrZero = FunctionConvertOrZero<DataTypeInt8, NameToInt8OrZero>;
using FunctionToInt16OrZero = FunctionConvertOrZero<DataTypeInt16, NameToInt16OrZero>;
using FunctionToInt32OrZero = FunctionConvertOrZero<DataTypeInt32, NameToInt32OrZero>;
using FunctionToInt64OrZero = FunctionConvertOrZero<DataTypeInt64, NameToInt64OrZero>;
using FunctionToFloat32OrZero = FunctionConvertOrZero<DataTypeFloat32, NameToFloat32OrZero>;
using FunctionToFloat64OrZero = FunctionConvertOrZero<DataTypeFloat64, NameToFloat64OrZero>;

struct NameToUInt8OrNull { static constexpr auto name = "toUInt8OrNull"; };
struct NameToUInt16OrNull { static constexpr auto name = "toUInt16OrNull"; };
struct NameToUInt32OrNull { static constexpr auto name = "toUInt32OrNull"; };
struct NameToUInt64OrNull { static constexpr auto name = "toUInt64OrNull"; };
struct NameToInt8OrNull { static constexpr auto name = "toInt8OrNull"; };
struct NameToInt16OrNull { static constexpr auto name = "toInt16OrNull"; };
struct NameToInt32OrNull { static constexpr auto name = "toInt32OrNull"; };
struct NameToInt64OrNull { static constexpr auto name = "toInt64OrNull"; };
struct NameToFloat32OrNull { static constexpr auto name = "toFloat32OrNull"; };
struct NameToFloat64OrNull { static constexpr auto name = "toFloat64OrNull"; };

using FunctionToUInt8OrNull = FunctionConvertOrNull<DataTypeUInt8, NameToUInt8OrNull>;
using FunctionToUInt16OrNull = FunctionConvertOrNull<DataTypeUInt16, NameToUInt16OrNull>;
using FunctionToUInt32OrNull = FunctionConvertOrNull<DataTypeUInt32, NameToUInt32OrNull>;
using FunctionToUInt64OrNull = FunctionConvertOrNull<DataTypeUInt64, NameToUInt64OrNull>;
using FunctionToInt8OrNull = FunctionConvertOrNull<DataTypeInt8, NameToInt8OrNull>;
using FunctionToInt16OrNull = FunctionConvertOrNull<DataTypeInt16, NameToInt16OrNull>;
using FunctionToInt32OrNull = FunctionConvertOrNull<DataTypeInt32, NameToInt32OrNull>;
using FunctionToInt64OrNull = FunctionConvertOrNull<DataTypeInt64, NameToInt64OrNull>;
using FunctionToFloat32OrNull = FunctionConvertOrNull<DataTypeFloat32, NameToFloat32OrNull>;
using FunctionToFloat64OrNull = FunctionConvertOrNull<DataTypeFloat64, NameToFloat64OrNull>;


class PreparedFunctionCast : public PreparedFunctionImpl
{
public:
    using WrapperType = std::function<void(Block &, const ColumnNumbers &, size_t)>;

    explicit PreparedFunctionCast(WrapperType && wrapper_function, const char * name)
            : wrapper_function(std::move(wrapper_function)), name(name) {}

    String getName() const override { return name; }

protected:
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// drop second argument, pass others
        ColumnNumbers new_arguments{arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2), std::end(arguments));

        wrapper_function(block, new_arguments, result);
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char * name;
};

class FunctionCast final : public IFunctionBase
{
public:
    using WrapperType = std::function<void(Block &, const ColumnNumbers &, size_t)>;
    using MonotonicityForRange = std::function<Monotonicity(const IDataType &, const Field &, const Field &)>;

    FunctionCast(const Context & context, const char * name, MonotonicityForRange && monotonicity_for_range
            , const DataTypes & argument_types, const DataTypePtr & return_type)
            : context(context), name(name), monotonicity_for_range(monotonicity_for_range)
            , argument_types(argument_types), return_type(return_type)
    {
    }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    PreparedFunctionPtr prepare(const Block & /*sample_block*/) const override
    {
        return std::make_shared<PreparedFunctionCast>(prepare(getArgumentTypes()[0], getReturnType().get()), name);
    }

    String getName() const override { return name; }

    bool hasInformationAboutMonotonicity() const override
    {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return monotonicity_for_range(type, left, right);
    }

private:

    const Context & context;
    const char * name;
    MonotonicityForRange monotonicity_for_range;

    DataTypes argument_types;
    DataTypePtr return_type;

    template <typename DataType>
    WrapperType createWrapper(const DataTypePtr & from_type, const DataType * const) const
    {
        using FunctionType = typename FunctionTo<DataType>::Type;

        auto function = FunctionType::create(context);

        /// Check conversion using underlying function
        {
            function->getReturnType(ColumnsWithTypeAndName(1, { nullptr, from_type, "" }));
        }

        return [function] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            function->execute(block, arguments, result);
        };
    }

    static WrapperType createFixedStringWrapper(const DataTypePtr & from_type, const size_t N)
    {
        if (!from_type->isStringOrFixedString())
            throw Exception{
                "CAST AS FixedString is only implemented for types String and FixedString",
                ErrorCodes::NOT_IMPLEMENTED};

        return [N] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            FunctionToFixedString::execute(block, arguments, result, N);
        };
    }

    WrapperType createArrayWrapper(const DataTypePtr & from_type_untyped, const DataTypeArray * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                ConvertImplGenericFromString::execute(block, arguments, result);
            };
        }

        DataTypePtr from_nested_type;
        DataTypePtr to_nested_type;
        auto from_type = checkAndGetDataType<DataTypeArray>(from_type_untyped.get());

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
            throw Exception{
                "CAST AS Array can only be performed between same-dimensional array types or from String",
                ErrorCodes::TYPE_MISMATCH};

        /// Prepare nested type conversion
        const auto nested_function = prepare(from_nested_type, to_nested_type.get());

        return [nested_function, from_nested_type, to_nested_type](
            Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            const auto & array_arg = block.getByPosition(arguments.front());

            if (const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(array_arg.column.get()))
            {
                /// create block for converting nested column containing original and result columns
                Block nested_block
                {
                    { col_array->getDataPtr(), from_nested_type, "" },
                    { nullptr, to_nested_type, "" }
                };

                /// convert nested column
                nested_function(nested_block, {0}, 1);

                /// set converted nested column to result
                block.getByPosition(result).column = ColumnArray::create(nested_block.getByPosition(1).column, col_array->getOffsetsPtr());
            }
            else
                throw Exception{
                    "Illegal column " + array_arg.column->getName() + " for function CAST AS Array",
                    ErrorCodes::LOGICAL_ERROR};
        };
    }

    WrapperType createTupleWrapper(const DataTypePtr & from_type_untyped, const DataTypeTuple * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                ConvertImplGenericFromString::execute(block, arguments, result);
            };
        }

        const auto from_type = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get());
        if (!from_type)
            throw Exception{
                "CAST AS Tuple can only be performed between tuple types or from String.\nLeft type: " + from_type_untyped->getName() +
                    ", right type: " + to_type->getName(),
                ErrorCodes::TYPE_MISMATCH};

        if (from_type->getElements().size() != to_type->getElements().size())
            throw Exception{
                "CAST AS Tuple can only be performed between tuple types with the same number of elements or from String.\n"
                    "Left type: " + from_type->getName() + ", right type: " + to_type->getName(),
                 ErrorCodes::TYPE_MISMATCH};

        const auto & from_element_types = from_type->getElements();
        const auto & to_element_types = to_type->getElements();
        std::vector<WrapperType> element_wrappers;
        element_wrappers.reserve(from_element_types.size());

        /// Create conversion wrapper for each element in tuple
        for (const auto & idx_type : ext::enumerate(from_type->getElements()))
            element_wrappers.push_back(prepare(idx_type.second, to_element_types[idx_type.first].get()));

        return [element_wrappers, from_element_types, to_element_types]
            (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            const auto col = block.getByPosition(arguments.front()).column.get();

            /// copy tuple elements to a separate block
            Block element_block;

            size_t tuple_size = from_element_types.size();
            const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*col);

            /// create columns for source elements
            for (size_t i = 0; i < tuple_size; ++i)
                element_block.insert({ column_tuple.getColumns()[i], from_element_types[i], "" });

            /// create columns for converted elements
            for (const auto & to_element_type : to_element_types)
                element_block.insert({ nullptr, to_element_type, "" });

            /// insert column for converted tuple
            element_block.insert({ nullptr, std::make_shared<DataTypeTuple>(to_element_types), "" });

            /// invoke conversion for each element
            for (const auto & idx_element_wrapper : ext::enumerate(element_wrappers))
                idx_element_wrapper.second(element_block, { idx_element_wrapper.first },
                    tuple_size + idx_element_wrapper.first);

            Columns converted_columns(tuple_size);
            for (size_t i = 0; i < tuple_size; ++i)
                converted_columns[i] = element_block.getByPosition(tuple_size + i).column;

            block.getByPosition(result).column = ColumnTuple::create(converted_columns);
        };
    }

    template <typename FieldType>
    WrapperType createEnumWrapper(const DataTypePtr & from_type, const DataTypeEnum<FieldType> * to_type) const
    {
        using EnumType = DataTypeEnum<FieldType>;
        using Function = typename FunctionTo<EnumType>::Type;

        if (const auto from_enum8 = checkAndGetDataType<DataTypeEnum8>(from_type.get()))
            checkEnumToEnumConversion(from_enum8, to_type);
        else if (const auto from_enum16 = checkAndGetDataType<DataTypeEnum16>(from_type.get()))
            checkEnumToEnumConversion(from_enum16, to_type);

        if (checkAndGetDataType<DataTypeString>(from_type.get()))
            return createStringToEnumWrapper<ColumnString, EnumType>();
        else if (checkAndGetDataType<DataTypeFixedString>(from_type.get()))
            return createStringToEnumWrapper<ColumnFixedString, EnumType>();
        else if (from_type->isNumber() || from_type->isEnum())
        {
            auto function = Function::create(context);

            /// Check conversion using underlying function
            {
                function->getReturnType(ColumnsWithTypeAndName(1, { nullptr, from_type, "" }));
            }

            return [function] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                function->execute(block, arguments, result);
            };
        }
        else
            throw Exception{
                "Conversion from " + from_type->getName() + " to " + to_type->getName() +
                    " is not supported",
                ErrorCodes::CANNOT_CONVERT_TYPE};
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
                throw Exception{
                    "Enum conversion changes value for element '" + name_value.first +
                        "' from " + toString(old_value) + " to " + toString(new_value),
                    ErrorCodes::CANNOT_CONVERT_TYPE};
        }
    };

    template <typename ColumnStringType, typename EnumType>
    WrapperType createStringToEnumWrapper() const
    {
        const char * function_name = name;
        return [function_name] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            const auto first_col = block.getByPosition(arguments.front()).column.get();

            auto & col_with_type_and_name = block.getByPosition(result);
            const auto & result_type = typeid_cast<const EnumType &>(*col_with_type_and_name.type);

            if (const auto col = typeid_cast<const ColumnStringType *>(first_col))
            {
                const auto size = col->size();

                auto res = result_type.createColumn();
                auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
                out_data.resize(size);

                for (const auto i : ext::range(0, size))
                    out_data[i] = result_type.getValue(col->getDataAt(i));

                col_with_type_and_name.column = std::move(res);
            }
            else
                throw Exception{
                    "Unexpected column " + first_col->getName() + " as first argument of function " + function_name,
                    ErrorCodes::LOGICAL_ERROR};
        };
    }

    WrapperType createIdentityWrapper(const DataTypePtr &) const
    {
        return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
        };
    }

    WrapperType createNothingWrapper(const IDataType * to_type) const
    {
        ColumnPtr res = to_type->createColumnConstWithDefaultValue(1);
        return [res] (Block & block, const ColumnNumbers &, const size_t result)
        {
            /// Column of Nothing type is trivially convertible to any other column
            block.getByPosition(result).column = res->cloneResized(block.rows())->convertToFullColumnIfConst();
        };
    }

    /// Actions to be taken when performing a conversion.
    struct NullableConversion
    {
        bool source_is_nullable = false;
        bool result_is_nullable = false;
    };

    WrapperType prepare(const DataTypePtr & from_type, const IDataType * to_type) const
    {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.

        NullableConversion nullable_conversion;
        nullable_conversion.source_is_nullable = from_type->isNullable();
        nullable_conversion.result_is_nullable = to_type->isNullable();

        /// Check that the requested conversion is allowed.
        if (nullable_conversion.source_is_nullable && !nullable_conversion.result_is_nullable)
            throw Exception{"Cannot convert data from a nullable type to a non-nullable type",
                ErrorCodes::CANNOT_CONVERT_TYPE};

        if (from_type->onlyNull())
        {
            return [](Block & block, const ColumnNumbers &, const size_t result)
            {
                auto & res = block.getByPosition(result);
                res.column = res.type->createColumnConstWithDefaultValue(block.rows())->convertToFullColumnIfConst();
            };
        }

        DataTypePtr from_inner_type;
        const IDataType * to_inner_type;

        /// Create the requested conversion.
        if (nullable_conversion.result_is_nullable)
        {
            if (nullable_conversion.source_is_nullable)
            {
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*from_type);
                from_inner_type = nullable_type.getNestedType();
            }
            else
                from_inner_type = from_type;

            const auto & nullable_type = static_cast<const DataTypeNullable &>(*to_type);
            to_inner_type = nullable_type.getNestedType().get();
        }
        else
        {
            from_inner_type = from_type;
            to_inner_type = to_type;
        }

        auto wrapper = prepareImpl(from_inner_type, to_inner_type);

        if (nullable_conversion.result_is_nullable)
        {
            return [wrapper, nullable_conversion] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                /// Create a temporary block on which to perform the operation.
                auto & res = block.getByPosition(result);
                const auto & ret_type = res.type;
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*ret_type);
                const auto & nested_type = nullable_type.getNestedType();

                Block tmp_block;
                if (nullable_conversion.source_is_nullable)
                    tmp_block = createBlockWithNestedColumns(block, arguments);
                else
                    tmp_block = block;

                size_t tmp_res_index = block.columns();
                tmp_block.insert({nullptr, nested_type, ""});

                /// Perform the requested conversion.
                wrapper(tmp_block, arguments, tmp_res_index);

                /// Wrap the result into a nullable column.
                ColumnPtr null_map;

                if (nullable_conversion.source_is_nullable)
                {
                    /// This is a conversion from a nullable to a nullable type.
                    /// So we just keep the null map of the input argument.
                    const auto & col = block.getByPosition(arguments[0]).column;
                    const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
                    null_map = nullable_col.getNullMapColumnPtr();
                }
                else
                {
                    /// This is a conversion from an ordinary type to a nullable type.
                    /// So we create a trivial null map.
                    null_map = ColumnUInt8::create(block.rows(), 0);
                }

                const auto & tmp_res = tmp_block.getByPosition(tmp_res_index);
                res.column = ColumnNullable::create(tmp_res.column, null_map);
            };
        }
        else
            return wrapper;
    }

    WrapperType prepareImpl(const DataTypePtr & from_type, const IDataType * to_type) const
    {
        if (from_type->equals(*to_type))
            return createIdentityWrapper(from_type);
        else if (checkDataType<DataTypeNothing>(from_type.get()))
            return createNothingWrapper(to_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeUInt8>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeUInt16>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeUInt32>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeUInt64>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeInt8>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeInt16>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeInt32>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeInt64>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeFloat32>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeFloat64>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeDate>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeDateTime>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = checkAndGetDataType<DataTypeString>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto type_fixed_string = checkAndGetDataType<DataTypeFixedString>(to_type))
            return createFixedStringWrapper(from_type, type_fixed_string->getN());
        else if (const auto type_array = checkAndGetDataType<DataTypeArray>(to_type))
            return createArrayWrapper(from_type, type_array);
        else if (const auto type_tuple = checkAndGetDataType<DataTypeTuple>(to_type))
            return createTupleWrapper(from_type, type_tuple);
        else if (const auto type_enum = checkAndGetDataType<DataTypeEnum8>(to_type))
            return createEnumWrapper(from_type, type_enum);
        else if (const auto type_enum = checkAndGetDataType<DataTypeEnum16>(to_type))
            return createEnumWrapper(from_type, type_enum);

        /// It's possible to use ConvertImplGenericFromString to convert from String to AggregateFunction,
        ///  but it is disabled because deserializing aggregate functions state might be unsafe.

        throw Exception{
            "Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported",
            ErrorCodes::CANNOT_CONVERT_TYPE};
    }
};

class FunctionBuilderCast : public FunctionBuilderImpl
{
public:
    using MonotonicityForRange = FunctionCast::MonotonicityForRange;

    static constexpr auto name = "CAST";
    static FunctionBuilderPtr create(const Context & context) { return std::make_shared<FunctionBuilderCast>(context); }

    FunctionBuilderCast(const Context & context) : context(context) {}

    String getName() const { return name; }

    size_t getNumberOfArguments() const override { return 2; }


protected:

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        auto monotonicity = getMonotonicityInformation(arguments.front().type, return_type.get());
        return std::make_shared<FunctionCast>(context, name, std::move(monotonicity), data_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto type_col = checkAndGetColumnConst<ColumnString>(arguments.back().column.get());
        if (!type_col)
            throw Exception("Second argument to " + getName() + " must be a constant string describing type",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypeFactory::instance().get(type_col->getValue<String>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }

private:
    template <typename DataType>
    static auto monotonicityForType(const DataType * const)
    {
        return FunctionTo<DataType>::Type::Monotonic::get;
    }

    MonotonicityForRange getMonotonicityInformation(const DataTypePtr & from_type, const IDataType * to_type) const
    {
        if (const auto type = checkAndGetDataType<DataTypeUInt8>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeUInt16>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeUInt32>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeUInt64>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt8>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt16>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt32>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeInt64>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeFloat32>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeFloat64>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeDate>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeDateTime>(to_type))
            return monotonicityForType(type);
        else if (const auto type = checkAndGetDataType<DataTypeString>(to_type))
            return monotonicityForType(type);
        else if (from_type->isEnum())
        {
            if (const auto type = checkAndGetDataType<DataTypeEnum8>(to_type))
                return monotonicityForType(type);
            else if (const auto type = checkAndGetDataType<DataTypeEnum16>(to_type))
                return monotonicityForType(type);
        }
        /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
        return {};
    }

    const Context & context;
};

}
