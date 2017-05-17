#pragma once

#include <ext/enumerate.hpp>
#include <ext/collection_cast.hpp>
#include <ext/range.hpp>
#include <type_traits>

#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromMemory.h>
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
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Core/FieldVisitors.h>
#include <Interpreters/ExpressionActions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionsDateTime.h>


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
    extern const int TOO_LARGE_STRING_SIZE;
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
            = typeid_cast<const ColumnVector<FromFieldType> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_to = std::make_shared<ColumnVector<ToFieldType>>();
            block.safeGetByPosition(result).column = col_to;

            const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
            typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = vec_from[i];
        }
        else if (const ColumnConst<FromFieldType> * col_from
            = typeid_cast<const ColumnConst<FromFieldType> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<ToFieldType>>(col_from->size(), col_from->getData());
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** Conversion of Date to DateTime: adding 00:00:00 time component.
  */
struct ToDateTimeImpl
{
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum_t(d));
    }
};

template <typename Name> struct ConvertImpl<DataTypeDate, DataTypeDateTime, Name>
    : DateTimeTransformImpl<UInt16, UInt32, ToDateTimeImpl, Name> {};


/// Implementation of toDate function.

template <typename FromType, typename ToType>
struct ToDateTransform32Or64
{
    static inline ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        return (from < 0xFFFF) ? from : time_zone.toDayNum(from);
    }
};

/** Conversion of DateTime to Date: throw off time component.
  */
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDate, Name>
    : DateTimeTransformImpl<UInt32, UInt16, ToDateImpl, Name> {};

/** Special case of converting (U)Int32 or (U)Int64 (and also, for convenience, Float32, Float64) to Date.
  * If number is less than 65536, then it is treated as DayNum, and if greater or equals, then as unix timestamp.
  * It's a bit illogical, as we actually have two functions in one.
  * But allows to support frequent case,
  *  when user write toDate(UInt32), expecting conversion of unix timestamp to Date.
  *  (otherwise such usage would be frequent mistake).
  */
template <typename Name> struct ConvertImpl<DataTypeUInt32, DataTypeDate, Name>
    : DateTimeTransformImpl<UInt32, UInt16, ToDateTransform32Or64<UInt32, UInt16>, Name> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDate, Name>
    : DateTimeTransformImpl<UInt64, UInt16, ToDateTransform32Or64<UInt64, UInt16>, Name> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDate, Name>
    : DateTimeTransformImpl<Int32, UInt16, ToDateTransform32Or64<Int32, UInt16>, Name> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDate, Name>
    : DateTimeTransformImpl<Int64, UInt16, ToDateTransform32Or64<Int64, UInt16>, Name> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDate, Name>
    : DateTimeTransformImpl<Float32, UInt16, ToDateTransform32Or64<Float32, UInt16>, Name> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDate, Name>
    : DateTimeTransformImpl<Float64, UInt16, ToDateTransform32Or64<Float64, UInt16>, Name> {};


/** Transformation of numbers, dates, datetimes to strings: through formatting.
  */
template <typename DataType>
struct FormatImpl
{
    static void execute(const typename DataType::FieldType x, WriteBuffer & wb, const DataType * type, const DateLUTImpl * time_zone)
    {
        writeText(x, wb);
    }
};

template <>
struct FormatImpl<DataTypeDate>
{
    static void execute(const DataTypeDate::FieldType x, WriteBuffer & wb, const DataTypeDate * type, const DateLUTImpl * time_zone)
    {
        writeDateText(DayNum_t(x), wb);
    }
};

template <>
struct FormatImpl<DataTypeDateTime>
{
    static void execute(const DataTypeDateTime::FieldType x, WriteBuffer & wb, const DataTypeDateTime * type, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(x, wb, *time_zone);
    }
};

template <typename FieldType>
struct FormatImpl<DataTypeEnum<FieldType>>
{
    static void execute(const FieldType x, WriteBuffer & wb, const DataTypeEnum<FieldType> * type, const DateLUTImpl * time_zone)
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
        block.safeGetByPosition(result).column = block.safeGetByPosition(arguments[0]).column;
    }
};


/// For functions toDateTime, toUnixTimestamp and toString from DateTime type, second argument with time zone could be specified.
const DateLUTImpl * extractTimeZoneFromFunctionArguments(Block & block, const ColumnNumbers & arguments);


template <typename FromDataType, typename Name>
struct ConvertImpl<FromDataType, DataTypeString, Name>
{
    using FromFieldType = typename FromDataType::FieldType;

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto & col_with_type_and_name = block.safeGetByPosition(arguments[0]);
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        const DateLUTImpl * time_zone = nullptr;

        /// For argument of DateTime type, second argument with time zone could be specified.
        if (std::is_same<FromDataType, DataTypeDateTime>::value)
            time_zone = extractTimeZoneFromFunctionArguments(block, arguments);

        if (const auto col_from = typeid_cast<const ColumnVector<FromFieldType> *>(col_with_type_and_name.column.get()))
        {
            auto col_to = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = col_to;

            const typename ColumnVector<FromFieldType>::Container_t & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                FormatImpl<FromDataType>::execute(vec_from[i], write_buffer, &type, time_zone);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            data_to.resize(write_buffer.count());
        }
        else if (const auto col_from = typeid_cast<const ColumnConst<FromFieldType> *>(col_with_type_and_name.column.get()))
        {
            std::vector<char> buf;
            WriteBufferFromVector<std::vector<char> > write_buffer(buf);
            FormatImpl<FromDataType>::execute(col_from->getData(), write_buffer, &type, time_zone);
            block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(col_from->size(), std::string(&buf[0], write_buffer.count()));
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Generic conversion of any type to String.
struct ConvertImplGenericToString
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const auto & col_with_type_and_name = block.safeGetByPosition(arguments[0]);
        const IDataType & type = *col_with_type_and_name.type;
        const IColumn & col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();

        if (!col_from.isConst())
        {
            auto col_to = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = col_to;

            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets_t & offsets_to = col_to->getOffsets();

            data_to.resize(size * 2);    /// Using coefficient 2 for initial size is arbitary.
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> write_buffer(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                type.serializeText(col_from, i, write_buffer);
                writeChar(0, write_buffer);
                offsets_to[i] = write_buffer.count();
            }

            data_to.resize(write_buffer.count());
        }
        else
        {
            String res;

            if (size)
            {
                WriteBufferFromString write_buffer(res);
                type.serializeText(*col_from.cut(0, 1)->convertToFullColumnIfConst(), 0, write_buffer);
            }

            block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(size, res);
        }
    }
};


/** Conversion of strings to numbers, dates, datetimes: through parsing.
  */
template <typename DataType> void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
{
    readText(x, rb);
}

template <> inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
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


/** Throw exception with verbose message when string value is not parsed completely.
  */
void throwExceptionForIncompletelyParsedValue(
    ReadBuffer & read_buffer, Block & block, const ColumnNumbers & arguments, size_t result);


template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeString, ToDataType, Name>
{
    using ToFieldType = typename ToDataType::FieldType;

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const DateLUTImpl * time_zone = nullptr;

        /// For conversion to DateTime type, second argument with time zone could be specified.
        if (std::is_same<ToDataType, DataTypeDateTime>::value)
            time_zone = extractTimeZoneFromFunctionArguments(block, arguments);

        if (const ColumnString * col_from = typeid_cast<const ColumnString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_to = std::make_shared<ColumnVector<ToFieldType>>();
            block.safeGetByPosition(result).column = col_to;

            typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
            size_t size = col_from->size();
            vec_to.resize(size);

            const ColumnString::Chars_t & chars = col_from->getChars();
            const IColumn::Offsets_t & offsets = col_from->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory read_buffer(&chars[current_offset], offsets[i] - current_offset - 1);

                parseImpl<ToDataType>(vec_to[i], read_buffer, time_zone);

                if (!read_buffer.eof()
                    && !(std::is_same<ToDataType, DataTypeDate>::value /// Special exception, that allows to parse string with DateTime as Date.
                        && offsets[i] - current_offset - 1 == strlen("YYYY-MM-DD hh:mm:ss")))
                    throwExceptionForIncompletelyParsedValue(read_buffer, block, arguments, result);

                current_offset = offsets[i];
            }
        }
        else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            const String & s = col_from->getData();
            ReadBufferFromString read_buffer(s);
            ToFieldType x = 0;
            parseImpl<ToDataType>(x, read_buffer, time_zone);

            if (!read_buffer.eof()
                && !(std::is_same<ToDataType, DataTypeDate>::value /// Special exception, that allows to parse string with DateTime as Date.
                    && s.size() == strlen("YYYY-MM-DD hh:mm:ss")))
                throwExceptionForIncompletelyParsedValue(read_buffer, block, arguments, result);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<ToFieldType>>(col_from->size(), x);
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename DataType>
typename std::enable_if<std::is_integral<typename DataType::FieldType>::value, bool>::type
tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb)
{
    return tryReadIntText(x, rb);
}

template <typename DataType>
typename std::enable_if<std::is_floating_point<typename DataType::FieldType>::value, bool>::type
tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb)
{
    return tryReadFloatText(x, rb);
}


/** Conversion from String through parsing, which returns default value instead of throwing an exception.
  */
template <typename ToDataType, typename Name>
struct ConvertOrZeroImpl
{
    using ToFieldType = typename ToDataType::FieldType;

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnString * col_from = typeid_cast<const ColumnString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_to = std::make_shared<ColumnVector<ToFieldType>>();
            block.safeGetByPosition(result).column = col_to;

            typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
            size_t size = col_from->size();
            vec_to.resize(size);

            const ColumnString::Chars_t & chars = col_from->getChars();
            const IColumn::Offsets_t & offsets = col_from->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory read_buffer(&chars[current_offset], offsets[i] - current_offset - 1);

                /// NOTE Need to implement for Date and DateTime too.
                if (!tryParseImpl<ToDataType>(vec_to[i], read_buffer) || !read_buffer.eof())
                    vec_to[i] = 0;

                current_offset = offsets[i];
            }
        }
        else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            const String & s = col_from->getData();
            ReadBufferFromString read_buffer(s);
            ToFieldType x = 0;
            if (!tryParseImpl<ToDataType>(x, read_buffer) || !read_buffer.eof())
                x = 0;
            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<ToFieldType>>(col_from->size(), x);
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Generic conversion of any type from String. Used for complex types: Array and Tuple.
struct ConvertImplGenericFromString
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        const IColumn & col_from = *block.safeGetByPosition(arguments[0]).column;
        size_t size = col_from.size();

        ColumnWithTypeAndName & column_type_name_to = block.safeGetByPosition(result);
        const IDataType & data_type_to = *column_type_name_to.type;

        if (const ColumnString * col_from_string = typeid_cast<const ColumnString *>(&col_from))
        {
            column_type_name_to.column = data_type_to.createColumn();

            if (!size)
                return;

            IColumn & column_to = *column_type_name_to.column;
            column_to.reserve(size);

            const ColumnString::Chars_t & chars = col_from_string->getChars();
            const IColumn::Offsets_t & offsets = col_from_string->getOffsets();

            size_t current_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory read_buffer(&chars[current_offset], offsets[i] - current_offset - 1);

                data_type_to.deserializeTextEscaped(column_to, read_buffer);

                if (!read_buffer.eof())
                    throwExceptionForIncompletelyParsedValue(read_buffer, block, arguments, result);

                current_offset = offsets[i];
            }
        }
        else if (const ColumnConstString * col_from_const_string = typeid_cast<const ColumnConstString *>(&col_from))
        {
            const String & s = col_from_const_string->getData();
            ReadBufferFromString read_buffer(s);

            auto tmp_col = data_type_to.createColumn();
            data_type_to.deserializeTextEscaped(*tmp_col, read_buffer);

            if (!read_buffer.eof())
                throwExceptionForIncompletelyParsedValue(read_buffer, block, arguments, result);

            block.safeGetByPosition(result).column = data_type_to.createConstColumn(size, (*tmp_col)[0]);
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of first argument of conversion function from string",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Function toUnixTimestamp has exactly the same implementation as toDateTime of String type.
/// Note that time zone argument could be passed only for toUnixTimestamp function.
struct NameToUnixTimestamp    { static constexpr auto name = "toUnixTimestamp"; };

template <>
struct ConvertImpl<DataTypeString, DataTypeUInt32, NameToUnixTimestamp>
    : ConvertImpl<DataTypeString, DataTypeDateTime, NameToUnixTimestamp> {};


/** If types are identical, just take reference to column.
  */
template <typename Name>
struct ConvertImpl<DataTypeString, DataTypeString, Name>
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        block.safeGetByPosition(result).column = block.safeGetByPosition(arguments[0]).column;
    }
};


/** Conversion from FixedString through parsing.
  */
template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeFixedString, ToDataType, Name>
{
    using ToFieldType = typename ToDataType::FieldType;

    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            const DateLUTImpl * time_zone = nullptr;

            /// For conversion to DateTime type, second argument with time zone could be specified.
            if (std::is_same<ToDataType, DataTypeDateTime>::value)
                time_zone = extractTimeZoneFromFunctionArguments(block, arguments);

            auto col_to = std::make_shared<ColumnVector<ToFieldType>>();
            block.safeGetByPosition(result).column = col_to;

            const ColumnFixedString::Chars_t & data_from = col_from->getChars();
            size_t n = col_from->getN();
            typename ColumnVector<ToFieldType>::Container_t & vec_to = col_to->getData();
            size_t size = col_from->size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
            {
                ReadBufferFromMemory read_buffer(&data_from[i * n], n);
                const char * end = read_buffer.buffer().end();
                parseImpl<ToDataType>(vec_to[i], read_buffer, time_zone);

                if (!read_buffer.eof())
                {
                    while (read_buffer.position() < end && *read_buffer.position() == 0)
                        ++read_buffer.position();

                    if (read_buffer.position() < end)
                        throwExceptionForIncompletelyParsedValue(read_buffer, block, arguments, result);
                }
            }
        }
        else if (typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            ConvertImpl<DataTypeString, ToDataType, Name>::execute(block, arguments, result);
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
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
        if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            auto col_to = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = col_to;

            const ColumnFixedString::Chars_t & data_from = col_from->getChars();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
            size_t size = col_from->size();
            size_t n = col_from->getN();
            data_to.resize(size * (n + 1));        /// + 1 - zero terminator
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
        }
        else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            const String & s = col_from->getData();

            size_t bytes_to_copy = s.size();
            while (bytes_to_copy > 0 && s[bytes_to_copy - 1] == 0)
                --bytes_to_copy;

            block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(col_from->size(), s.substr(0, bytes_to_copy));
        }
        else
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Declared early because used below.
struct NameToDate            { static constexpr auto name = "toDate"; };
struct NameToString            { static constexpr auto name = "toString"; };


template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction
{
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionConvert>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const Block &) override { return std::is_same<Name, NameToString>::value; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return getReturnTypeInternal(arguments);
    }

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
                || e.code() == ErrorCodes::CANNOT_PARSE_DATETIME)
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
        IDataType * from_type = block.safeGetByPosition(arguments[0]).type.get();

        if      (typeid_cast<const DataTypeUInt8 *        >(from_type)) ConvertImpl<DataTypeUInt8,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeUInt16 *        >(from_type)) ConvertImpl<DataTypeUInt16,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeUInt32 *        >(from_type)) ConvertImpl<DataTypeUInt32,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeUInt64 *        >(from_type)) ConvertImpl<DataTypeUInt64,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeInt8 *        >(from_type)) ConvertImpl<DataTypeInt8,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeInt16 *        >(from_type)) ConvertImpl<DataTypeInt16,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeInt32 *        >(from_type)) ConvertImpl<DataTypeInt32,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeInt64 *        >(from_type)) ConvertImpl<DataTypeInt64,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeFloat32 *    >(from_type)) ConvertImpl<DataTypeFloat32,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeFloat64 *    >(from_type)) ConvertImpl<DataTypeFloat64,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeDate *        >(from_type)) ConvertImpl<DataTypeDate,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeDateTime *    >(from_type)) ConvertImpl<DataTypeDateTime,    ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeString *        >(from_type)) ConvertImpl<DataTypeString,     ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeFixedString *>(from_type)) ConvertImpl<DataTypeFixedString, ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeEnum8 *>(from_type))          ConvertImpl<DataTypeEnum8, ToDataType, Name>::execute(block, arguments, result);
        else if (typeid_cast<const DataTypeEnum16 *>(from_type))      ConvertImpl<DataTypeEnum16, ToDataType, Name>::execute(block, arguments, result);
        else
        {
            /// Generic conversion of any type to String.
            if (std::is_same<ToDataType, DataTypeString>::value)
            {
                ConvertImplGenericToString::execute(block, arguments, result);
            }
            else
                throw Exception("Illegal type " + block.safeGetByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    template<typename ToDataType2 = ToDataType, typename Name2 = Name>
    DataTypePtr getReturnTypeInternal(const DataTypes & arguments,
        typename std::enable_if<!(std::is_same<ToDataType2, DataTypeString>::value ||
            std::is_same<Name2, NameToUnixTimestamp>::value ||
            std::is_same<Name2, NameToDate>::value)>::type * = nullptr) const
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<ToDataType>();
    }

    /** Conversion of anything to String. For DateTime, it allows second optional argument - time zone.
      */
    template<typename ToDataType2 = ToDataType, typename Name2 = Name>
    DataTypePtr getReturnTypeInternal(const DataTypes & arguments,
        typename std::enable_if<std::is_same<ToDataType2, DataTypeString>::value>::type * = nullptr) const
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (typeid_cast<const DataTypeDateTime *>(arguments[0].get()) == nullptr)
        {
            if (arguments.size() != 1)
                throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                    + toString(arguments.size()) + ", should be 1.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        else if ((arguments.size() == 2) && (typeid_cast<const DataTypeString *>(arguments[1].get()) == nullptr))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            };
        }

        return std::make_shared<ToDataType2>();
    }

    template<typename ToDataType2 = ToDataType, typename Name2 = Name>
    DataTypePtr getReturnTypeInternal(const DataTypes & arguments,
        typename std::enable_if<std::is_same<Name2, NameToUnixTimestamp>::value, void>::type * = nullptr) const
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (typeid_cast<const DataTypeString *>(arguments[0].get()) == nullptr)
        {
            if (arguments.size() != 1)
                throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                    + toString(arguments.size()) + ", should be 1.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        else if ((arguments.size() == 2) && (typeid_cast<const DataTypeString *>(arguments[1].get()) == nullptr))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            };
        }

        return std::make_shared<ToDataType2>();
    }

    template<typename ToDataType2 = ToDataType, typename Name2 = Name>
    DataTypePtr getReturnTypeInternal(const DataTypes & arguments,
        typename std::enable_if<std::is_same<Name2, NameToDate>::value>::type * = nullptr) const
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if ((arguments.size() == 2) && (typeid_cast<const DataTypeString *>(arguments[1].get()) == nullptr))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of 2nd argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            };
        }

        return std::make_shared<ToDataType2>();
    }
};


/** Functions tryToT (where T is number of date or datetime type):
  *  try to convert from String to type T through parsing,
  *  if cannot parse, return default value instead of throwing exception.
  * NOTE Also need implement tryToUnixTimestamp with timezone.
  */
template <typename ToDataType, typename Name>
class FunctionConvertOrZero : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionConvertOrZero>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<ToDataType>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        IDataType * from_type = block.safeGetByPosition(arguments[0]).type.get();

        if (typeid_cast<const DataTypeString *>(from_type))
            ConvertOrZeroImpl<ToDataType, Name>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.safeGetByPosition(arguments[0]).type->getName() + " of argument of function " + getName()
                + ". Only String argument is accepted for try-conversion function. For other arguments, use function without 'try'.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


/** Conversion to fixed string is implemented only for strings.
  */
class FunctionToFixedString : public IFunction
{
public:
    static constexpr auto name = "toFixedString";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionToFixedString>(); };

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const Block &) override { return true; }

    /** Get the result type by argument types and constant argument values.
      * If the function does not apply to these arguments, throw an exception.
      * For non-constant columns arguments[i].column = nullptr.
      */
    void getReturnTypeAndPrerequisitesImpl(const ColumnsWithTypeAndName & arguments,
        DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites) override
    {
        if (!arguments[1].column)
            throw Exception("Second argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
        if (!typeid_cast<const DataTypeString *>(arguments[0].type.get()) &&
            !typeid_cast<const DataTypeFixedString *>(arguments[0].type.get()))
            throw Exception(getName() + " is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED);

        const size_t n = getSize(arguments[1]);

        out_return_type = std::make_shared<DataTypeFixedString>(n);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto n = getSize(block.safeGetByPosition(arguments[1]));
        return execute(block, arguments, result, n);
    }

    static void execute(Block & block, const ColumnNumbers & arguments, const size_t result, const size_t n)
    {
        const auto & column = block.safeGetByPosition(arguments[0]).column;

        if (const auto column_const = typeid_cast<const ColumnConstString *>(column.get()))
        {
            if (column_const->getData().size() > n)
                throw Exception("String too long for type FixedString(" + toString(n) + ")",
                    ErrorCodes::TOO_LARGE_STRING_SIZE);

            auto resized_string = column_const->getData();
            resized_string.resize(n);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<String>>(
                column_const->size(), std::move(resized_string), std::make_shared<DataTypeFixedString>(n));
        }
        else if (const auto column_string = typeid_cast<const ColumnString *>(column.get()))
        {
            const auto column_fixed = std::make_shared<ColumnFixedString>(n);
            ColumnPtr result_ptr = column_fixed;

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

            block.safeGetByPosition(result).column = result_ptr;
        }
        else if (const auto column_fixed_string = typeid_cast<const ColumnFixedString *>(column.get()))
        {
            const auto src_n = column_fixed_string->getN();
            if (src_n > n)
                throw Exception{
                    "String too long for type FixedString(" + toString(n) + ")",
                    ErrorCodes::TOO_LARGE_STRING_SIZE
                };

            const auto column_fixed = std::make_shared<ColumnFixedString>(n);
            block.safeGetByPosition(result).column = column_fixed;

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_fixed_string->getChars();
            const auto size = column_fixed_string->size();
            out_chars.resize_fill(size * n);

            for (const auto i : ext::range(0, size))
                memcpy(&out_chars[i * n], &in_chars[i * src_n], src_n);
        }
        else
            throw Exception("Unexpected column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    bool getSizeTyped(const ColumnWithTypeAndName & column, size_t & out_size)
    {
        if (!typeid_cast<const DataTypeNumber<T> *>(column.type.get()))
            return false;
        const ColumnConst<T> * column_const = typeid_cast<const ColumnConst<T> *>(column.column.get());
        if (!column_const)
            throw Exception("Unexpected type of column for FixedString length: " + column.column->getName(), ErrorCodes::ILLEGAL_COLUMN);
        T s = column_const->getData();
        if (s <= 0)
            throw Exception("FixedString length must be positive (unlike " + toString(s) + ")", ErrorCodes::ILLEGAL_COLUMN);
        out_size = static_cast<size_t>(s);
        return true;
    }

    size_t getSize(const ColumnWithTypeAndName & column)
    {
        size_t res;
        if (getSizeTyped<UInt8>(column, res) ||
            getSizeTyped<UInt16>(column, res) ||
            getSizeTyped<UInt32>(column, res) ||
            getSizeTyped<UInt64>(column, res) ||
            getSizeTyped< Int8 >(column, res) ||
            getSizeTyped< Int16>(column, res) ||
            getSizeTyped< Int32>(column, res) ||
            getSizeTyped< Int64>(column, res))
            return res;
        throw Exception("Length of FixedString must be integer; got " + column.type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


/// Monotonicity.

struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        return { true };
    }
};

template <typename T>
struct ToIntMonotonicity
{
    static bool has() { return true; }

    template <typename T2 = T>
    static UInt64 divideByRangeOfType(typename std::enable_if_t<sizeof(T2) != sizeof(UInt64), UInt64> x) { return x >> (sizeof(T) * 8); };

    template <typename T2 = T>
    static UInt64 divideByRangeOfType(typename std::enable_if_t<sizeof(T2) == sizeof(UInt64), UInt64> x) { return 0; };

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        size_t size_of_type = type.getSizeOfField();

        /// If type is expanding, then function is monotonic.
        if (sizeof(T) > size_of_type)
            return { true };

        /// If type is same, too. (Enum has separate case, because it is different data type)
        if (typeid_cast<const DataTypeNumber<T> *>(&type) ||
            typeid_cast<const DataTypeEnum<T> *>(&type))
            return { true };

        /// In other cases, if range is unbounded, we don't know, whether function is monotonic or not.
        if (left.isNull() || right.isNull())
            return {};

        /// If converting from float, for monotonicity, arguments must fit in range of result type.
        if (typeid_cast<const DataTypeFloat32 *>(&type)
            || typeid_cast<const DataTypeFloat64 *>(&type))
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

        if (typeid_cast<const DataTypeDate *>(&type)
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


struct NameToUInt8             { static constexpr auto name = "toUInt8"; };
struct NameToUInt16         { static constexpr auto name = "toUInt16"; };
struct NameToUInt32         { static constexpr auto name = "toUInt32"; };
struct NameToUInt64         { static constexpr auto name = "toUInt64"; };
struct NameToInt8             { static constexpr auto name = "toInt8"; };
struct NameToInt16             { static constexpr auto name = "toInt16"; };
struct NameToInt32            { static constexpr auto name = "toInt32"; };
struct NameToInt64            { static constexpr auto name = "toInt64"; };
struct NameToFloat32        { static constexpr auto name = "toFloat32"; };
struct NameToFloat64        { static constexpr auto name = "toFloat64"; };
struct NameToDateTime        { static constexpr auto name = "toDateTime"; };

using FunctionToUInt8         = FunctionConvert<DataTypeUInt8,    NameToUInt8,    ToIntMonotonicity<UInt8>>;
using FunctionToUInt16         = FunctionConvert<DataTypeUInt16,    NameToUInt16,    ToIntMonotonicity<UInt16>>;
using FunctionToUInt32         = FunctionConvert<DataTypeUInt32,    NameToUInt32,    ToIntMonotonicity<UInt32>>;
using FunctionToUInt64         = FunctionConvert<DataTypeUInt64,    NameToUInt64,    ToIntMonotonicity<UInt64>>;
using FunctionToInt8         = FunctionConvert<DataTypeInt8,        NameToInt8,        ToIntMonotonicity<Int8>>;
using FunctionToInt16         = FunctionConvert<DataTypeInt16,    NameToInt16,    ToIntMonotonicity<Int16>>;
using FunctionToInt32         = FunctionConvert<DataTypeInt32,    NameToInt32,    ToIntMonotonicity<Int32>>;
using FunctionToInt64         = FunctionConvert<DataTypeInt64,    NameToInt64,    ToIntMonotonicity<Int64>>;
using FunctionToFloat32     = FunctionConvert<DataTypeFloat32,    NameToFloat32,    PositiveMonotonicity>;
using FunctionToFloat64     = FunctionConvert<DataTypeFloat64,    NameToFloat64,    PositiveMonotonicity>;
using FunctionToDate         = FunctionConvert<DataTypeDate,        NameToDate,        ToIntMonotonicity<UInt16>>;
using FunctionToDateTime     = FunctionConvert<DataTypeDateTime,    NameToDateTime,    ToIntMonotonicity<UInt32>>;
using FunctionToString         = FunctionConvert<DataTypeString,    NameToString,     ToStringMonotonicity>;
using FunctionToUnixTimestamp = FunctionConvert<DataTypeUInt32,    NameToUnixTimestamp, ToIntMonotonicity<UInt32>>;

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
template <> struct FunctionTo<DataTypeString> { using Type = FunctionToString; };
template <> struct FunctionTo<DataTypeFixedString> { using Type = FunctionToFixedString; };
template <typename FieldType> struct FunctionTo<DataTypeEnum<FieldType>>
    : FunctionTo<DataTypeNumber<FieldType>>
{
};

struct NameToUInt8OrZero         { static constexpr auto name = "toUInt8OrZero"; };
struct NameToUInt16OrZero         { static constexpr auto name = "toUInt16OrZero"; };
struct NameToUInt32OrZero         { static constexpr auto name = "toUInt32OrZero"; };
struct NameToUInt64OrZero         { static constexpr auto name = "toUInt64OrZero"; };
struct NameToInt8OrZero         { static constexpr auto name = "toInt8OrZero"; };
struct NameToInt16OrZero         { static constexpr auto name = "toInt16OrZero"; };
struct NameToInt32OrZero        { static constexpr auto name = "toInt32OrZero"; };
struct NameToInt64OrZero        { static constexpr auto name = "toInt64OrZero"; };
struct NameToFloat32OrZero        { static constexpr auto name = "toFloat32OrZero"; };
struct NameToFloat64OrZero        { static constexpr auto name = "toFloat64OrZero"; };

using FunctionToUInt8OrZero     = FunctionConvertOrZero<DataTypeUInt8,        NameToUInt8OrZero>;
using FunctionToUInt16OrZero     = FunctionConvertOrZero<DataTypeUInt16,        NameToUInt16OrZero>;
using FunctionToUInt32OrZero     = FunctionConvertOrZero<DataTypeUInt32,        NameToUInt32OrZero>;
using FunctionToUInt64OrZero     = FunctionConvertOrZero<DataTypeUInt64,        NameToUInt64OrZero>;
using FunctionToInt8OrZero         = FunctionConvertOrZero<DataTypeInt8,        NameToInt8OrZero>;
using FunctionToInt16OrZero     = FunctionConvertOrZero<DataTypeInt16,        NameToInt16OrZero>;
using FunctionToInt32OrZero     = FunctionConvertOrZero<DataTypeInt32,        NameToInt32OrZero>;
using FunctionToInt64OrZero     = FunctionConvertOrZero<DataTypeInt64,        NameToInt64OrZero>;
using FunctionToFloat32OrZero     = FunctionConvertOrZero<DataTypeFloat32,    NameToFloat32OrZero>;
using FunctionToFloat64OrZero     = FunctionConvertOrZero<DataTypeFloat64,    NameToFloat64OrZero>;


class FunctionCast final : public IFunction
{
    using WrapperType = std::function<void(Block &, const ColumnNumbers &, size_t)>;
    const Context & context;
    WrapperType wrapper_function;
    std::function<Monotonicity(const IDataType &, const Field &, const Field &)> monotonicity_for_range;

public:
    FunctionCast(const Context & context) : context(context) {}
private:

    template <typename DataType>
    WrapperType createWrapper(const DataTypePtr & from_type, const DataType * const)
    {
        using FunctionType = typename FunctionTo<DataType>::Type;

        auto function = FunctionType::create(context);

        /// Check conversion using underlying function
        (void) function->getReturnType({ from_type });

        return [function] (Block & block, const ColumnNumbers & arguments, const size_t result) {
            function->execute(block, arguments, result);
        };
    }

    static WrapperType createFixedStringWrapper(const DataTypePtr & from_type, const size_t N)
    {
        if (!typeid_cast<const DataTypeString *>(from_type.get()) &&
            !typeid_cast<const DataTypeFixedString *>(from_type.get()))
            throw Exception{
                "CAST AS FixedString is only implemented for types String and FixedString",
                ErrorCodes::NOT_IMPLEMENTED
            };

        return [N] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            FunctionToFixedString::execute(block, arguments, result, N);
        };
    }

    WrapperType createArrayWrapper(const DataTypePtr & from_type_untyped, const DataTypeArray * to_type)
    {
        /// Conversion from String through parsing.
        if (typeid_cast<const DataTypeString *>(from_type_untyped.get()))
        {
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                ConvertImplGenericFromString::execute(block, arguments, result);
            };
        }

        DataTypePtr from_nested_type, to_nested_type;
        auto from_type = typeid_cast<const DataTypeArray *>(from_type_untyped.get());

        /// get the most nested type
        while (from_type && to_type)
        {
            from_nested_type = from_type->getNestedType();
            to_nested_type = to_type->getNestedType();

            from_type = typeid_cast<const DataTypeArray *>(from_nested_type.get());
            to_type = typeid_cast<const DataTypeArray *>(to_nested_type.get());
        }

        /// both from_type and to_type should be nullptr now is array types had same dimensions
        if (from_type || to_type)
            throw Exception{
                "CAST AS Array can only be performed between same-dimensional array types or from String",
                ErrorCodes::TYPE_MISMATCH
            };

        /// Prepare nested type conversion
        const auto nested_function = prepareImpl(from_nested_type, to_nested_type.get());

        return [nested_function, from_nested_type, to_nested_type] (
            Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            auto array_arg = block.safeGetByPosition(arguments.front());

            /// @todo add const variant which retains array constness
            if (const auto col_const_array = typeid_cast<const ColumnConstArray *>(array_arg.column.get()))
                array_arg.column = col_const_array->convertToFullColumn();

            if (auto col_array = typeid_cast<const ColumnArray *>(array_arg.column.get()))
            {
                auto res = new ColumnArray(nullptr, col_array->getOffsetsColumn());
                block.safeGetByPosition(result).column.reset(res);

                /// get the most nested column
                while (const auto nested_col_array = typeid_cast<const ColumnArray *>(col_array->getDataPtr().get()))
                {
                    /// create new level of array, copy offsets
                    res->getDataPtr() = std::make_shared<ColumnArray>(nullptr, nested_col_array->getOffsetsColumn());

                    res = static_cast<ColumnArray *>(res->getDataPtr().get());
                    col_array = nested_col_array;
                }

                /// create block for converting nested column containing original and result columns
                Block nested_block{
                    { col_array->getDataPtr(), from_nested_type, "" },
                    { nullptr, to_nested_type, "" }
                };

                const auto nested_result = 1;
                /// convert nested column
                nested_function(nested_block, {0 }, nested_result);

                /// set converted nested column to result
                res->getDataPtr() = nested_block.safeGetByPosition(nested_result).column;
            }
            else
                throw Exception{
                    "Illegal column " + array_arg.column->getName() + " for function CAST AS Array",
                    ErrorCodes::LOGICAL_ERROR};
        };
    }

    WrapperType createTupleWrapper(const DataTypePtr & from_type_untyped, const DataTypeTuple * to_type)
    {
        /// Conversion from String through parsing.
        if (typeid_cast<const DataTypeString *>(from_type_untyped.get()))
        {
            return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                ConvertImplGenericFromString::execute(block, arguments, result);
            };
        }

        const auto from_type = typeid_cast<const DataTypeTuple *>(from_type_untyped.get());
        if (!from_type)
            throw Exception{
                "CAST AS Tuple can only be performed between tuple types or from String.\nLeft type: " + from_type_untyped->getName() +
                    ", right type: " + to_type->getName(),
                ErrorCodes::TYPE_MISMATCH
            };

        if (from_type->getElements().size() != to_type->getElements().size())
            throw Exception{
                "CAST AS Tuple can only be performed between tuple types with the same number of elements or from String.\n"
                    "Left type: " + from_type->getName() + ", right type: " + to_type->getName(),
                 ErrorCodes::TYPE_MISMATCH
            };

        const auto & from_element_types = from_type->getElements();
        const auto & to_element_types = to_type->getElements();
        std::vector<WrapperType> element_wrappers;
        element_wrappers.reserve(from_element_types.size());

        /// Create conversion wrapper for each element in tuple
        for (const auto & idx_type : ext::enumerate(from_type->getElements()))
            element_wrappers.push_back(prepareImpl(idx_type.second, to_element_types[idx_type.first].get()));

        auto function_tuple = FunctionTuple::create(context);
        return [element_wrappers, function_tuple, from_element_types, to_element_types]
            (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            const auto col = block.safeGetByPosition(arguments.front()).column.get();

            /// copy tuple elements to a separate block
            Block element_block;

            /// @todo retain constness
            if (const auto column_tuple = typeid_cast<const ColumnTuple *>(col))
                element_block = column_tuple->getData();
            else if (const auto column_const_tuple = typeid_cast<const ColumnConstTuple *>(col))
                element_block = static_cast<const ColumnTuple &>(*column_const_tuple->convertToTupleOfConstants()).getData();

            /// create columns for converted elements
            for (const auto & to_element_type : to_element_types)
                element_block.insert({ nullptr, to_element_type, "" });

            /// store position for converted tuple
            const auto converted_tuple_pos = element_block.columns();

            /// insert column for converted tuple
            element_block.insert({ nullptr, std::make_shared<DataTypeTuple>(to_element_types), "" });

            const auto converted_element_offset = from_element_types.size();

            /// invoke conversion for each element
            for (const auto & idx_element_wrapper : ext::enumerate(element_wrappers))
                idx_element_wrapper.second(element_block, { idx_element_wrapper.first },
                    converted_element_offset + idx_element_wrapper.first);

            /// form tuple from converted elements using FunctionTuple
            function_tuple->execute(element_block,
                ext::collection_cast<ColumnNumbers>(ext::range(converted_element_offset, 2 * converted_element_offset)),
                converted_tuple_pos);

            /// copy FunctionTuple's result from element_block to resulting block
            block.safeGetByPosition(result).column = element_block.safeGetByPosition(converted_tuple_pos).column;
        };
    }

    template <typename FieldType>
    WrapperType createEnumWrapper(const DataTypePtr & from_type, const DataTypeEnum<FieldType> * to_type)
    {
        using EnumType = DataTypeEnum<FieldType>;
        using Function = typename FunctionTo<EnumType>::Type;

        if (const auto from_enum8 = typeid_cast<const DataTypeEnum8 *>(from_type.get()))
            checkEnumToEnumConversion(from_enum8, to_type);
        else if (const auto from_enum16 = typeid_cast<const DataTypeEnum16 *>(from_type.get()))
            checkEnumToEnumConversion(from_enum16, to_type);

        if (typeid_cast<const DataTypeString *>(from_type.get()))
            return createStringToEnumWrapper<ColumnString, EnumType>();
        else if (typeid_cast<const DataTypeFixedString *>(from_type.get()))
            return createStringToEnumWrapper<ColumnFixedString, EnumType>();
        else if (from_type->behavesAsNumber())
        {
            auto function = Function::create(context);

            /// Check conversion using underlying function
            (void) function->getReturnType({ from_type });

            return [function] (Block & block, const ColumnNumbers & arguments, const size_t result) {
                function->execute(block, arguments, result);
            };
        }
        else
            throw Exception{
                "Conversion from " + from_type->getName() + " to " + to_type->getName() +
                    " is not supported",
                ErrorCodes::CANNOT_CONVERT_TYPE
            };
    }

    template <typename EnumTypeFrom, typename EnumTypeTo>
    void checkEnumToEnumConversion(const EnumTypeFrom * const from_type, const EnumTypeTo * const to_type)
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
                    ErrorCodes::CANNOT_CONVERT_TYPE
                };
        }
    };

    template <typename ColumnStringType, typename EnumType>
    WrapperType createStringToEnumWrapper()
    {
        return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            const auto first_col = block.safeGetByPosition(arguments.front()).column.get();

            auto & col_with_type_and_name = block.safeGetByPosition(result);
            auto & result_col = col_with_type_and_name.column;
            const auto & result_type = typeid_cast<EnumType &>(*col_with_type_and_name.type);

            if (const auto col = typeid_cast<const ColumnStringType *>(first_col))
            {
                const auto size = col->size();

                auto res = result_type.createColumn();
                auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
                out_data.resize(size);

                for (const auto i : ext::range(0, size))
                    out_data[i] = result_type.getValue(col->getDataAt(i));

                result_col = res;
            }
            else if (const auto const_col = typeid_cast<const ColumnConstString *>(first_col))
            {
                result_col = result_type.createConstColumn(const_col->size(),
                    nearestFieldType(result_type.getValue(const_col->getData())));
            }
            else
                throw Exception{
                    "Unexpected column " + first_col->getName() + " as first argument of function " +
                        name,
                    ErrorCodes::LOGICAL_ERROR
                };
        };
    }

    WrapperType createIdentityWrapper(const DataTypePtr &)
    {
        return [] (Block & block, const ColumnNumbers & arguments, const size_t result)
        {
            block.safeGetByPosition(result).column = block.safeGetByPosition(arguments.front()).column;
        };
    }

    /// Actions to be taken when performing a conversion.
    struct Action
    {
        /// If neither the input type nor the output type is nullable or null,
        /// we perform the conversion without any pre and/or processing.
        static constexpr auto NONE = UInt64(0);
        /// The input has a nullable type. We must extract its nested type
        /// before performing any conversion.
        static constexpr auto UNWRAP_NULLABLE_INPUT = UInt64(1) << 0;
        /// The output has a nullable type. We must wrap the result from the
        /// conversion into a ColumnNullable.
        static constexpr auto WRAP_RESULT_INTO_NULLABLE = UInt64(1) << 1;
        /// The input is the NULL value. Before performing any conversion,
        /// we will turn it into a single UInt8 zero value.
        static constexpr auto CONVERT_NULL = UInt64(1) << 2;
    };

    WrapperType prepare(const DataTypePtr & from_type, const IDataType * const to_type, const uint64_t action)
    {
        auto wrapper = prepareImpl((action & Action::CONVERT_NULL) ?
                                        std::make_shared<DataTypeUInt8>() :
                                        from_type,
                                    to_type);

        if (action & Action::WRAP_RESULT_INTO_NULLABLE)
        {
            return [wrapper, action] (Block & block, const ColumnNumbers & arguments, const size_t result)
            {
                /// Create a temporary block on which to perform the operation.
                auto & res = block.safeGetByPosition(result);
                const auto & ret_type = res.type;
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*ret_type);
                const auto & nested_type = nullable_type.getNestedType();

                Block tmp_block;
                if (action & Action::UNWRAP_NULLABLE_INPUT)
                    tmp_block = createBlockWithNestedColumns(block, arguments);
                else if (action & Action::CONVERT_NULL)
                {
                    /// The input is replaced by a trivial UInt8 column
                    /// which contains only one row whose value is 0.
                    tmp_block = block;
                    auto & elem = tmp_block.getByPosition(arguments[0]);
                    elem.column = std::make_shared<ColumnUInt8>(block.rows(), 0);
                    elem.type = std::make_shared<DataTypeUInt8>();
                }
                else
                    tmp_block = block;

                size_t tmp_res_index = block.columns();
                tmp_block.insert({nullptr, nested_type, ""});

                /// Perform the requested conversion.
                wrapper(tmp_block, arguments, tmp_res_index);

                /// Wrap the result into a nullable column.
                ColumnPtr null_map;

                if (action & Action::UNWRAP_NULLABLE_INPUT)
                {
                    /// This is a conversion from a nullable to a nullable type.
                    /// So we just keep the null map of the input argument.
                    const auto & col = block.safeGetByPosition(arguments[0]).column;
                    const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
                    null_map = nullable_col.getNullMapColumn();
                }
                else if (action & Action::CONVERT_NULL)
                {
                    /// A NULL value has been converted to a nullable type.
                    null_map = std::make_shared<ColumnUInt8>(block.rows(), 1);
                }
                else
                {
                    /// This is a conversion from an ordinary type to a nullable type.
                    /// So we create a trivial null map.
                    null_map = std::make_shared<ColumnUInt8>(block.rows(), 0);
                }

                const auto & tmp_res = tmp_block.safeGetByPosition(tmp_res_index);
                res.column = std::make_shared<ColumnNullable>(tmp_res.column, null_map);
            };
        }
        else
            return wrapper;
    }

    WrapperType prepareImpl(const DataTypePtr & from_type, const IDataType * const to_type)
    {
        if (from_type->equals(*to_type))
            return createIdentityWrapper(from_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeUInt8 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeUInt16 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeUInt32 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeUInt64 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeInt8 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeInt16 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeInt32 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeInt64 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeFloat32 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeFloat64 *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeDate *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeDateTime *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto to_actual_type = typeid_cast<const DataTypeString *>(to_type))
            return createWrapper(from_type, to_actual_type);
        else if (const auto type_fixed_string = typeid_cast<const DataTypeFixedString *>(to_type))
            return createFixedStringWrapper(from_type, type_fixed_string->getN());
        else if (const auto type_array = typeid_cast<const DataTypeArray *>(to_type))
            return createArrayWrapper(from_type, type_array);
        else if (const auto type_tuple = typeid_cast<const DataTypeTuple *>(to_type))
            return createTupleWrapper(from_type, type_tuple);
        else if (const auto type_enum = typeid_cast<const DataTypeEnum8 *>(to_type))
            return createEnumWrapper(from_type, type_enum);
        else if (const auto type_enum = typeid_cast<const DataTypeEnum16 *>(to_type))
            return createEnumWrapper(from_type, type_enum);

        /// It's possible to use ConvertImplGenericFromString to convert from String to AggregateFunction,
        ///  but it is disabled because deserializing aggregate functions state might be unsafe.

        throw Exception{
            "Conversion from " + from_type->getName() + " to " + to_type->getName() +
                " is not supported",
            ErrorCodes::CANNOT_CONVERT_TYPE
        };
    }

    template <typename DataType> static auto monotonicityForType(const DataType * const)
    {
        return FunctionTo<DataType>::Type::Monotonic::get;
    }

    void prepareMonotonicityInformation(const DataTypePtr & from_type, const IDataType * to_type)
    {
        if (const auto type = typeid_cast<const DataTypeUInt8 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeUInt16 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeUInt32 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeUInt64 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeInt8 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeInt16 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeInt32 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeInt64 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeFloat32 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeFloat64 *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeDate *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeDateTime *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (const auto type = typeid_cast<const DataTypeString *>(to_type))
            monotonicity_for_range = monotonicityForType(type);
        else if (from_type->isNumeric())
        {
            if (const auto type = typeid_cast<const DataTypeEnum8 *>(to_type))
                monotonicity_for_range = monotonicityForType(type);
            else if (const auto type = typeid_cast<const DataTypeEnum16 *>(to_type))
                monotonicity_for_range = monotonicityForType(type);
        }
        /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
    }

public:
    static constexpr auto name = "CAST";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionCast>(context); }

    String getName() const override { return name; }

    bool hasSpecialSupportForNulls() const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments, DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites) override
    {
        const auto type_col = typeid_cast<const ColumnConstString *>(arguments.back().column.get());
        if (!type_col)
            throw Exception("Second argument to " + getName() + " must be a constant string describing type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        out_return_type = DataTypeFactory::instance().get(type_col->getData());

        /// Determine whether pre-processing and/or post-processing must take
        /// place during conversion.
        uint64_t action = Action::NONE;
        const auto & from_type = arguments.front().type;

        if (from_type->isNullable())
            action |= Action::UNWRAP_NULLABLE_INPUT;
        else if (from_type->isNull() && !out_return_type->isNull())
            action |= Action::CONVERT_NULL;

        if (out_return_type->isNullable())
            action |= Action::WRAP_RESULT_INTO_NULLABLE;

        /// Check that the requested conversion is allowed.
        if (!(action & Action::WRAP_RESULT_INTO_NULLABLE))
        {
            if (action & Action::CONVERT_NULL)
                throw Exception{"Cannot convert NULL into a non-nullable type",
                    ErrorCodes::CANNOT_CONVERT_TYPE};
            else if (action & Action::UNWRAP_NULLABLE_INPUT)
                throw Exception{"Cannot convert data from a nullable type to a non-nullable type",
                    ErrorCodes::CANNOT_CONVERT_TYPE};
        }

        DataTypePtr from_inner_type;
        const IDataType * to_inner_type;

        /// Create the requested conversion.
        if (action & Action::WRAP_RESULT_INTO_NULLABLE)
        {
            if (action & Action::UNWRAP_NULLABLE_INPUT)
            {
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*from_type);
                from_inner_type = nullable_type.getNestedType();
            }
            else
                from_inner_type = from_type;

            const auto & nullable_type = static_cast<const DataTypeNullable &>(*out_return_type);
            to_inner_type = nullable_type.getNestedType().get();
        }
        else
        {
            from_inner_type = from_type;
            to_inner_type = out_return_type.get();
        }

        wrapper_function = prepare(from_inner_type, to_inner_type, action);
        prepareMonotonicityInformation(from_inner_type, to_inner_type);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        /// drop second argument, pass others
        ColumnNumbers new_arguments{arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2), std::end(arguments));

        wrapper_function(block, new_arguments, result);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return monotonicity_for_range(type, left, right);
    }
};

}
