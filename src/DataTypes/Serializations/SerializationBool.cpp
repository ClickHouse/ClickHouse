#include <DataTypes/Serializations/SerializationBool.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/PeekableReadBuffer.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_PARSE_BOOL;
}

namespace
{

constexpr char str_true[5] = "true";
constexpr char str_false[6] = "false";

const ColumnUInt8 * checkAndGetSerializeColumnType(const IColumn & column)
{
    const auto * col = checkAndGetColumn<ColumnUInt8>(&column);
    if (!col)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Bool type can only serialize columns of type UInt8.{}", column.getName());
    return col;
}

ColumnUInt8 * checkAndGetDeserializeColumnType(IColumn & column)
{
    auto * col =  typeid_cast<ColumnUInt8 *>(&column);
    if (!col)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Bool type can only deserialize columns of type UInt8.{}",
                        column.getName());
    return col;
}

void serializeCustom(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings)
{
    const auto * col = checkAndGetSerializeColumnType(column);

    if (col->getData()[row_num])
    {
        writeString(settings.bool_true_representation, ostr);
    }
    else
    {
        writeString(settings.bool_false_representation, ostr);
    }
}

void serializeSimple(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &)
{
    const auto * col = checkAndGetSerializeColumnType(column);

    if (col->getData()[row_num])
        ostr.write(str_true, sizeof(str_true) - 1);
    else
        ostr.write(str_false, sizeof(str_false) - 1);
}

bool tryDeserializeAllVariants(ColumnUInt8 * column, ReadBuffer & istr)
{
    if (checkCharCaseInsensitive('1', istr))
    {
        column->insert(true);
    }
    else if (checkCharCaseInsensitive('0', istr))
    {
        column->insert(false);
    }
    /// 'True' and 'T'
    else if (checkCharCaseInsensitive('t', istr))
    {
        /// Check if it's just short form `T` or full form `True`
        if (checkCharCaseInsensitive('r', istr))
        {
            if (!checkStringCaseInsensitive("ue", istr))
                return false;
        }
        column->insert(true);
    }
    /// 'False' and 'F'
    else if (checkCharCaseInsensitive('f', istr))
    {
        /// Check if it's just short form `F` or full form `False`
        if (checkCharCaseInsensitive('a', istr))
        {
            if (!checkStringCaseInsensitive("lse", istr))
                return false;
        }
        column->insert(false);
    }
    /// 'Yes' and 'Y'
    else if (checkCharCaseInsensitive('y', istr))
    {
        /// Check if it's just short form `Y` or full form `Yes`
        if (checkCharCaseInsensitive('e', istr))
        {
            if (!checkCharCaseInsensitive('s', istr))
                return false;
        }
        column->insert(true);
    }
    /// 'No' and 'N'
    else if (checkCharCaseInsensitive('n', istr))
    {
        /// Check if it's just short form `N` or full form `No`
        checkCharCaseInsensitive('o', istr);
        column->insert(false);
    }
    /// 'On' and 'Off'
    else if (checkCharCaseInsensitive('o', istr))
    {
        if (checkCharCaseInsensitive('n', istr))
            column->insert(true);
        else if (checkStringCaseInsensitive("ff", istr))
        {
            column->insert(false);
        }
        else
            return false;
    }
    /// 'Enable' and 'Enabled'
    else if (checkStringCaseInsensitive("enable", istr))
    {
        /// Check if it's 'enable' or 'enabled'
        checkCharCaseInsensitive('d', istr);
        column->insert(true);
    }
    /// 'Disable' and 'Disabled'
    else if (checkStringCaseInsensitive("disable", istr))
    {
        /// Check if it's 'disable' or 'disabled'
        checkCharCaseInsensitive('d', istr);
        column->insert(false);
    }
    else
    {
        return false;
    }

    return true;
}

template <typename ReturnType = void>
ReturnType deserializeImpl(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings, std::function<bool(ReadBuffer &)> check_end_of_value)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    ColumnUInt8 * col = checkAndGetDeserializeColumnType(column);
    auto restore_column_if_needed = [&, prev_size = col->size()]()
    {
        if (col->size() > prev_size)
            col->popBack(1);
    };

    PeekableReadBuffer buf(istr);
    buf.setCheckpoint();
    if (checkString(settings.bool_true_representation, buf) && check_end_of_value(buf))
    {
        col->insert(true);
        return ReturnType(true);
    }

    buf.rollbackToCheckpoint();
    if (checkString(settings.bool_false_representation, buf) && check_end_of_value(buf))
    {
        buf.dropCheckpoint();
        if (buf.hasUnreadData())
        {
            if constexpr (throw_exception)
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_BOOL,
                    "Cannot continue parsing after parsed bool value because it will result in the loss of some data. It may happen if "
                    "bool_true_representation or bool_false_representation contains some delimiters of input format");
            return ReturnType(false);
        }
        col->insert(false);
        return ReturnType(true);
    }

    buf.rollbackToCheckpoint();
    if (tryDeserializeAllVariants(col, buf) && check_end_of_value(buf))
    {
        buf.dropCheckpoint();
        if (buf.hasUnreadData())
        {
            restore_column_if_needed();
            if constexpr (throw_exception)
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_BOOL,
                    "Cannot continue parsing after parsed bool value because it will result in the loss of some data. It may happen if "
                    "bool_true_representation or bool_false_representation contains some delimiters of input format");
            return ReturnType(false);
        }
        return ReturnType(true);
    }

    buf.makeContinuousMemoryFromCheckpointToPos();
    buf.rollbackToCheckpoint();
    restore_column_if_needed();
    if constexpr (throw_exception)
        throw Exception(
            ErrorCodes::CANNOT_PARSE_BOOL,
            "Cannot parse boolean value here: '{}', should be '{}' or '{}' controlled by setting bool_true_representation and "
            "bool_false_representation or one of "
            "True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0",
            String(buf.position(), std::min(10lu, buf.available())),
            settings.bool_true_representation, settings.bool_false_representation);

    return ReturnType(false);
}

}


SerializationBool::SerializationBool(const SerializationPtr &nested_)
        : SerializationWrapper(nested_)
{
}

void SerializationBool::serializeText(const IColumn & column, size_t row_num, WriteBuffer &ostr, const FormatSettings & settings) const
{
    serializeCustom(column, row_num, ostr, settings);
}

void SerializationBool::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeCustom(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");
    if (settings.tsv.crlf_end_of_line_input)
        deserializeImpl(column, istr, settings, [](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n' || *buf.position() == '\r'; });
    else
        deserializeImpl(column, istr, settings, [](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'; });
}

bool SerializationBool::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        return false;

    return deserializeImpl<bool>(column, istr, settings, [](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'; });
}

void SerializationBool::serializeTextJSON(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &settings) const
{
    serializeSimple(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextJSON(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    if (istr.eof())
        throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");

    ColumnUInt8 * col = checkAndGetDeserializeColumnType(column);
    bool value = false;

    char first_char = *istr.position();
    if (first_char == 't' || first_char == 'f')
        readBoolTextWord(value, istr);
    else if (first_char == '1' || first_char == '0')
        readBoolText(value, istr);
    else
        throw Exception(ErrorCodes::CANNOT_PARSE_BOOL,
            "Invalid boolean value, should be true/false, 1/0, but it starts with the '{}' character.", first_char);

    col->insert(value);
}

bool SerializationBool::tryDeserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings &) const
{
    if (istr.eof())
        return false;

    ColumnUInt8 * col = checkAndGetDeserializeColumnType(column);
    bool value = false;
    char first_char = *istr.position();
    if (first_char == 't' || first_char == 'f')
    {
        if (!readBoolTextWord<bool>(value, istr))
            return false;
    }
    else if (first_char == '1' || first_char == '0')
    {
        /// Doesn't throw.
        readBoolText(value, istr);
    }
    else
    {
        return false;
    }

    col->insert(value);
    return true;
}

void SerializationBool::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeCustom(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");

    deserializeImpl(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r'; });
}

bool SerializationBool::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        return false;

    return deserializeImpl<bool>(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r'; });
}

void SerializationBool::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeCustom(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");

    deserializeImpl(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'; });
}

bool SerializationBool::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        return false;

    return deserializeImpl<bool>(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'; });
}

void SerializationBool::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeSimple(column, row_num, ostr, settings);
}

template <typename ReturnType>
ReturnType deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (istr.eof())
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");
        return ReturnType(false);
    }

    auto * col = checkAndGetDeserializeColumnType(column);

    char symbol = toLowerIfAlphaASCII(*istr.position());
    switch (symbol)
    {
        case 't':
            if constexpr (throw_exception)
                assertStringCaseInsensitive("true", istr);
            else if (!checkStringCaseInsensitive("true", istr))
                return ReturnType(false);
            col->insert(true);
            break;
        case 'f':
            if constexpr (throw_exception)
                assertStringCaseInsensitive("false", istr);
            else if (!checkStringCaseInsensitive("false", istr))
                return ReturnType(false);
            col->insert(false);
            break;
        case '1':
            col->insert(true);
            break;
        case '0':
            col->insert(false);
            break;
        case '\'':
            ++istr.position();
            if constexpr (throw_exception)
            {
                deserializeImpl(column, istr, settings, [](ReadBuffer & buf){ return !buf.eof() && *buf.position() == '\''; });
                assertChar('\'', istr);
            }
            else
            {
                if (!deserializeImpl<bool>(column, istr, settings, [](ReadBuffer & buf) { return !buf.eof() && *buf.position() == '\''; }) || !checkChar('\'', istr))
                    return ReturnType(false);
            }
            break;
        default:
        {
            if constexpr (throw_exception)
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_BOOL,
                    "Cannot parse boolean value here: '{}', should be true/false, 1/0 or on of "
                    "True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0 in quotes",
                    String(istr.position(), std::min(10ul, istr.available())));
            return ReturnType(false);
        }
    }

    return ReturnType(true);
}

void SerializationBool::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextQuotedImpl<void>(column, istr, settings);
}

bool SerializationBool::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextQuotedImpl<bool>(column, istr, settings);
}

void SerializationBool::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Expected boolean value but get EOF.");

    deserializeImpl(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof(); });
}

bool SerializationBool::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        return false;

    return deserializeImpl<bool>(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof(); });
}

void SerializationBool::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeSimple(column, row_num, ostr, settings);
}

}
