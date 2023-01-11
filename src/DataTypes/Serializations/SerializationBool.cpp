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
    if (!checkAndGetColumn<ColumnUInt8>(&column))
        throw Exception("Bool type can only serialize columns of type UInt8." + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    return col;
}

ColumnUInt8 * checkAndGetDeserializeColumnType(IColumn & column)
{
    auto * col =  typeid_cast<ColumnUInt8 *>(&column);
    if (!checkAndGetColumn<ColumnUInt8>(&column))
        throw Exception("Bool type can only deserialize columns of type UInt8." + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
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

void deserializeImpl(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings, std::function<bool(ReadBuffer &)> check_end_of_value)
{
    ColumnUInt8 * col = checkAndGetDeserializeColumnType(column);

    PeekableReadBuffer buf(istr);
    buf.setCheckpoint();
    if (checkString(settings.bool_true_representation, buf) && check_end_of_value(buf))
    {
        col->insert(true);
        return;
    }

    buf.rollbackToCheckpoint();
    if (checkString(settings.bool_false_representation, buf) && check_end_of_value(buf))
    {
        col->insert(false);
        buf.dropCheckpoint();
        if (buf.hasUnreadData())
            throw Exception(
                ErrorCodes::CANNOT_PARSE_BOOL,
                "Cannot continue parsing after parsed bool value because it will result in the loss of some data. It may happen if "
                "bool_true_representation or bool_false_representation contains some delimiters of input format");
        return;
    }

    buf.rollbackToCheckpoint();
    if (tryDeserializeAllVariants(col, buf) && check_end_of_value(buf))
    {
        buf.dropCheckpoint();
        if (buf.hasUnreadData())
            throw Exception(
                ErrorCodes::CANNOT_PARSE_BOOL,
                "Cannot continue parsing after parsed bool value because it will result in the loss of some data. It may happen if "
                "bool_true_representation or bool_false_representation contains some delimiters of input format");
        return;
    }

    buf.makeContinuousMemoryFromCheckpointToPos();
    buf.rollbackToCheckpoint();
    throw Exception(
        ErrorCodes::CANNOT_PARSE_BOOL,
        "Cannot parse boolean value here: '{}', should be '{}' or '{}' controlled by setting bool_true_representation and "
        "bool_false_representation or one of "
        "True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0",
        String(buf.position(), std::min(10lu, buf.available())),
        settings.bool_true_representation, settings.bool_false_representation);
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
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_BOOL);

    deserializeImpl(column, istr, settings, [](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'; });
}

void SerializationBool::serializeTextJSON(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &settings) const
{
    serializeSimple(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextJSON(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    if (istr.eof())
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_BOOL);

    ColumnUInt8 * col = checkAndGetDeserializeColumnType(column);
    bool value = false;

    if (*istr.position() == 't' || *istr.position() == 'f')
        readBoolTextWord(value, istr);
    else if (*istr.position() == '1' || *istr.position() == '0')
        readBoolText(value, istr);
    else
        throw Exception("Invalid boolean value, should be true/false, 1/0.",
                        ErrorCodes::CANNOT_PARSE_BOOL);
    col->insert(value);
}

void SerializationBool::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeCustom(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_BOOL);

    deserializeImpl(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\n'; });
}

void SerializationBool::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeCustom(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_BOOL);

    deserializeImpl(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'; });
}

void SerializationBool::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeSimple(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_BOOL);

    auto * col = checkAndGetDeserializeColumnType(column);

    char symbol = toLowerIfAlphaASCII(*istr.position());
    switch (symbol)
    {
        case 't':
            assertStringCaseInsensitive("true", istr);
            col->insert(true);
            break;
        case 'f':
            assertStringCaseInsensitive("false", istr);
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
            deserializeImpl(column, istr, settings, [](ReadBuffer & buf){ return !buf.eof() && *buf.position() == '\''; });
            assertChar('\'', istr);
            break;
        default:
            throw Exception(
                ErrorCodes::CANNOT_PARSE_BOOL,
                "Cannot parse boolean value here: '{}', should be true/false, 1/0 or on of "
                "True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0 in quotes",
                String(istr.position(), std::min(10ul, istr.available())));
    }
}

void SerializationBool::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (istr.eof())
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_BOOL);

    deserializeImpl(column, istr, settings, [&](ReadBuffer & buf){ return buf.eof(); });
}

void SerializationBool::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeSimple(column, row_num, ostr, settings);
}

}
