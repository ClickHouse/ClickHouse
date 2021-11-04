#include <DataTypes/Serializations/SerializationBool.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int ILLEGAL_COLUMN;
}

SerializationBool::SerializationBool(const SerializationPtr &nested_)
        : SerializationCustomSimpleText(nested_)
{
}

void SerializationBool::serializeText(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &) const
{
    const auto *col = checkAndGetColumn<ColumnUInt8>(&column);
    if (!col)
        throw Exception("Bool type can only serialize columns of type UInt8." + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);

    if (col->getData()[row_num])
        ostr.write(str_true, sizeof(str_true) - 1);
    else
        ostr.write(str_false, sizeof(str_false) - 1);
}

void SerializationBool::deserializeText(IColumn &column, ReadBuffer &istr, const FormatSettings &) const
{
    ColumnUInt8 *col = typeid_cast<ColumnUInt8 *>(&column);
    if (!col)
    {
        throw Exception("Bool type can only deserialize columns of type UInt32." + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }

    if (!istr.eof())
    {
        bool value = false;

        if (*istr.position() == 't' || *istr.position() == 'f')
            readBoolTextWord(value, istr);
        else if (*istr.position() == '1' || *istr.position() == '0')
            readBoolText(value, istr);
        else
            throw Exception("Invalid boolean value, should be true, false, 1, or 0.",
                            ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
        col->insert(value);
    }
    else
        throw Exception("Expected boolean value but get EOF.", ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING);
}

void SerializationBool::serializeTextJSON(const IColumn &column, size_t row_num, WriteBuffer &ostr, const FormatSettings &settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationBool::deserializeTextJSON(IColumn &column, ReadBuffer &istr, const FormatSettings &settings) const
{
    deserializeText(column, istr, settings);
}

}
