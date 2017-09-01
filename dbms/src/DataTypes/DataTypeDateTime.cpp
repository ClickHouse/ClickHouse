#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

void DataTypeDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeDateTimeText(static_cast<const ColumnUInt32 &>(column).getData()[row_num], ostr);
}

static void deserializeText(IColumn & column, ReadBuffer & istr)
{
    time_t x;
    readDateTimeText(x, istr);
    static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}

void DataTypeDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr);
    writeChar('\'', ostr);
}

void DataTypeDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    time_t x;
    assertChar('\'', istr);
    readDateTimeText(x, istr);
    assertChar('\'', istr);
    static_cast<ColumnUInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void DataTypeDateTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    time_t x;
    if (checkChar('"', istr)) /// Cases: "2017-08-31 18:36:48" or "1504193808"
    {
        readDateTimeText(x, istr);
        assertChar('"', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
    time_t x;
    readCSVSimple(x, istr, readDateTimeText);
    static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("DateTime", [] { return DataTypePtr(std::make_shared<DataTypeDateTime>()); }, DataTypeFactory::CaseInsensitive);
}


}
