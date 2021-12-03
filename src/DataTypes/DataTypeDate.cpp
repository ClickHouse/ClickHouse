#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeFactory.h>

#include <Common/assert_cast.h>


namespace DB
{

void DataTypeDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(DayNum(assert_cast<const ColumnUInt16 &>(column).getData()[row_num]), ostr);
}

void DataTypeDate::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void DataTypeDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    readDateText(x, istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void DataTypeDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void DataTypeDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void DataTypeDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    assertChar('\'', istr);
    readDateText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void DataTypeDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    assertChar('"', istr);
    readDateText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void DataTypeDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    LocalDate value;
    readCSV(value, istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(value.getDayNum());
}

bool DataTypeDate::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


void registerDataTypeDate(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Date", [] { return DataTypePtr(std::make_shared<DataTypeDate>()); }, DataTypeFactory::CaseInsensitive);
}

}
