#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/ProtobufWriter.h>


namespace DB
{

void DataTypeDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(DayNum(static_cast<const ColumnUInt16 &>(column).getData()[row_num]), ostr);
}

void DataTypeDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    readDateText(x, istr);
    static_cast<ColumnUInt16 &>(column).getData().push_back(x);
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
    static_cast<ColumnUInt16 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
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
    static_cast<ColumnUInt16 &>(column).getData().push_back(x);
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
    static_cast<ColumnUInt16 &>(column).getData().push_back(value.getDayNum());
}

void DataTypeDate::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf) const
{
    protobuf.writeDate(DayNum(static_cast<const ColumnUInt16 &>(column).getData()[row_num]));
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
