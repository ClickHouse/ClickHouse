#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{

void DataTypeUUID::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(UUID(assert_cast<const ColumnUInt128 &>(column).getData()[row_num]), ostr);
}

void DataTypeUUID::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readText(x, istr);
    assert_cast<ColumnUInt128 &>(column).getData().push_back(x);
}

void DataTypeUUID::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeText(column, istr, settings);
}

void DataTypeUUID::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void DataTypeUUID::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void DataTypeUUID::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnUInt128 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void DataTypeUUID::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeUUID::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnUInt128 &>(column).getData().push_back(x);
}

void DataTypeUUID::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeUUID::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    readCSV(value, istr);
    assert_cast<ColumnUInt128 &>(column).getData().push_back(value);
}

void DataTypeUUID::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    value_index = static_cast<bool>(protobuf.writeUUID(UUID(assert_cast<const ColumnUInt128 &>(column).getData()[row_num])));
}

void DataTypeUUID::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    UUID uuid;
    if (!protobuf.readUUID(uuid))
        return;

    auto & container = assert_cast<ColumnUInt128 &>(column).getData();
    if (allow_add_row)
    {
        container.emplace_back(uuid);
        row_added = true;
    }
    else
        container.back() = uuid;
}

bool DataTypeUUID::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


void registerDataTypeUUID(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UUID", [] { return DataTypePtr(std::make_shared<DataTypeUUID>()); });
}

}
