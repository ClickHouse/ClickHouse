#include <DataTypes/Serializations/SerializationUUID.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{

void SerializationUUID::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnUUID &>(column).getData()[row_num], ostr);
}

void SerializationUUID::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    UUID x;
    readText(x, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "UUID");
}

void SerializationUUID::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeText(column, istr, settings, false);
}

void SerializationUUID::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationUUID::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationUUID::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationUUID::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);
}

void SerializationUUID::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    readCSV(value, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(value);
}


void SerializationUUID::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    UUID x = get<UUID>(field);
    writeBinary(x, ostr);
}

void SerializationUUID::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UUID x;
    readBinary(x, istr);
    field = NearestFieldType<UUID>(x);
}

void SerializationUUID::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(assert_cast<const ColumnVector<UUID> &>(column).getData()[row_num], ostr);
}

void SerializationUUID::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    UUID x;
    readBinary(x, istr);
    assert_cast<ColumnVector<UUID> &>(column).getData().push_back(x);
}

void SerializationUUID::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<UUID>::Container & x = typeid_cast<const ColumnVector<UUID> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(UUID) * limit);
}

void SerializationUUID::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<UUID>::Container & x = typeid_cast<ColumnVector<UUID> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(UUID) * limit);
    x.resize(initial_size + size / sizeof(UUID));
}

}
