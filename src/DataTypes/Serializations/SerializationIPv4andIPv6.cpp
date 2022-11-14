#include <cmath>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{

void SerializationIPv4::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnIPv4 &>(column).getData()[row_num], ostr);
}

void SerializationIPv4::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    IPv4 x;
    readText(x, istr);
    assert_cast<ColumnIPv4 &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "IPv4");
}

void SerializationIPv4::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeText(column, istr, settings, false);
}

void SerializationIPv4::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationIPv4::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationIPv4::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    IPv4 x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnIPv4 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationIPv4::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationIPv4::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    IPv4 x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnIPv4 &>(column).getData().push_back(x);
}

void SerializationIPv4::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationIPv4::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    IPv4 value;
    readCSV(value, istr);
    assert_cast<ColumnIPv4 &>(column).getData().push_back(value);
}


void SerializationIPv4::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    IPv4 x = field.get<IPv4>();
    writeBinary(x, ostr);
}

void SerializationIPv4::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    IPv4 x;
    readBinary(x, istr);
    field = NearestFieldType<IPv4>(x);
}

void SerializationIPv4::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(assert_cast<const ColumnVector<IPv4> &>(column).getData()[row_num], ostr);
}

void SerializationIPv4::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    IPv4 x;
    readBinary(x, istr);
    assert_cast<ColumnVector<IPv4> &>(column).getData().push_back(x);
}

void SerializationIPv4::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<IPv4>::Container & x = typeid_cast<const ColumnVector<IPv4> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(IPv4) * limit);
}

void SerializationIPv4::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<IPv4>::Container & x = typeid_cast<ColumnVector<IPv4> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(IPv4) * limit);
    x.resize(initial_size + size / sizeof(IPv4));
}


void SerializationIPv6::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnIPv6 &>(column).getData()[row_num], ostr);
}

void SerializationIPv6::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    IPv6 x;
    readText(x, istr);
    assert_cast<ColumnIPv6 &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "IPv6");
}

void SerializationIPv6::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeText(column, istr, settings, false);
}

void SerializationIPv6::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationIPv6::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationIPv6::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    IPv6 x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnIPv6 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationIPv6::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationIPv6::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    IPv6 x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnIPv6 &>(column).getData().push_back(x);
}

void SerializationIPv6::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationIPv6::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    IPv6 value;
    readCSV(value, istr);
    assert_cast<ColumnIPv6 &>(column).getData().push_back(value);
}


void SerializationIPv6::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    IPv6 x = field.get<IPv6>();
    writeBinary(x, ostr);
}

void SerializationIPv6::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    IPv6 x;
    readBinary(x, istr);
    field = NearestFieldType<IPv6>(x);
}

void SerializationIPv6::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(assert_cast<const ColumnVector<IPv6> &>(column).getData()[row_num], ostr);
}

void SerializationIPv6::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    IPv6 x;
    readBinary(x, istr);
    assert_cast<ColumnVector<IPv6> &>(column).getData().push_back(x);
}

void SerializationIPv6::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<IPv6>::Container & x = typeid_cast<const ColumnVector<IPv6> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(IPv6) * limit);
}

void SerializationIPv6::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<IPv6>::Container & x = typeid_cast<ColumnVector<IPv6> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(IPv6) * limit);
    x.resize(initial_size + size / sizeof(IPv6));
}

}
