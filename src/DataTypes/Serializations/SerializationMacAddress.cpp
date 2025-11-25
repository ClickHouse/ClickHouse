#include <Columns/ColumnVector.h>
#include <DataTypes/Serializations/SerializationMacAddress.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>

namespace DB
{

void SerializationMacAddress::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnVector<MacAddress> &>(column).getData()[row_num], ostr);
}

void SerializationMacAddress::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    MacAddress x;
    readText(x, istr);

    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, {TypeName<MacAddress>.data(), TypeName<MacAddress>.size()});
}

bool SerializationMacAddress::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    MacAddress x;
    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);
    return true;
}

void SerializationMacAddress::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationMacAddress::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    MacAddress x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationMacAddress::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    MacAddress x;
    if (!checkChar('\'', istr) || !tryReadText(x, istr) || !checkChar('\'', istr))
        return false;
    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);
    return true;
}

void SerializationMacAddress::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationMacAddress::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    MacAddress x;
    assertChar('"', istr);
    readText(x, istr);
    /// this code looks weird, but we want to throw specific exception to match original behavior...
    if (istr.eof())
        assertChar('"', istr);
    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);
    if (*istr.position() != '"')
        throwUnexpectedDataAfterParsedValue(column, istr, settings, {TypeName<MacAddress>.data(), TypeName<MacAddress>.size()});
    istr.ignore();
}

bool SerializationMacAddress::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    MacAddress x;
    if (!checkChar('"', istr) || !tryReadText(x, istr) || !checkChar('"', istr))
        return false;

    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);
    return true;
}

void SerializationMacAddress::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationMacAddress::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    MacAddress value;
    readCSV(value, istr);

    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(value);
}

bool SerializationMacAddress::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    MacAddress value;
    if (!tryReadCSV(value, istr))
        return false;

    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(value);
    return true;
}

void SerializationMacAddress::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto x = field.safeGet<MacAddress>();
    writeBinary(x.toUInt64(), ostr);
}

void SerializationMacAddress::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    UInt64 u;
    readBinary(u, istr);
    field = NearestFieldType<MacAddress>(MacAddress(u));
}

void SerializationMacAddress::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinary(assert_cast<const ColumnVector<MacAddress> &>(column).getData()[row_num], ostr);
}

void SerializationMacAddress::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    MacAddress x;
    readBinary(x.toUnderType(), istr);
    assert_cast<ColumnVector<MacAddress> &>(column).getData().push_back(x);
}

void SerializationMacAddress::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const auto & x = typeid_cast<const ColumnVector<MacAddress> &>(column).getData();

    const auto size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit == 0)
        return;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(MacAddress) * limit);
}

void SerializationMacAddress::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t /*rows_offset*/, size_t limit, double /*avg_value_size_hint*/) const
{
    auto & x = typeid_cast<ColumnVector<MacAddress> &>(column).getData();
    const auto initial_size = x.size();
    x.resize(initial_size + limit);
    const auto size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(MacAddress) * limit);
    x.resize(initial_size + size / sizeof(MacAddress));
}

}

