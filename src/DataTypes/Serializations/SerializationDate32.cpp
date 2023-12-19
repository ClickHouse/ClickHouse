#include <DataTypes/Serializations/SerializationDate32.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

namespace DB
{

void SerializationDate32::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(ExtendedDayNum(assert_cast<const ColumnInt32 &>(column).getData()[row_num]), ostr, time_zone);
}

void SerializationDate32::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Date32");
}

bool SerializationDate32::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone) || !istr.eof())
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate32::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    readDateText(x, istr, time_zone);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

bool SerializationDate32::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone))
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate32::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate32::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate32::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    assertChar('\'', istr);
    readDateText(x, istr, time_zone);
    assertChar('\'', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationDate32::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    if (!checkChar('\'', istr) || !tryReadDateText(x, istr, time_zone) || !checkChar('\'', istr))
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
    return true;
}

void SerializationDate32::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate32::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    assertChar('"', istr);
    readDateText(x, istr, time_zone);
    assertChar('"', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

bool SerializationDate32::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ExtendedDayNum x;
    if (!checkChar('"', istr) || !tryReadDateText(x, istr, time_zone) || !checkChar('"', istr))
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate32::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate32::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    LocalDate value;
    readCSV(value, istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
}

bool SerializationDate32::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    LocalDate value;
    if (!tryReadCSV(value, istr))
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
    return true;
}

SerializationDate32::SerializationDate32(const DateLUTImpl & time_zone_) : time_zone(time_zone_)
{
}
}
