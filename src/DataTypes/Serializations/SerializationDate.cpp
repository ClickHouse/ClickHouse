#include <DataTypes/Serializations/SerializationDate.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

namespace DB
{

void SerializationDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(DayNum(assert_cast<const ColumnUInt16 &>(column).getData()[row_num]), ostr, time_zone);
}

void SerializationDate::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Date");
}

bool SerializationDate::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    if (!tryReadDateText(x, istr, time_zone) || !istr.eof())
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    readDateText(x, istr, time_zone);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

bool SerializationDate::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    if (!tryReadDateText(x, istr, time_zone))
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    assertChar('\'', istr);
    readDateText(x, istr, time_zone);
    assertChar('\'', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationDate::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    if (!checkChar('\'', istr) || !tryReadDateText(x, istr, time_zone) || !checkChar('\'', istr))
        return false;

    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    assertChar('"', istr);
    readDateText(x, istr, time_zone);
    assertChar('"', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

bool SerializationDate::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum x;
    if (!checkChar('"', istr) || !tryReadDateText(x, istr, time_zone) || !checkChar('"', istr))
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum value;
    readCSV(value, istr, time_zone);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(value);
}

bool SerializationDate::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    DayNum value;
    if (!tryReadCSV(value, istr, time_zone))
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(value);
    return true;
}

SerializationDate::SerializationDate(const DateLUTImpl & time_zone_) : time_zone(time_zone_)
{
}

}
