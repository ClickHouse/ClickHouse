#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDate32.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DATE;
}

UInt128 SerializationDate32::getHash(const DateLUTImpl & time_zone_)
{
    SipHash hash;
    hash.update("Date32");
    const auto & tz = time_zone_.getTimeZone();
    hash.update(tz.size());
    hash.update(tz);
    return hash.get128();
}

void SerializationDate32::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

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

bool SerializationDate32::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw) || !istr.eof())
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate32::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    readDateText(x, istr, time_zone, settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

bool SerializationDate32::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw))
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

void SerializationDate32::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    assertChar('\'', istr);
    readDateText(x, istr, time_zone, settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw);
    assertChar('\'', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationDate32::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    if (!checkChar('\'', istr) || !tryReadDateText(x, istr, time_zone, nullptr, settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw) || !checkChar('\'', istr))
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

void SerializationDate32::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const
{
    if (!checkChar('"', istr))
    {
        SerializationNumber<Int32>::deserializeTextJSON(column, istr, format_settings);
        return;
    }
    ExtendedDayNum x;
    readDateText(x, istr, time_zone, format_settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw);
    assertChar('"', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

bool SerializationDate32::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const
{
    if (!checkChar('"', istr))
        return SerializationNumber<Int32>::tryDeserializeTextJSON(column, istr, format_settings);

    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, format_settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Throw) || !checkChar('"', istr))
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

void SerializationDate32::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    LocalDate value;
    readCSV(value, istr);
    /// This one goes through `LocalDate`, which accepts a calendar-invalid date and resolves it to a default
    if (settings.date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw
        && !tryToMakeDayNum(time_zone, value.year(), value.month(), value.day()))
        throw Exception(ErrorCodes::CANNOT_PARSE_DATE, "Cannot parse date");
    assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
}

bool SerializationDate32::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    LocalDate value;
    if (!tryReadCSV(value, istr))
        return false;
    if (settings.date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw
        && !tryToMakeDayNum(time_zone, value.year(), value.month(), value.day()))
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
    return true;
}

SerializationDate32::SerializationDate32(const DateLUTImpl & time_zone_) : time_zone(time_zone_)
{
}

SerializationPtr SerializationDate32::create(const DateLUTImpl & time_zone_)
{
    return ISerialization::pooled(getHash(time_zone_), [&] { return new SerializationDate32(time_zone_); });
}

}
