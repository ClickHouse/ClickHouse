#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Columns/ColumnVector.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Common/DateLUT.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace
{

inline void readText(time_t & x, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTimeTextImpl<>(x, istr, time_zone);
            break;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTimeBestEffort(x, istr, time_zone, utc_time_zone);
            break;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            parseDateTimeBestEffortUS(x, istr, time_zone, utc_time_zone);
            break;
    }

    x = std::max<time_t>(0, x);
}

inline void readAsIntText(time_t & x, ReadBuffer & istr)
{
    readIntText(x, istr);
    x = std::max<time_t>(0, x);
}

inline bool tryReadText(time_t & x, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    bool res;
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            res = tryReadDateTimeText(x, istr, time_zone);
            break;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            res = tryParseDateTimeBestEffort(x, istr, time_zone, utc_time_zone);
            break;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            res = tryParseDateTimeBestEffortUS(x, istr, time_zone, utc_time_zone);
            break;
    }

    x = std::max<time_t>(0, x);
    return res;
}

inline bool tryReadAsIntText(time_t & x, ReadBuffer & istr)
{
    if (!tryReadIntText(x, istr))
        return false;
    x = std::max<time_t>(0, x);
    return true;
}

}

SerializationDateTime::SerializationDateTime(const TimezoneMixin & time_zone_)
    : TimezoneMixin(time_zone_)
{
}

void SerializationDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    switch (settings.date_time_output_format)
    {
        case FormatSettings::DateTimeOutputFormat::Simple:
            writeDateTimeText(value, ostr, time_zone);
            return;
        case FormatSettings::DateTimeOutputFormat::UnixTimestamp:
            writeIntText(value, ostr);
            return;
        case FormatSettings::DateTimeOutputFormat::ISO:
            writeDateTimeTextISO(value, ostr, utc_time_zone);
            return;
    }
}

void SerializationDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDateTime::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "DateTime");
}

bool SerializationDateTime::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (!tryReadText(x, istr, settings, time_zone, utc_time_zone) || !istr.eof())
        return false;

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
    return true;
}

void SerializationDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    readText(x, istr, settings, time_zone, utc_time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
}

bool SerializationDateTime::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (!tryReadText(x, istr, settings, time_zone, utc_time_zone))
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
    return true;
}

void SerializationDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readAsIntText(x, istr);
    }

    /// It's important to do this at the end - for exception safety.
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
}

bool SerializationDateTime::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        if (!tryReadText(x, istr, settings, time_zone, utc_time_zone) || !checkChar('\'', istr))
            return false;
    }
    else /// Just 1504193808 or 01504193808
    {
        if (!tryReadAsIntText(x, istr))
            return false;
    }

    /// It's important to do this at the end - for exception safety.
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
    return true;
}

void SerializationDateTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (checkChar('"', istr))
    {
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readAsIntText(x, istr);
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
}

bool SerializationDateTime::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;
    if (checkChar('"', istr))
    {
        if (!tryReadText(x, istr, settings, time_zone, utc_time_zone) || !checkChar('"', istr))
            return false;
    }
    else
    {
        if (!tryReadIntText(x, istr))
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
    return true;
}

void SerializationDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        ++istr.position();
        readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar(maybe_quote, istr);
    }
    else
    {
        if (settings.csv.delimiter != ',' || settings.date_time_input_format == FormatSettings::DateTimeInputFormat::Basic)
        {
            readText(x, istr, settings, time_zone, utc_time_zone);
        }
        /// Best effort parsing supports datetime in format like "01.01.2000, 00:00:00"
        /// and can mistakenly read comma as a part of datetime.
        /// For example data "...,01.01.2000,some string,..." cannot be parsed correctly.
        /// To fix this problem we first read CSV string and then try to parse it as datetime.
        else
        {
            String datetime_str;
            readCSVString(datetime_str, istr, settings.csv);
            ReadBufferFromString buf(datetime_str);
            readText(x, buf, settings, time_zone, utc_time_zone);
            if (!buf.eof())
                throwUnexpectedDataAfterParsedValue(column, istr, settings, "DateTime");
        }
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
}

bool SerializationDateTime::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;

    if (istr.eof())
        return false;

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        ++istr.position();
        if (!tryReadText(x, istr, settings, time_zone, utc_time_zone) || !checkChar(maybe_quote, istr))
            return false;
    }
    else
    {
        if (settings.csv.delimiter != ',' || settings.date_time_input_format == FormatSettings::DateTimeInputFormat::Basic)
        {
            if (!tryReadText(x, istr, settings, time_zone, utc_time_zone))
                return false;
        }
        else
        {
            String datetime_str;
            readCSVString(datetime_str, istr, settings.csv);
            ReadBufferFromString buf(datetime_str);
            if (!tryReadText(x, buf, settings, time_zone, utc_time_zone) || !buf.eof())
                return false;
        }
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<UInt32>(x));
    return true;
}

}
