#include <DataTypes/Serializations/SerializationDateTime64.h>

#include <Columns/ColumnVector.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Common/DateLUT.h>
#include <Common/assert_cast.h>

namespace DB
{

SerializationDateTime64::SerializationDateTime64(
    UInt32 scale_, const TimezoneMixin & time_zone_)
    : SerializationDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_)
    , TimezoneMixin(time_zone_)
{
}

void SerializationDateTime64::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    switch (settings.date_time_output_format)
    {
        case FormatSettings::DateTimeOutputFormat::Simple:
            if (settings.date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands)
                writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(value, scale, ostr, time_zone);
            else
                writeDateTimeText(value, scale, ostr, time_zone);
            return;
        case FormatSettings::DateTimeOutputFormat::UnixTimestamp:
            writeDateTimeUnixTimestamp(value, scale, ostr);
            return;
        case FormatSettings::DateTimeOutputFormat::ISO:
            writeDateTimeTextISO(value, scale, ostr, utc_time_zone);
            return;
    }
}

void SerializationDateTime64::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    DateTime64 result = 0;
    readDateTime64Text(result, scale, istr, time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(result);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "DateTime64");
}

bool SerializationDateTime64::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    DateTime64 result = 0;
    if (!tryReadDateTime64Text(result, scale, istr, time_zone) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnType &>(column).getData().push_back(result);
    return true;
}

void SerializationDateTime64::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "DateTime64");
}

void SerializationDateTime64::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

static inline void readText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTime64Text(x, scale, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTime64BestEffort(x, scale, istr, time_zone, utc_time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            parseDateTime64BestEffortUS(x, scale, istr, time_zone, utc_time_zone);
            return;
    }
}

static inline bool tryReadText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            return tryReadDateTime64Text(x, scale, istr, time_zone);
        case FormatSettings::DateTimeInputFormat::BestEffort:
            return tryParseDateTime64BestEffort(x, scale, istr, time_zone, utc_time_zone);
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            return tryParseDateTime64BestEffortUS(x, scale, istr, time_zone, utc_time_zone);
    }
}


bool SerializationDateTime64::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (!tryReadText(x, scale, istr, settings, time_zone, utc_time_zone) || !istr.eof())
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

void SerializationDateTime64::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    readText(x, scale, istr, settings, time_zone, utc_time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

bool SerializationDateTime64::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (!tryReadText(x, scale, istr, settings, time_zone, utc_time_zone))
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

void SerializationDateTime64::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDateTime64::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationDateTime64::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        if (!tryReadText(x, scale, istr, settings, time_zone, utc_time_zone) || !checkChar('\'', istr))
            return false;
    }
    else /// Just 1504193808 or 01504193808
    {
        if (!tryReadIntText(x, istr))
            return false;
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
    return true;
}

void SerializationDateTime64::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime64::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('"', istr))
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

bool SerializationDateTime64::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('"', istr))
    {
        if (!tryReadText(x, scale, istr, settings, time_zone, utc_time_zone) || !checkChar('"', istr))
            return false;
    }
    else
    {
        if (!tryReadIntText(x, istr))
            return false;
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

void SerializationDateTime64::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime64::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        ++istr.position();
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar(maybe_quote, istr);
    }
    else
    {
        if (settings.csv.delimiter != ',' || settings.date_time_input_format == FormatSettings::DateTimeInputFormat::Basic)
        {
            readText(x, scale, istr, settings, time_zone, utc_time_zone);
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
            readText(x, scale, buf, settings, time_zone, utc_time_zone);
        }
    }

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

bool SerializationDateTime64::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;

    if (istr.eof())
        return false;

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        ++istr.position();
        if (!tryReadText(x, scale, istr, settings, time_zone, utc_time_zone) || !checkChar(maybe_quote, istr))
            return false;
    }
    else
    {
        if (settings.csv.delimiter != ',' || settings.date_time_input_format == FormatSettings::DateTimeInputFormat::Basic)
        {
            if (!tryReadText(x, scale, istr, settings, time_zone, utc_time_zone))
                return false;
        }
        else
        {
            String datetime_str;
            readCSVString(datetime_str, istr, settings.csv);
            ReadBufferFromString buf(datetime_str);
            if (!tryReadText(x, scale, buf, settings, time_zone, utc_time_zone) || !buf.eof())
                return false;
        }
    }

    assert_cast<ColumnType &>(column).getData().push_back(x);
    return true;
}

}
