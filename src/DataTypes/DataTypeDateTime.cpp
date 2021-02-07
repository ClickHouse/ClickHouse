#include <DataTypes/DataTypeDateTime.h>

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <common/DateLUT.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Parsers/ASTLiteral.h>

namespace
{
using namespace DB;
inline void readText(time_t & x, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTimeText(x, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTimeBestEffort(x, istr, time_zone, utc_time_zone);
            return;
    }
}
}

namespace DB
{

TimezoneMixin::TimezoneMixin(const String & time_zone_name)
    : has_explicit_time_zone(!time_zone_name.empty()),
    time_zone(DateLUT::instance(time_zone_name)),
    utc_time_zone(DateLUT::instance("UTC"))
{}

DataTypeDateTime::DataTypeDateTime(const String & time_zone_name)
    : TimezoneMixin(time_zone_name)
{
}

DataTypeDateTime::DataTypeDateTime(const TimezoneMixin & time_zone_)
    : TimezoneMixin(time_zone_)
{}

String DataTypeDateTime::doGetName() const
{
    if (!has_explicit_time_zone)
        return "DateTime";

    WriteBufferFromOwnString out;
    out << "DateTime(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void DataTypeDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
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

void DataTypeDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void DataTypeDateTime::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
}

void DataTypeDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;
    ::readText(x, istr, settings, time_zone, utc_time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void DataTypeDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        ::readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void DataTypeDateTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;
    if (checkChar('"', istr))
    {
        ::readText(x, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    ::readText(x, istr, settings, time_zone, utc_time_zone);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;

    // On some platforms `time_t` is `long` but not `unsigned int` (UInt32 that we store in column), hence static_cast.
    value_index = static_cast<bool>(protobuf.writeDateTime(static_cast<time_t>(assert_cast<const ColumnType &>(column).getData()[row_num])));
}

void DataTypeDateTime::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    time_t t;
    if (!protobuf.readDateTime(t))
        return;

    auto & container = assert_cast<ColumnType &>(column).getData();
    if (allow_add_row)
    {
        container.emplace_back(t);
        row_added = true;
    }
    else
        container.back() = t;
}

bool DataTypeDateTime::equals(const IDataType & rhs) const
{
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}

}
