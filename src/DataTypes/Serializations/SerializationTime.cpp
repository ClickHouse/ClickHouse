#include <DataTypes/Serializations/SerializationDateTime.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeTime.h>
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

namespace ErrorCodes
{
extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
}

inline void readTimeText(
    time_t & x,
    ReadBuffer & istr,
    const FormatSettings & /*settings*/,
    const DateLUTImpl & time_zone,
    const DateLUTImpl & /*utc_time_zone*/)
{
    readTimeTextImpl<>(x, istr, time_zone);

    x = std::max<time_t>(0, x);
}

inline bool tryReadTimeText(
    time_t & x,
    ReadBuffer & istr,
    const FormatSettings & /*settings*/,
    const DateLUTImpl & time_zone,
    const DateLUTImpl & /*utc_time_zone*/)
{
    bool res;
    res = tryReadTimeText(x, istr, time_zone);

    x = std::max<time_t>(0, x);
    return res;
}

SerializationTime::SerializationTime(const DataTypeTime & /*time_type*/)
{
}

void SerializationTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const
{
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    writeTimeText(value, ostr);
}

void SerializationTime::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Time");
}

bool SerializationTime::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    if (!tryReadTimeText(x, istr) || !istr.eof())
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
    return true;
}

void SerializationTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    readTimeText(x, istr);
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
}

bool SerializationTime::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    if (!tryReadTimeText(x, istr))
        return false;
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
    return true;
}

void SerializationTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    if (checkChar('\'', istr)) /// Cases: '18:36:48' or '493808'
    {
        readTimeText(x, istr);
        assertChar('\'', istr);
    }
    else /// Just 493808 or 0493808
    {
        readIntText(x, istr);
    }
    /// It's important to do this at the end - for exception safety.
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
}

bool SerializationTime::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    if (checkChar('\'', istr)) /// Cases: '18:36:48' or '123808'
    {
        if (!tryReadTimeText(x, istr) || !checkChar('\'', istr))
            return false;
    }
    else
    {
        if (!tryReadIntText(x, istr))
            return false;
    }

    /// It's important to do this at the end - for exception safety.
    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
    return true;
}

void SerializationTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    if (checkChar('"', istr))
    {
        readTimeText(x, istr);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
}

bool SerializationTime::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    time_t x = 0;
    if (checkChar('"', istr))
    {
        if (!tryReadTimeText(x, istr) || !checkChar('"', istr))
            return false;
    }
    else
    {
        if (!tryReadIntText(x, istr))
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
    return true;
}

void SerializationTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '"')
    {
        ++istr.position();
        readTimeText(x, istr);
        assertChar(maybe_quote, istr);
    }
    else
    {
        String time_str;
        readCSVString(time_str, istr, settings.csv);
        ReadBufferFromString buf(time_str);
        readTimeText(x, buf);
        if (!buf.eof())
                throw Exception(
                    ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
                    "Unexpected data '{}' after parsed Time value '{}'",
                    String(buf.position(), buf.buffer().end()),
                    String(buf.buffer().begin(), buf.position()));
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
}

bool SerializationTime::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    time_t x = 0;

    if (istr.eof())
        return false;

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '"')
    {
        ++istr.position();
        if (!tryReadTimeText(x, istr) || !checkChar(maybe_quote, istr))
            return false;
    }
    else
    {
        String time_str;
        readCSVString(time_str, istr, settings.csv);
        ReadBufferFromString buf(time_str);
        if (!tryReadTimeText(x, buf) || !buf.eof())
            return false;
    }

    assert_cast<ColumnType &>(column).getData().push_back(static_cast<Int32>(x));
    return true;
}

}
