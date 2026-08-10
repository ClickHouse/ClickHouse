#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>

#include <Columns/ColumnVector.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
}

SerializationDateTime64::SerializationDateTime64(
    UInt32 scale_, const TimezoneMixin & time_zone_)
    : SerializationDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_)
    , TimezoneMixin(time_zone_)
{
}

UInt128 SerializationDateTime64::getHash(UInt32 scale_, const TimezoneMixin & time_zone_)
{
    SipHash hash;
    hash.update("DateTime64");
    hash.update(scale_);
    auto tz = time_zone_.getTimeZone().getTimeZone();
    hash.update(tz.size());
    hash.update(tz);
    hash.update(time_zone_.hasExplicitTimeZone());
    return hash.get128();
}

void SerializationDateTime64::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// Hive timestamps are always the simple `yyyy-MM-dd HH:mm:ss.fffffffff` text, regardless of `date_time_output_format`.
    /// Delegating to `serializeText` would honor that setting and could emit epoch seconds (`unix_timestamp`) or
    /// `T...Z` (`iso`), which Hive cannot parse as a `TIMESTAMP`.
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    if (settings.date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands)
        writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(value, scale, ostr, time_zone);
    else
        writeDateTimeText(value, scale, ostr, time_zone);
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

SerializationPtr SerializationDateTime64::create(UInt32 scale_, const TimezoneMixin & time_zone_)
{
    return ISerialization::pooled(getHash(scale_, time_zone_), [&] { return new SerializationDateTime64(scale_, time_zone_); });
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
    else if (settings.read_datetime_number_as_raw_value) /// Legacy: the raw scaled value (ticks).
    {
        readDateTime64AsRawValue(x, istr);
    }
    else /// Just 1504193808 or 1703363853.035 (a Unix timestamp, possibly with sub-second precision)
    {
        readDateTime64AsNumber(x, scale, istr);
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
    else if (settings.read_datetime_number_as_raw_value) /// Legacy: the raw scaled value (ticks).
    {
        if (!tryReadDateTime64AsRawValue(x, istr))
            return false;
    }
    else /// Just 1504193808 or 1703363853.035 (a Unix timestamp, possibly with sub-second precision)
    {
        if (!tryReadDateTime64AsNumber(x, scale, istr))
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

/// checkString() consumes matched bytes even on a partial mismatch, so the caller must roll back
/// (via a PeekableReadBuffer checkpoint) on a false return before trying anything else.
static bool checkISODatePrefix(ReadBuffer & istr)
{
    if (istr.eof())
        return false;
    if (*istr.position() == 'n')
        return checkString("new ISODate(", istr);
    if (*istr.position() == 'I')
        return checkString("ISODate(", istr);
    return false;
}

/// Not valid JSON but accept it as mongodb shell syntax to parse inner string
/// Case: ISODate("2024-05-29T23:16:12.256") or new ISODate("2024-05-29T23:16:12.256Z")
template <typename ReturnType>
static ReturnType deserializeISODateJSON(
    DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings,
    const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    String inner;
    if constexpr (throw_exception)
        readJSONString(inner, istr, settings.json);
    else if (!tryReadJSONStringInto(inner, istr, settings.json))
        return ReturnType(false);

    if constexpr (throw_exception)
        assertChar(')', istr);
    else if (!checkChar(')', istr))
        return ReturnType(false);

    /// 'basic' has no notion of a 'Z' suffix, unlike 'best_effort'/'best_effort_us' which consume it
    /// themselves. Strip it upfront and force UTC, otherwise it'd parse in the column time zone.
    const bool basic_format = settings.date_time_input_format == FormatSettings::DateTimeInputFormat::Basic;
    const bool has_trailing_z = basic_format && !inner.empty() && inner.back() == 'Z';
    if (has_trailing_z)
        inner.pop_back();

    ReadBufferFromString buf(inner);
    if constexpr (throw_exception)
        readText(x, scale, buf, settings, has_trailing_z ? utc_time_zone : time_zone, utc_time_zone);
    else if (!tryReadText(x, scale, buf, settings, has_trailing_z ? utc_time_zone : time_zone, utc_time_zone))
        return ReturnType(false);

    /// Consume a 'Z' left behind by 'best_effort'/'best_effort_us'; skip for 'basic' since it was
    /// already stripped above, so anything left is malformed (e.g. a second 'Z' in "...256ZZ").
    if (!has_trailing_z && !buf.eof() && *buf.position() == 'Z')
        ++buf.position();

    if (!buf.eof())
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
                "Unexpected data after parsed DateTime64 value inside ISODate wrapper");
        return ReturnType(false);
    }

    return ReturnType(true);
}

/// The non-ISODate part of a non-quoted JSON value: either the legacy raw scaled value (ticks) or a
/// Unix timestamp, possibly with sub-second precision.
template <typename ReturnType>
static ReturnType deserializeNumberJSON(DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if constexpr (throw_exception)
    {
        if (settings.read_datetime_number_as_raw_value) /// Legacy: the raw scaled value (ticks).
            readDateTime64AsRawValue(x, istr);
        else
            readDateTime64AsNumber(x, scale, istr);
    }
    else
    {
        if (settings.read_datetime_number_as_raw_value) /// Legacy: the raw scaled value (ticks).
            return ReturnType(tryReadDateTime64AsRawValue(x, istr));
        return ReturnType(tryReadDateTime64AsNumber(x, scale, istr));
    }
}

/// Handles the non-quoted JSON cases: a numeric timestamp, or the mongodb shell syntax
/// ISODate("...") / new ISODate("..."). Uses PeekableReadBuffer so a malformed near-miss like
/// "ISODate123" rolls back instead of falling through to numeric parsing on "123".
/// The wrapper syntax is recognized regardless of `input_format_read_datetime_number_as_raw_value`;
/// that setting governs only how an actual number is interpreted.
template <typename ReturnType>
static ReturnType deserializeNonQuotedJSON(
    DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings,
    const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// A number is by far the common case; avoid PeekableReadBuffer's allocation for it.
    if (istr.eof() || (*istr.position() != 'n' && *istr.position() != 'I'))
    {
        if constexpr (throw_exception)
        {
            deserializeNumberJSON<void>(x, scale, istr, settings);
            return;
        }
        else
            return ReturnType(deserializeNumberJSON<bool>(x, scale, istr, settings));
    }

    PeekableReadBuffer peekable_buf(istr, true);
    peekable_buf.setCheckpoint();
    SCOPE_EXIT(peekable_buf.dropCheckpoint());

    if (checkISODatePrefix(peekable_buf))
    {
        if constexpr (throw_exception)
            deserializeISODateJSON<void>(x, scale, peekable_buf, settings, time_zone, utc_time_zone);
        else if (!deserializeISODateJSON<bool>(x, scale, peekable_buf, settings, time_zone, utc_time_zone))
            return ReturnType(false);
        return ReturnType(true);
    }

    peekable_buf.rollbackToCheckpoint();
    if constexpr (throw_exception)
        deserializeNumberJSON<void>(x, scale, peekable_buf, settings);
    else if (!deserializeNumberJSON<bool>(x, scale, peekable_buf, settings))
        return ReturnType(false);
    return ReturnType(true);
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
        deserializeNonQuotedJSON<void>(x, scale, istr, settings, time_zone, utc_time_zone);
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
    else if (!deserializeNonQuotedJSON<bool>(x, scale, istr, settings, time_zone, utc_time_zone))
    {
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
            if (!buf.eof())
                throw Exception(
                    ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE,
                    "Unexpected data '{}' after parsed DateTime64 value '{}'",
                    String(buf.position(), buf.buffer().end()),
                    String(buf.buffer().begin(), buf.position()));
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
