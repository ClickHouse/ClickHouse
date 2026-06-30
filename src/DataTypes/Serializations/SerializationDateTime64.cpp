#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>

#include <Columns/ColumnVector.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/readDecimalText.h>
#include <Common/assert_cast.h>
#include <base/arithmeticOverflow.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int DECIMAL_OVERFLOW;
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

/// An unquoted number in JSON or the Quoted format is a Unix timestamp (seconds since the epoch)
/// with optional sub-second precision, e.g. `1703363853.035`. It is parsed as a decimal value at
/// the column scale, exactly like `CAST`, `toDateTime64`, the `Values` format and the `Decimal`
/// type (`DateTime64` is a `Decimal` underneath). As with the previous `readIntText`, parsing stops
/// at the first character that is not part of the number (e.g. the `,` or `}` that follows the value
/// in JSON).
///
/// The number is accumulated into a 128-bit temporary rather than directly into the `Int64` ticks of
/// `DateTime64`. A `DateTime64` spans the whole `Int64` range, so an input near (or beyond) the
/// `Int64` boundary would otherwise wrap around silently during accumulation; the wide temporary lets
/// us range-check the final value and report `DECIMAL_OVERFLOW` instead. A 128-bit accumulator holds
/// up to `max_precision<Decimal128>` (38) decimal digits, which covers the whole valid `DateTime64`
/// range at every scale.
static constexpr UInt32 datetime64_number_precision = DecimalUtils::max_precision<Decimal128>;

/// Applies the `unread_scale` decimal places still pending after `readDecimalText` to scale the
/// parsed `value` to ticks, then writes it into `x`. Returns false on overflow instead of throwing.
static bool scaleAndStoreDateTime64(DateTime64 & x, Int128 value, UInt32 unread_scale)
{
    if (common::mulOverflow(value, DecimalUtils::scaleMultiplier<Int128>(unread_scale), value)
        || value > std::numeric_limits<DateTime64::NativeType>::max()
        || value < std::numeric_limits<DateTime64::NativeType>::min())
        return false;
    x.value = static_cast<DateTime64::NativeType>(value);
    return true;
}

static void readDateTime64AsNumber(DateTime64 & x, UInt32 scale, ReadBuffer & istr)
{
    Decimal128 tmp;
    UInt32 unread_scale = scale;
    readDecimalText(istr, tmp, datetime64_number_precision, unread_scale);
    if (!scaleAndStoreDateTime64(x, tmp.value, unread_scale))
        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric value is out of range for DateTime64");
}

static bool tryReadDateTime64AsNumber(DateTime64 & x, UInt32 scale, ReadBuffer & istr)
{
    Decimal128 tmp;
    UInt32 unread_scale = scale;
    if (!readDecimalText<Decimal128, bool>(istr, tmp, datetime64_number_precision, unread_scale, /*digits_only=*/false))
        return false;
    return scaleAndStoreDateTime64(x, tmp.value, unread_scale);
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

void SerializationDateTime64::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('"', istr))
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else if (settings.json.read_datetime64_number_as_raw_value)
    {
        readIntText(x, istr);
    }
    else
    {
        readDateTime64AsNumber(x, scale, istr);
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
    else if (settings.json.read_datetime64_number_as_raw_value)
    {
        if (!tryReadIntText(x, istr))
            return false;
    }
    else
    {
        if (!tryReadDateTime64AsNumber(x, scale, istr))
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
