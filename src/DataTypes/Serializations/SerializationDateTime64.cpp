#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>

#include <Columns/ColumnVector.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int CANNOT_PARSE_NUMBER;
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

/// Reads an unquoted numeric DateTime64 as used in containers (arrays/tuples/maps) and JSON.
/// A bare integer is a scaled tick count, e.g. `1783585473954` for scale 3 (unchanged, backward compatible).
/// If a decimal point follows, the integer part is whole seconds and the fraction is subseconds,
/// e.g. `1783585473.954`, matching how scalar columns already parse the fractional unix-timestamp form.
template <typename ReturnType>
static inline ReturnType readNumericTextImpl(DateTime64 & x, UInt32 scale, ReadBuffer & istr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// Reject a leading '+' to stay in parity with scalar DateTime64 basic parsing, which
    /// rejects it (readDateTimeTextFallback only special-cases '-'). readIntText would
    /// silently accept it, so filter it here before reading the whole part. This also
    /// matches the pre-PR container behavior, which parsed elements through readDateTime64Text.
    if (!istr.eof() && *istr.position() == '+')
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number with a leading '+' sign");
        else
            return ReturnType(false);
    }

    bool is_negative = (!istr.eof() && *istr.position() == '-');

    time_t whole = 0;
    if (is_negative)
    {
        /// Consume the sign up front, then decide from the next byte. This is chunk-boundary
        /// safe: unlike a lookahead into the current buffer (istr.available()/position()[1]),
        /// calling istr.eof() after the sign forces a refill, so the byte after '-' is
        /// reliably visible even when '-' was the last byte of the previous chunk.
        ++istr.position();
        if (istr.eof() || (*istr.position() != '.' && !isNumericASCII(*istr.position())))
        {
            /// A lone '-' with neither a fraction nor a magnitude is malformed. readIntText
            /// would have rejected "-<non-digit>" (has_sign && !has_number); preserve that
            /// after pre-consuming the sign so we don't silently parse it as 0.
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number without any digits");
            else
                return ReturnType(false);
        }
        if (*istr.position() == '.')
        {
            /// Bare shorthand `-.123` (sign directly followed by the decimal point, implied
            /// zero whole part): leave whole == 0. adjustFractionalDateTimeSign restores the
            /// sign below. This mirrors the scalar readDateTime64Text path, which also accepts
            /// `-.123`.
        }
        else
        {
            /// Regular negative value: read the unsigned magnitude and re-apply the sign.
            if constexpr (throw_exception)
                readIntText(whole, istr);
            else if (!tryReadIntText(whole, istr))
                return ReturnType(false);
            whole = -whole;
        }
    }
    else if constexpr (throw_exception)
        readIntText(whole, istr);
    else if (!tryReadIntText(whole, istr))
        return ReturnType(false);

    if (istr.eof() || *istr.position() != '.')
    {
        /// No fractional part: the integer is a scaled tick count.
        x = static_cast<DateTime64::NativeType>(whole);
        return ReturnType(true);
    }

    ++istr.position();

    DB::DecimalUtils::DecimalComponents<DateTime64> components{static_cast<DateTime64::NativeType>(whole), 0};

    /// Read digits, up to 'scale' positions.
    for (size_t i = 0; i < scale; ++i)
    {
        if (!istr.eof() && isNumericASCII(*istr.position()))
        {
            components.fractional *= 10;
            components.fractional += *istr.position() - '0';
            ++istr.position();
        }
        else
        {
            /// Adjust to scale.
            components.fractional *= 10;
        }
    }

    /// Ignore digits that are out of precision.
    while (!istr.eof() && isNumericASCII(*istr.position()))
        ++istr.position();

    /// Shared with the scalar readDateTime64Text path so both agree on pre-epoch sub-second signs.
    int negative_fraction_multiplier = adjustFractionalDateTimeSign(components, is_negative, scale);

    if constexpr (throw_exception)
    {
        x = DecimalUtils::decimalFromComponents<DateTime64>(components, scale) * negative_fraction_multiplier;
        return;
    }
    else
    {
        if (!DecimalUtils::tryGetDecimalFromComponents<DateTime64>(components, scale, x))
            return ReturnType(false);
        x *= negative_fraction_multiplier;
        return ReturnType(true);
    }
}

static inline void readNumericText(DateTime64 & x, UInt32 scale, ReadBuffer & istr)
{
    readNumericTextImpl<void>(x, scale, istr);
}

static inline bool tryReadNumericText(DateTime64 & x, UInt32 scale, ReadBuffer & istr)
{
    return readNumericTextImpl<bool>(x, scale, istr);
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
    else /// Just 1504193808 or 01504193808 or 1504193808.808
    {
        readNumericText(x, scale, istr);
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
    else /// Just 1504193808 or 01504193808 or 1504193808.808
    {
        if (!tryReadNumericText(x, scale, istr))
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
        readNumericText(x, scale, istr);
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
        if (!tryReadNumericText(x, scale, istr))
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
