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
///
/// The dotted fractional / signed / shorthand forms are a Basic-parser feature: scalar DateTime64
/// accepts them only via readDateTime64Text under date_time_input_format = 'basic', while best_effort
/// routes through parseDateTime64BestEffort, which rejects them. This helper honours the same setting
/// so the effective parser depends on date_time_input_format, not on quoting/nesting.
template <typename ReturnType>
static inline ReturnType readNumericTextImpl(DateTime64 & x, UInt32 scale, ReadBuffer & istr, FormatSettings::DateTimeInputFormat input_format)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// Reject a leading '+' to stay in parity with scalar DateTime64 parsing, which rejects it
    /// under every date_time_input_format: basic goes through readDateTimeTextFallback (only
    /// special-cases '-'), best_effort through parseDateTime64BestEffort (treats a leading '+'
    /// as a malformed timezone offset). readIntText would silently accept '+', so filter it here
    /// before either the best_effort bare-tick fallback or the basic whole-part parse. This also
    /// matches the pre-PR container behavior, which parsed elements through readDateTime64Text.
    /// Done before the input_format branch so a chunk boundary between '+' and the digits cannot
    /// sneak the sign past the best_effort early return (the byte is already visible here).
    if (!istr.eof() && *istr.position() == '+')
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number with a leading '+' sign");
        else
            return ReturnType(false);
    }

    if (input_format != FormatSettings::DateTimeInputFormat::Basic)
    {
        /// Under best_effort / best_effort_us fall back to the pre-PR bare-integer tick-count path.
        /// A trailing '.' is left unread and the outer container reader rejects it, exactly as the
        /// scalar and quoted-nested best_effort paths reject the dotted fractional unix-timestamp form.
        /// Use CHECK_OVERFLOW so the throw and try paths agree and an out-of-range tick count is
        /// rejected consistently (bare readIntText defaults to DO_NOT_CHECK_OVERFLOW).
        if constexpr (throw_exception)
        {
            readIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(x, istr);
            return;
        }
        else
            return ReturnType(tryReadIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(x, istr));
    }

    bool is_negative = (!istr.eof() && *istr.position() == '-');

    time_t whole = 0;
    bool whole_has_digit = false;
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
            /// Regular negative value. The sign was already consumed above (chunk-boundary
            /// safety), so read the magnitude as an *unsigned* value and re-apply the sign via
            /// well-defined two's-complement negation. Doing `readIntText(whole); whole = -whole`
            /// on a signed `time_t` would be signed-overflow UB for the minimum tick value
            /// -9223372036854775808 (magnitude 2^63 does not fit a signed Int64) and would also
            /// diverge between the throw path (readIntText, no overflow check) and the try path
            /// (tryReadIntText, CHECK_OVERFLOW rejects 2^63). Reading the magnitude as UInt64 keeps
            /// both paths in agreement and preserves INT64_MIN, matching the pre-PR readIntText(x)
            /// container behavior for bare tick counts.
            using UnsignedTime = std::make_unsigned_t<time_t>;
            /// Largest magnitude representable by a negative time_t (|INT64_MIN| == 2^63).
            constexpr UnsignedTime max_magnitude = static_cast<UnsignedTime>(std::numeric_limits<time_t>::max()) + 1;
            UnsignedTime magnitude = 0;
            if constexpr (throw_exception)
            {
                readIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(magnitude, istr);
                if (magnitude > max_magnitude)
                    throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Overflow while parsing a number");
            }
            else if (!tryReadIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(magnitude, istr) || magnitude > max_magnitude)
                return ReturnType(false);
            whole = static_cast<time_t>(0 - magnitude);
            whole_has_digit = true;
        }
    }
    else if constexpr (throw_exception)
    {
        /// CHECK_OVERFLOW mirrors the scalar readDateTime64Text whole-part parse (which uses
        /// ReadIntTextCheckOverflow::CHECK_OVERFLOW) and keeps the throw path in agreement with
        /// the try path below (tryReadIntText also checks overflow). A bare readIntText would
        /// default to DO_NOT_CHECK_OVERFLOW and silently wrap an out-of-range whole seconds value.
        whole_has_digit = !istr.eof() && isNumericASCII(*istr.position());
        readIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(whole, istr);
    }
    else
    {
        whole_has_digit = !istr.eof() && isNumericASCII(*istr.position());
        if (!tryReadIntText<ReadIntTextCheckOverflow::CHECK_OVERFLOW>(whole, istr))
            return ReturnType(false);
    }

    if (istr.eof() || *istr.position() != '.')
    {
        /// No fractional part: the integer is a scaled tick count.
        x = static_cast<DateTime64::NativeType>(whole);
        return ReturnType(true);
    }

    ++istr.position();

    DB::DecimalUtils::DecimalComponents<DateTime64> components{static_cast<DateTime64::NativeType>(whole), 0};

    /// Shared with the scalar readDateTime64Text path (reads fractional digits, pads to scale).
    size_t fractional_digits = readFractionalDateTimePart(components, scale, istr);

    /// A lone '.' / '-.' with no digit on either side of the point (e.g. `[.]`, `[-.]`) is
    /// malformed: the scale-padding loop above would otherwise coerce it to the epoch. Reject
    /// it, matching the scalar readDateTime64Text path so scalar and container agree.
    if (!whole_has_digit && fractional_digits == 0)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number without any digits");
        else
            return ReturnType(false);
    }

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

static inline void readNumericText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, FormatSettings::DateTimeInputFormat input_format)
{
    readNumericTextImpl<void>(x, scale, istr, input_format);
}

static inline bool tryReadNumericText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, FormatSettings::DateTimeInputFormat input_format)
{
    return readNumericTextImpl<bool>(x, scale, istr, input_format);
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
        readNumericText(x, scale, istr, settings.date_time_input_format);
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
        if (!tryReadNumericText(x, scale, istr, settings.date_time_input_format))
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
        readNumericText(x, scale, istr, settings.date_time_input_format);
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
        if (!tryReadNumericText(x, scale, istr, settings.date_time_input_format))
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
