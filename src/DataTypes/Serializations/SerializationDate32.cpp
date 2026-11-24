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
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

/// `MAX_DATE32_TIMESTAMP` from Functions/DateTimeTransforms.h, which DataTypes cannot include.
constexpr Int64 max_date32_timestamp = 253402300799LL;

/// Reads a JSON integer the way `toDate32` converts one: a day number up to `DATE_LUT_MAX_EXTEND_DAY_NUM`,
/// a Unix timestamp above it.
template <typename ReturnType>
ReturnType deserializeJSONNumberAsDate32(IColumn & column, ReadBuffer & istr, const DateLUTImpl & time_zone, const FormatSettings & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    Int64 value = 0;
    if constexpr (throw_exception)
        readIntTextImpl<Int64, void, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(value, istr);
    else if (!readIntTextImpl<Int64, bool, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(value, istr))
        return ReturnType(false);

    if (value < DATE_LUT_MIN_EXTEND_DAY_NUM || value > max_date32_timestamp)
    {
        if (settings.throwOnDateTimeOverflow())
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type Date32", value);
            else
                return ReturnType(false);
        }
        value = std::clamp<Int64>(value, DATE_LUT_MIN_EXTEND_DAY_NUM, max_date32_timestamp);
    }

    if (value > DATE_LUT_MAX_EXTEND_DAY_NUM)
        value = time_zone.toDayNum(value).toUnderType();

    assert_cast<ColumnInt32 &>(column).getData().push_back(static_cast<Int32>(value));
    return ReturnType(true);
}

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
    if (!tryReadDateText(x, istr, time_zone, nullptr, !settings.throwOnDateTimeOverflow()) || !istr.eof())
        return false;
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate32::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    readDateText(x, istr, time_zone, !settings.throwOnDateTimeOverflow());
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

bool SerializationDate32::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, !settings.throwOnDateTimeOverflow()))
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
    readDateText(x, istr, time_zone, !settings.throwOnDateTimeOverflow());
    assertChar('\'', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationDate32::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ExtendedDayNum x;
    if (!checkChar('\'', istr) || !tryReadDateText(x, istr, time_zone, nullptr, !settings.throwOnDateTimeOverflow()) || !checkChar('\'', istr))
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
        /// `null`, `true` and `false` keep going through `SerializationNumber`.
        if (!istr.eof() && (isNumericASCII(*istr.position()) || *istr.position() == '-'))
            deserializeJSONNumberAsDate32<void>(column, istr, time_zone, format_settings);
        else
            SerializationNumber<Int32>::deserializeTextJSON(column, istr, format_settings);
        return;
    }
    ExtendedDayNum x;
    readDateText(x, istr, time_zone, !format_settings.throwOnDateTimeOverflow());
    assertChar('"', istr);
    assert_cast<ColumnInt32 &>(column).getData().push_back(x);
}

bool SerializationDate32::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const
{
    if (!checkChar('"', istr))
    {
        if (!istr.eof() && (isNumericASCII(*istr.position()) || *istr.position() == '-'))
            return deserializeJSONNumberAsDate32<bool>(column, istr, time_zone, format_settings);
        return SerializationNumber<Int32>::tryDeserializeTextJSON(column, istr, format_settings);
    }

    ExtendedDayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, !format_settings.throwOnDateTimeOverflow()) || !checkChar('"', istr))
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
    if (settings.throwOnDateTimeOverflow()
        && !tryToMakeDayNum(time_zone, value.year(), value.month(), value.day()))
        throw Exception(ErrorCodes::CANNOT_PARSE_DATE, "Cannot parse date");
    assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
}

bool SerializationDate32::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    LocalDate value;
    if (!tryReadCSV(value, istr))
        return false;
    if (settings.throwOnDateTimeOverflow()
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
