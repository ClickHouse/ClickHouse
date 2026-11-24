#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDate.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

/// `MAX_DATETIME_TIMESTAMP` from Functions/DateTimeTransforms.h, which DataTypes cannot include.
constexpr Int64 max_datetime_timestamp = 0xFFFFFFFF;

/// Reads a JSON integer the way `toDate` converts one: a day number up to `DATE_LUT_MAX_DAY_NUM`,
/// a Unix timestamp above it.
template <typename ReturnType>
ReturnType deserializeJSONNumberAsDate(IColumn & column, ReadBuffer & istr, const DateLUTImpl & time_zone, const FormatSettings & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    Int64 value = 0;
    if constexpr (throw_exception)
        readIntTextImpl<Int64, void, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(value, istr);
    else if (!readIntTextImpl<Int64, bool, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(value, istr))
        return ReturnType(false);

    if (value < 0 || value > max_datetime_timestamp)
    {
        if (settings.throwOnDateTimeOverflow())
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", value);
            else
                return ReturnType(false);
        }
        value = std::clamp<Int64>(value, 0, max_datetime_timestamp);
    }

    if (value > DATE_LUT_MAX_DAY_NUM)
        value = time_zone.toDayNum(value).toUnderType();

    assert_cast<ColumnUInt16 &>(column).getData().push_back(static_cast<UInt16>(value));
    return ReturnType(true);
}

}

UInt128 SerializationDate::getHash(const DateLUTImpl & time_zone_)
{
    SipHash hash;
    hash.update("Date");
    const auto & tz = time_zone_.getTimeZone();
    hash.update(tz.size());
    hash.update(tz);
    return hash.get128();
}

void SerializationDate::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeDateText(DayNum(assert_cast<const ColumnUInt16 &>(column).getData()[row_num]), ostr, time_zone);
}

void SerializationDate::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Date");
}

bool SerializationDate::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, !settings.throwOnDateTimeOverflow()) || !istr.eof())
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    readDateText(x, istr, time_zone, !settings.throwOnDateTimeOverflow());
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

bool SerializationDate::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, !settings.throwOnDateTimeOverflow()))
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    assertChar('\'', istr);
    readDateText(x, istr, time_zone, !settings.throwOnDateTimeOverflow());
    assertChar('\'', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationDate::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum x;
    if (!checkChar('\'', istr) || !tryReadDateText(x, istr, time_zone, nullptr, !settings.throwOnDateTimeOverflow()) || !checkChar('\'', istr))
        return false;

    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const
{
    if (!checkChar('"', istr))
    {
        /// `null`, `true` and `false` keep going through `SerializationNumber`.
        if (!istr.eof() && (isNumericASCII(*istr.position()) || *istr.position() == '-'))
            deserializeJSONNumberAsDate<void>(column, istr, time_zone, format_settings);
        else
            SerializationNumber<UInt16>::deserializeTextJSON(column, istr, format_settings);
        return;
    }
    DayNum x;
    readDateText(x, istr, time_zone, !format_settings.throwOnDateTimeOverflow());
    assertChar('"', istr);
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

bool SerializationDate::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & format_settings) const
{
    if (!checkChar('"', istr))
    {
        if (!istr.eof() && (isNumericASCII(*istr.position()) || *istr.position() == '-'))
            return deserializeJSONNumberAsDate<bool>(column, istr, time_zone, format_settings);
        return SerializationNumber<UInt16>::tryDeserializeTextJSON(column, istr, format_settings);
    }

    DayNum x;
    if (!tryReadDateText(x, istr, time_zone, nullptr, !format_settings.throwOnDateTimeOverflow()) || !checkChar('"', istr))
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(x);
    return true;
}

void SerializationDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum value;
    readCSV(value, istr, time_zone, !settings.throwOnDateTimeOverflow());
    assert_cast<ColumnUInt16 &>(column).getData().push_back(value);
}

bool SerializationDate::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DayNum value;
    if (!tryReadCSV(value, istr, time_zone, !settings.throwOnDateTimeOverflow()))
        return false;
    assert_cast<ColumnUInt16 &>(column).getData().push_back(value);
    return true;
}

SerializationDate::SerializationDate(const DateLUTImpl & time_zone_) : time_zone(time_zone_)
{
}

SerializationPtr SerializationDate::create(const DateLUTImpl & time_zone_)
{
    return ISerialization::pooled(getHash(time_zone_), [&] { return new SerializationDate(time_zone_); });
}

}
