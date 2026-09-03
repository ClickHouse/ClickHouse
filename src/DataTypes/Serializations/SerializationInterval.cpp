#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationInterval.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <IO/WriteBuffer.h>
#include <base/arithmeticOverflow.h>

#include <fmt/format.h>

#include <cmath>


namespace DB
{

using ColumnInterval = DataTypeInterval::ColumnType;

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The Kusto timespan format, `[-][d.]hh:mm:ss[.fffffff]`, where the fraction counts
/// 100-nanosecond ticks. Used for `interval_output_format = 'kusto'`, which is how a KQL
/// timespan is rendered.
std::string formatKustoTimespan(const Int64 ticks)
{
    static constexpr Int64 TICKS_PER_SECOND = 10'000'000;
    static constexpr Int64 TICKS_PER_MINUTE = TICKS_PER_SECOND * 60;
    static constexpr Int64 TICKS_PER_HOUR = TICKS_PER_MINUTE * 60;
    static constexpr Int64 TICKS_PER_DAY = TICKS_PER_HOUR * 24;

    /// `std::abs` of the most negative value is undefined, so widen first.
    const auto absolute = ticks == std::numeric_limits<Int64>::min() ? static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1
                                                                     : static_cast<UInt64>(std::abs(ticks));

    std::string result = ticks < 0 ? "-" : "";
    if (absolute >= static_cast<UInt64>(TICKS_PER_DAY))
        result.append(fmt::format("{}.", absolute / TICKS_PER_DAY));

    result.append(fmt::format(
        "{:02}:{:02}:{:02}",
        (absolute / TICKS_PER_HOUR) % 24,
        (absolute / TICKS_PER_MINUTE) % 60,
        (absolute / TICKS_PER_SECOND) % 60));

    if (const auto fraction = absolute % TICKS_PER_SECOND)
        result.append(fmt::format(".{:07}", fraction));

    return result;
}

void serializeTextKusto(IntervalKind interval_kind, const IColumn & column, const size_t row, WriteBuffer & ostr)
{
    const auto * interval_column = checkAndGetColumn<ColumnInterval>(&column);
    if (!interval_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected column of underlying type of Interval");

    const Int64 value = interval_column->getData()[row];
    if (!interval_kind.isFixedLength())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot format a calendar interval in Kusto timespan format");

    if (interval_kind == IntervalKind::Kind::Nanosecond && value % 100)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot format an IntervalNanosecond that is not a multiple of 100 in Kusto timespan format");

    Int64 nanoseconds = 0;
    if (common::mulOverflow(interval_kind.toAvgNanoseconds(), value, nanoseconds))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Formatting an interval in Kusto dialect will overflow");

    const std::string text = formatKustoTimespan(nanoseconds / 100);
    ostr.write(text.c_str(), text.length());
}

}

SerializationInterval::SerializationInterval(IntervalKind interval_kind_) : interval_kind(std::move(interval_kind_))
{
}


UInt128 SerializationInterval::getHash(IntervalKind kind_)
{
    SipHash hash;
    hash.update("Interval");
    hash.update(kind_.toString());
    return hash.get128();
}

SerializationPtr SerializationInterval::create(IntervalKind kind_)
{
    return ISerialization::pooled(getHash(kind_), [=] { return new SerializationInterval(kind_); });
}

void SerializationInterval::serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    switch (settings.interval_output_format)
    {
        case FormatSettings::IntervalOutputFormat::Numeric:
            Base::serializeText(column, row, ostr, settings);
            return;
        case FormatSettings::IntervalOutputFormat::Kusto:
            serializeTextKusto(interval_kind, column, row, ostr);
            return;
    }
}

void SerializationInterval::serializeTextJSON(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    switch (settings.interval_output_format)
    {
        case FormatSettings::IntervalOutputFormat::Numeric:
            Base::serializeTextJSON(column, row, ostr, settings);
            return;
        case FormatSettings::IntervalOutputFormat::Kusto:
            ostr.write('"');
            serializeTextKusto(interval_kind, column, row, ostr);
            ostr.write('"');
            return;
    }
}

void SerializationInterval::serializeTextCSV(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    switch (settings.interval_output_format)
    {
        case FormatSettings::IntervalOutputFormat::Numeric:
            Base::serializeTextCSV(column, row, ostr, settings);
            return;
        case FormatSettings::IntervalOutputFormat::Kusto:
            ostr.write('"');
            serializeTextKusto(interval_kind, column, row, ostr);
            ostr.write('"');
            return;
    }
}

void SerializationInterval::serializeTextQuoted(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    switch (settings.interval_output_format)
    {
        case FormatSettings::IntervalOutputFormat::Numeric:
            Base::serializeTextQuoted(column, row, ostr, settings);
            return;
        case FormatSettings::IntervalOutputFormat::Kusto:
            ostr.write('\'');
            serializeTextKusto(interval_kind, column, row, ostr);
            ostr.write('\'');
            return;
    }
}

void SerializationInterval::serializeTextHive(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type Interval is not supported by the HiveText output format");
}

}
