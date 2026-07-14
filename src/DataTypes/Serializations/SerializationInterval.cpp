#include <DataTypes/Serializations/SerializationInterval.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <IO/WriteBuffer.h>
#include <Parsers/Kusto/Formatters.h>
#include <base/arithmeticOverflow.h>


namespace DB
{

using ColumnInterval = DataTypeInterval::ColumnType;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Trash for Kusto dialect:
void serializeTextKusto(IntervalKind interval_kind, const IColumn & column, const size_t row, WriteBuffer & ostr, const FormatSettings &)
{
    const auto * interval_column = checkAndGetColumn<ColumnInterval>(&column);
    if (!interval_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected column of underlying type of Interval");

    Int64 value = interval_column->getData()[row];
    Int64 ticks = 0;
    if (common::mulOverflow(interval_kind.toAvgNanoseconds(), value, ticks))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Formatting an interval in Kusto dialect will overflow");
    ticks = ticks / 100;
    std::string interval_as_string = formatKQLTimespan(ticks);
    ostr.write(interval_as_string.c_str(), interval_as_string.length());
}

}

SerializationInterval::SerializationInterval(IntervalKind interval_kind_) : interval_kind(std::move(interval_kind_))
{
}


void SerializationInterval::serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    switch (settings.interval_output_format)
    {
        case FormatSettings::IntervalOutputFormat::Numeric:
            Base::serializeText(column, row, ostr, settings);
            return;
        case FormatSettings::IntervalOutputFormat::Kusto:
            serializeTextKusto(interval_kind, column, row, ostr, settings);
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
            serializeTextKusto(interval_kind, column, row, ostr, settings);
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
            serializeTextKusto(interval_kind, column, row, ostr, settings);
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
            serializeTextKusto(interval_kind, column, row, ostr, settings);
            ostr.write('\'');
            return;
    }
}

}
