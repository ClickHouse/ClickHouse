#include "SerializationInterval.h"

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <Parsers/Kusto/Formatters.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

SerializationInterval::SerializationInterval(IntervalKind interval_kind_)
    : SerializationCustomSimpleText(std::make_shared<SerializationNumber<typename DataTypeInterval::FieldType>>()),
    interval_kind(std::move(interval_kind_))
{
}

void SerializationInterval::serializeText(
    const IColumn & column, const size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    switch (settings.interval.output_format)
    {
        case FormatSettings::IntervalOutputFormat::Numeric:
        {
            getNested()->serializeTextEscaped(column, row, ostr, settings);
            break;
        }

        case FormatSettings::IntervalOutputFormat::Kusto:
        {
            const auto * interval_column = checkAndGetColumn<DataTypeInterval::ColumnType>(column);
            if (!interval_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected column of underlying type of Interval");

            const auto & value = interval_column->getData()[row];
            const auto ticks = interval_kind.toAvgNanoseconds() * value / 100;
            const auto interval_as_string = formatKQLTimespan(ticks);
            ostr.write(interval_as_string.data(), interval_as_string.size());

            break;
        }
    }
}

void SerializationInterval::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    return whole
        ? getNested()->deserializeWholeText(column, istr, settings)
        : getNested()->deserializeTextEscaped(column, istr, settings);
}

}
